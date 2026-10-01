import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect"
import { Money } from "@opencode-ai/schema/money"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Global } from "@opencode-ai/util/global"
import { makeGlobalNode, makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { Database } from "@opencode-ai/core/database/database"
import { Bus } from "@opencode-ai/core/bus"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { Config } from "@opencode-ai/core/config"
import { Location } from "@opencode-ai/core/location"
import { Model } from "@opencode-ai/core/model"
import { Provider } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Agent } from "@opencode-ai/core/agent"
import { Job } from "@opencode-ai/core/job"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Session } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionInbox } from "@opencode-ai/core/session/inbox"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { Git } from "@opencode-ai/core/git"
import { Permission } from "@opencode-ai/core/permission"
import { Plugin } from "@opencode-ai/core/plugin"
import { PluginSupervisor } from "@opencode-ai/core/plugin/supervisor"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { SubagentTool } from "@opencode-ai/core/tool/plugin/subagent"
import { Tool } from "@opencode-ai/core/tool"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { offlineModels } from "./fixture/models"
import { testEffect } from "./lib/effect"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

/**
 * Spec-compliance characterization for async/batch execution.
 *
 * - §13 (non-blocking spawn): the spawn call returns a running handle plus an
 *   ID while the child is still incomplete. Proven at two levels: the subagent
 *   tool returns `backgroundResult` without awaiting the child job, and a
 *   Deferred-gated Job stays `running` across `start` + `background`.
 * - §14 (no polling): the completion notice lands in the parent inbox via the
 *   push path (bus `InboxEnqueued` -> pending inbox row). The test blocks on
 *   the bus event and never calls a status/poll API (`jobs.wait`, `jobs.get`
 *   polling, `tool_wait`, shell status).
 * - §15 (parallel + batch): two `executeTool` calls issued together (one
 *   message carrying two calls) both execute. This proves registry-level
 *   concurrency; the production single-turn fan-out (one fiber per tool-call
 *   event, all joined) lives in `session/runner/step.ts` and is pinned by
 *   `session-step-metrics.test.ts` (`maxParallelTools: 2`).
 * - §16 (batched delivery): two completions admitted before any boundary are
 *   both pending, and one `SessionInbox.promote` delivers both. The mock
 *   execution never auto-promotes (wake is a no-op), modelling a parent that
 *   stays busy mid-turn; withholding `promote` until both admissions land is
 *   the deterministic proxy for "while the parent is busy".
 */

const childText = "child final response"
const childModel = Model.Ref.make({ id: Model.ID.make("child"), providerID: Provider.ID.make("test") })
const parentModel = Model.Ref.make({ id: Model.ID.make("parent"), providerID: Provider.ID.make("test") })
const tokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }

const outputSessionID = (value: unknown) =>
  Schema.decodeUnknownSync(Schema.Struct({ sessionID: Session.ID }))(value).sessionID

const executionNode = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.effect(
    SessionExecution.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const store = yield* SessionStore.Service
      const completed = new Set<Session.ID>()
      const complete = Effect.fn("SubagentTest.complete")(function* (sessionID: Session.ID) {
        if (completed.has(sessionID)) return
        if ((yield* store.get(sessionID))?.title?.includes("fail")) {
          yield* new SessionRunnerModel.ModelNotSelectedError({ sessionID })
          return
        }
        completed.add(sessionID)
        const assistantMessageID = SessionMessage.ID.create()
        yield* bus.publish(SessionEvent.Step.Started, {
          sessionID,
          assistantMessageID,
          agent: Agent.ID.make("reviewer"),
          model: childModel,
        })
        yield* bus.publish(SessionEvent.Text.Started, {
          sessionID,
          assistantMessageID,
          ordinal: 0,
        })
        yield* bus.publish(SessionEvent.Text.Ended, {
          sessionID,
          assistantMessageID,
          ordinal: 0,
          text: childText,
        })
        yield* bus.publish(SessionEvent.Step.Ended, {
          sessionID,
          assistantMessageID,
          finish: "stop",
          cost: Money.USD.zero,
          tokens,
        })
      })
      return SessionExecution.Service.of({
        active: Effect.succeed(new Set()),
        isActive: () => Effect.succeed(false),
        resume: complete,
        wake: () => Effect.void,
        interrupt: () => Effect.succeed(false),
        awaitIdle: (sessionID) => complete(sessionID).pipe(Effect.exit, Effect.asVoid),
      })
    }),
  ),
  deps: [Bus.node, SessionStore.node],
})

const subagentPluginSupervisor = makeLocationNode({
  name: "test/subagent-plugins",
  layer: Layer.effectDiscard(registerToolPlugin(SubagentTool.Plugin)),
  deps: [Agent.node, Bus.node, Config.node, FSUtil.node, Permission.node, Session.node, SessionTeam.node, Job.node, Tool.node, Git.node],
})

const nodes = LayerNode.group([
  Database.node,
  Bus.node,
  Job.node,
  Session.node,
  SessionTeam.node,
  SessionExecution.node,
  LocationServiceMap.node,
])
const replacements = [
  SessionExecution.node.replace(executionNode),
  Global.node.replace(tempGlobalLayer),
  offlineModels,
] satisfies LayerNode.Replacements
const it = testEffect(
  AppNodeBuilder.build(nodes, [...replacements, PluginSupervisor.node.replace(subagentPluginSupervisor)]),
)

const withSubagent = (location: Location.Ref) =>
  Effect.gen(function* () {
    const locations = yield* LocationServiceMap.Service
    yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
    yield* Agent.Service.use((agents) =>
      agents.transform((editor) => {
        // The caller identity used by executeTool; subagent permission asserts against it.
        editor.update(toolIdentity.agent, (agent) => {
          agent.mode = "primary"
          agent.permissions.push({ action: "*", resource: "*", effect: "allow" })
        })
        editor.update(Agent.ID.make("reviewer"), (agent) => {
          agent.mode = "subagent"
          agent.model = childModel
        })
        editor.update(Agent.ID.make("fallback"), (agent) => {
          agent.mode = "subagent"
        })
        editor.update(Agent.ID.make("primary"), (agent) => {
          agent.mode = "primary"
        })
      }),
    ).pipe(Effect.provide(locations.get(location)))
  })

describe("ToolAsyncCompliance", () => {
  it.live("§13 background spawn returns immediately while work is still incomplete", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, model: parentModel })
          yield* withSubagent(parent.location)
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(parent.location)))
          const jobs = yield* Job.Service
          const progress: Tool.Metadata[] = []

          // Subagent level: the spawn call settles at once with a running
          // handle plus the child sessionID; it never awaits the child job.
          const settled = yield* executeTool(registry, {
            sessionID: parent.id,
            ...toolIdentity,
            progress: (update) => Effect.sync(() => progress.push(update)),
            call: {
              type: "tool-call",
              id: "call-async-spawn",
              name: SubagentTool.name,
              input: { agent: "reviewer", description: "async review", prompt: "review this" },
            },
          })
          const childID = outputSessionID(settled.metadata)
          expect(settled.status).toBe("completed")
          expect(settled.metadata).toEqual({ sessionID: childID, status: "running" })
          expect(settled.content).toEqual([
            { type: "text", text: expect.stringContaining(`sessionID: ${childID}`) },
          ])
          expect(progress).toEqual([{ sessionID: childID, status: "running" }])
          expect((yield* sessions.get(childID)).parentID).toBe(parent.id)
          expect((yield* sessions.inbox(childID)).find((message) => message.type === "user")?.payload.text).toBe(
            "You are a subagent spawned by another session.\nreview this",
          )

          // Job level: the "still incomplete" instant, pinned deterministically.
          // The child fiber parks on an unreleased gate, so start + background
          // must both return while the job is provably still running.
          const gate = yield* Deferred.make<void>()
          const spawned = yield* jobs.start({
            id: "job-async-spawn",
            type: "test",
            metadata: { sessionID: parent.id },
            run: Deferred.await(gate).pipe(Effect.as("finished work")),
          })
          expect(spawned.status).toBe("running")
          const backgrounded = yield* jobs.background(spawned.id)
          expect(backgrounded?.status).toBe("running")
          expect((yield* jobs.get(spawned.id))?.status).toBe("running")
          yield* Deferred.succeed(gate, undefined)
          const finished = yield* jobs.wait({ id: spawned.id })
          expect(finished.info?.status).toBe("completed")
          expect(finished.info?.output).toBe("finished work")
        }),
      ),
    ),
  )

  it.live("§14 background completion is injected into the parent inbox with no status polling", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, model: parentModel })
          yield* withSubagent(parent.location)
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(parent.location)))
          const bus = yield* Bus.Service
          // Push-based admission signal: block on the bus event, never on a
          // status/poll API (no jobs.wait/get, no tool_wait, no status tool).
          const admitted = yield* bus.subscribe(SessionEvent.InboxEnqueued).pipe(
            Stream.filter((event) => event.data.sessionID === parent.id && event.data.item.type === "synthetic"),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )

          const settled = yield* executeTool(registry, {
            sessionID: parent.id,
            ...toolIdentity,
            call: {
              type: "tool-call",
              id: "call-background-injection",
              name: SubagentTool.name,
              input: { agent: "reviewer", description: "background review", prompt: "review this" },
            },
          })
          const childID = outputSessionID(settled.metadata)
          expect(settled.metadata).toEqual({ sessionID: childID, status: "running" })

          const admission = Array.from(yield* Fiber.join(admitted))[0]
          expect(admission?.data.item.type).toBe("synthetic")
          if (admission?.data.item.type !== "synthetic") return yield* Effect.die("Expected synthetic inbox item")

          // The completion notice is present in the parent inbox; no poll ran.
          const pending = yield* sessions.inbox(parent.id)
          const notice = pending.find((item) => item.type === "synthetic")
          expect(notice?.type).toBe("synthetic")
          if (notice === undefined || notice.type !== "synthetic")
            return yield* Effect.die("Expected synthetic inbox item")
          expect(notice.payload.text).toContain(`<subagent sessionID="${childID}" state="completed"`)
          expect(notice.payload.text).toContain(childText)
          expect(notice.payload.description).toBe("background review")
          expect(admission.data.item.payload.text).toBe(notice.payload.text)
        }),
      ),
    ),
  )

  it.live("§15 two tool calls issued together both execute", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, model: parentModel })
          yield* withSubagent(parent.location)
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(parent.location)))
          const bus = yield* Bus.Service
          const delivered = yield* bus.subscribe(SessionEvent.InboxEnqueued).pipe(
            Stream.filter((event) => event.data.sessionID === parent.id && event.data.item.type === "synthetic"),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )

          const spawn = (id: string, description: string) =>
            executeTool(registry, {
              sessionID: parent.id,
              ...toolIdentity,
              call: {
                type: "tool-call",
                id,
                name: SubagentTool.name,
                input: { agent: "reviewer", description, prompt: "review this" },
              },
            })
          // Simplest deterministic form: two executeTool calls sharing one
          // concurrent batch, standing in for one message carrying two calls.
          // This proves the registry runs both; the production single-turn
          // fan-out (one fiber per tool-call event, all joined) is
          // SessionStep.attempt in packages/core/src/session/runner/step.ts.
          const [first, second] = yield* Effect.all(
            [spawn("call-batch-one", "batch one"), spawn("call-batch-two", "batch two")],
            { concurrency: "unbounded" },
          )
          const firstID = outputSessionID(first.metadata)
          const secondID = outputSessionID(second.metadata)
          expect(first.metadata).toEqual({ sessionID: firstID, status: "running" })
          expect(second.metadata).toEqual({ sessionID: secondID, status: "running" })
          expect(firstID).not.toBe(secondID)
          for (const childID of [firstID, secondID]) {
            expect((yield* sessions.get(childID)).parentID).toBe(parent.id)
          }

          // Both background children ran to completion and notified.
          const completions = Array.from(yield* Fiber.join(delivered))
          expect(completions).toHaveLength(2)
          const texts = completions.map((event) =>
            event.data.item.type === "synthetic" ? event.data.item.payload.text : "",
          )
          expect(texts.join("\n")).toContain(`<subagent sessionID="${firstID}" state="completed"`)
          expect(texts.join("\n")).toContain(`<subagent sessionID="${secondID}" state="completed"`)
        }),
      ),
    ),
  )

  it.live("§16 completions batch at the next turn boundary", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, model: parentModel })
          yield* withSubagent(parent.location)
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(parent.location)))
          const bus = yield* Bus.Service
          const database = yield* Database.Service
          const delivered = yield* bus.subscribe(SessionEvent.InboxEnqueued).pipe(
            Stream.filter((event) => event.data.sessionID === parent.id && event.data.item.type === "synthetic"),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )

          const spawn = (id: string, description: string) =>
            executeTool(registry, {
              sessionID: parent.id,
              ...toolIdentity,
              call: {
                type: "tool-call",
                id,
                name: SubagentTool.name,
                input: { agent: "reviewer", description, prompt: "review this" },
              },
            })
          const first = yield* spawn("call-boundary-one", "boundary one")
          const second = yield* spawn("call-boundary-two", "boundary two")
          const firstID = outputSessionID(first.metadata)
          const secondID = outputSessionID(second.metadata)

          // Both completions are admitted before any boundary runs: the mock
          // execution never auto-promotes (wake is a no-op), modelling a
          // parent that stays busy mid-turn.
          yield* Fiber.join(delivered)
          const pending = yield* sessions.inbox(parent.id)
          expect(pending.filter((item) => item.type === "synthetic")).toHaveLength(2)

          // Observed batching semantics: one promote publishes InboxDelivered
          // for every pending steer, so a single boundary suffices for both.
          const promoted = yield* SessionInbox.promote(database.db, bus, parent.id, "steer")
          expect(promoted).toBe(2)
          expect(yield* sessions.inbox(parent.id)).toHaveLength(0)
          const synthetics = (yield* sessions.context(parent.id)).filter((message) => message.type === "synthetic")
          expect(synthetics).toHaveLength(2)
          const texts = synthetics.flatMap((message) => (message.type === "synthetic" ? [message.text] : []))
          expect(texts.join("\n")).toContain(`<subagent sessionID="${firstID}" state="completed"`)
          expect(texts.join("\n")).toContain(`<subagent sessionID="${secondID}" state="completed"`)
        }),
      ),
    ),
  )
})
