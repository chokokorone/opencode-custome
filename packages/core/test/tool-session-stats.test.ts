import { describe, expect } from "bun:test"
import { LanguageModel, LLM, LLMEvent } from "@opencode-ai/ai"
import { OpenAIChat } from "@opencode-ai/ai/protocols/openai-chat"
import { TestLLM } from "@opencode-ai/ai/testing"
import { Agent } from "@opencode-ai/core/agent"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Config } from "@opencode-ai/core/config"
import { Job } from "@opencode-ai/core/job"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Permission } from "@opencode-ai/core/permission"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { Session } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { SessionStep } from "@opencode-ai/core/session/runner/step"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { Tool } from "@opencode-ai/core/tool"
import { SessionStatsTool } from "@opencode-ai/core/tool/plugin/session-stats"
import { ToolOutput } from "@opencode-ai/core/tool-output"
import { Plugin } from "@opencode-ai/core/plugin"
import { PluginSupervisor } from "@opencode-ai/core/plugin/supervisor"
import { Money } from "@opencode-ai/schema/money"
import { makeGlobalNode, makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Global } from "@opencode-ai/util/global"
import { Deferred, Effect, Layer } from "effect"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { testEffect } from "./lib/effect"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"
import { contentText, throwingPermission, toolCall } from "./lib/tool-extra"

// Approval gates must never fire for session_stats: any ask/assert/reply attempt
// is a test failure. Every tool call in this file runs behind this stub.
const throwingStatsPermission = throwingPermission("session_stats tools")

const executionNode = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.effect(
    SessionExecution.Service,
    Effect.succeed(
      SessionExecution.Service.of({
        active: Effect.succeed(new Set()),
        isActive: () => Effect.succeed(false),
        resume: () => Effect.void,
        wake: () => Effect.void,
        interrupt: () => Effect.succeed(false),
        awaitIdle: () => Effect.void,
      }),
    ),
  ),
  deps: [Bus.node, SessionStore.node],
})

const statsPluginSupervisor = makeLocationNode({
  name: "test/session-stats-plugins",
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      yield* registerToolPlugin(SessionStatsTool.Plugin)
    }),
  ),
  deps: [Database.node, Tool.node],
})

const toolNodes = LayerNode.group([
  Database.node,
  Bus.node,
  Job.node,
  Session.node,
  SessionTeam.node,
  SessionExecution.node,
  LocationServiceMap.node,
])

const itTool = testEffect(
  AppNodeBuilder.build(toolNodes, [
    SessionExecution.node.replace(executionNode),
    Bus.node.replace(Bus.configured({ persist: true })),
    Global.node.replace(tempGlobalLayer),
    PluginSupervisor.node.replace(statsPluginSupervisor),
    Permission.node.replace(throwingStatsPermission),
  ]),
)

const itProof = testEffect(
  Layer.merge(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node]), [
      Bus.node.replace(Bus.configured({ persist: true })),
    ]),
    TestLLM.testLayer(),
  ),
)

const tokensFor = (input: number, output: number, read: number, write: number) => ({
  input,
  output,
  reasoning: 0,
  cache: { read, write },
})

describe("SessionStatsTool", () => {
  itTool.live("aggregates durable Step.Ended events into compact text", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const bus = yield* Bus.Service
          const session = yield* sessions.create({ location, title: "stats" })
          const other = yield* sessions.create({ location, title: "other" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
            Effect.provide(locations.get(location)),
          )
          const calls = { calls: 0 }
          const call = (sessionID: Session.ID, name: string, input: Record<string, unknown>) =>
            toolCall(registry, sessionID, name, input, calls)

          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID: session.id,
            assistantMessageID: SessionMessage.ID.create(),
            finish: "stop",
            cost: Money.USD.make(0.001),
            tokens: tokensFor(800, 200, 500, 0),
            metrics: { toolCalls: 3, maxParallelTools: 2, stepLatencyMs: 5, contextBytes: 10 },
          })
          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID: session.id,
            assistantMessageID: SessionMessage.ID.create(),
            finish: "stop",
            cost: Money.USD.make(0.0002),
            tokens: tokensFor(400, 100, 300, 0),
            metrics: { toolCalls: 2, maxParallelTools: 1, stepLatencyMs: 5, contextBytes: 10 },
          })
          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID: other.id,
            assistantMessageID: SessionMessage.ID.create(),
            finish: "stop",
            cost: Money.USD.make(9),
            tokens: tokensFor(9000, 9000, 9000, 9000),
            metrics: { toolCalls: 99, maxParallelTools: 99, stepLatencyMs: 5, contextBytes: 10 },
          })

          const explicit = yield* call(session.id, "session_stats", { sessionID: session.id })
          expect(explicit.status).toBe("completed")
          const out = contentText(explicit)
          expect(out).toContain(`session: ${session.id} | turns: 2 | llm_requests: 2`)
          expect(out).toContain("tokens: in=1200 out=300 cached=800 | cost=0.0012")
          expect(out).toContain("tools: 5 calls, peak parallel 2")
          expect(out).not.toContain("{")
          expect(out).not.toContain("9000")
          expect(out).not.toContain("99 calls")

          const implicit = yield* call(session.id, "session_stats", {})
          expect(implicit.status).toBe("completed")
          expect(contentText(implicit)).toBe(out)

          const empty = yield* call(other.id, "session_stats", {})
          expect(empty.status).toBe("completed")
          expect(contentText(empty)).toContain("turns: 1")
          expect(contentText(empty)).toContain("tools: 99 calls, peak parallel 99")

          const fresh = yield* sessions.create({ location, title: "fresh" })
          const none = yield* call(fresh.id, "session_stats", {})
          expect(none.status).toBe("completed")
          expect(contentText(none)).toContain("turns: 0 | llm_requests: 0")
          expect(contentText(none)).toContain("tokens: in=0 out=0 cached=0 | cost=0")
          expect(contentText(none)).toContain("tools: 0 calls, peak parallel 0")
        }),
      ),
    ),
  )
})

describe("SessionStats measured proof", () => {
  itProof.effect("batching two tool calls into one turn uses fewer LLM requests than one call per turn", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const llm = yield* TestLLM.Test
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
      const batchID = Session.ID.create()
      const sequentialID = Session.ID.create()
      yield* db
        .insert(SessionTable)
        .values({ id: batchID, project_id: Project.ID.global, slug: "batch", directory: "/project", version: "test" })
        .run()
      yield* db
        .insert(SessionTable)
        .values({
          id: sequentialID,
          project_id: Project.ID.global,
          slug: "sequential",
          directory: "/project",
          version: "test",
        })
        .run()
      const model = SessionRunnerModel.resolved(
        LanguageModel.make({ id: "test-model", provider: "test", route: OpenAIChat.route }),
        {
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          limit: { context: 100_000, output: 1_000 },
          cost: [
            {
              input: Money.USDPerMillionTokens.make(1),
              output: Money.USDPerMillionTokens.make(2),
              cache: { read: Money.USDPerMillionTokens.make(0.1), write: Money.USDPerMillionTokens.make(0.5) },
            },
          ],
        },
      )
      const steps = yield* SessionStep.make.pipe(
        Effect.provide(
          Layer.mock(Snapshot.Service)({
            capture: () => Effect.succeed(Snapshot.ID.make("snap")),
            files: () => Effect.succeed([]),
          }),
        ),
      )
      const usage = {
        inputTokens: 15,
        outputTokens: 6,
        nonCachedInputTokens: 10,
        cacheReadInputTokens: 3,
        cacheWriteInputTokens: 2,
        reasoningTokens: 2,
      }

      // (a) One turn issuing 2 tool calls together. The first blocks until the
      // second starts, proving peak parallelism of 2 without timing assumptions.
      const gate = yield* Deferred.make<void>()
      const releaseSecond = yield* Deferred.make<void>()
      let started = 0
      yield* llm.push(
        TestLLM.complete(
          { reason: { normalized: "stop" }, usage },
          LLMEvent.toolCall({ id: "call-one", name: "test", input: {} }),
          LLMEvent.toolCall({ id: "call-two", name: "test", input: {} }),
        ),
      )
      const beforeBatch = (yield* llm.requests()).length
      const batch = yield* steps.attempt({
        sessionID: batchID,
        assistantMessageID: SessionMessage.ID.create(),
        agent: Agent.defaultID,
        model,
        prepared: {
          retry: () => Effect.void,
          request: LLM.request({ model: model.model, prompt: "Run two tools" }),
          options: {},
          executeTool: (input) =>
            Effect.gen(function* () {
              started += 1
              if (input.call.id === "call-one") yield* Deferred.await(gate)
              if (started === 2) yield* Deferred.succeed(releaseSecond, undefined)
              if (input.call.id === "call-two") {
                yield* Deferred.await(releaseSecond)
                yield* Deferred.succeed(gate, undefined)
              }
              return { content: [{ type: "text", text: "done" }] }
            }),
        },
        retry: () => Effect.succeed({ retry: false }),
        recoverContinuation: true,
        recoverOverflow: Effect.succeed(false),
      })
      expect(SessionStep.Outcome.$is("Completed")(batch)).toBe(true)
      const batchRequests = (yield* llm.requests()).length - beforeBatch
      const batchSummary = yield* SessionStatsTool.aggregate(batchID)
      expect(batchSummary.turns).toBe(1)
      expect(batchSummary.llmRequests).toBe(1)
      expect(batchSummary.toolCalls).toBe(2)
      expect(batchSummary.peakParallel).toBe(2)
      expect(SessionStatsTool.format(batchSummary)).toContain("turns: 1 | llm_requests: 1")

      // (b) Two turns issuing 1 tool call each.
      yield* llm.push(
        TestLLM.complete({ reason: { normalized: "stop" }, usage }, LLMEvent.toolCall({ id: "solo-one", name: "test", input: {} })),
      )
      yield* llm.push(
        TestLLM.complete({ reason: { normalized: "stop" }, usage }, LLMEvent.toolCall({ id: "solo-two", name: "test", input: {} })),
      )
      const beforeSequential = (yield* llm.requests()).length
      for (const solo of ["solo-one", "solo-two"] as const) {
        const outcome = yield* steps.attempt({
          sessionID: sequentialID,
          assistantMessageID: SessionMessage.ID.create(),
          agent: Agent.defaultID,
          model,
          prepared: {
            retry: () => Effect.void,
            request: LLM.request({ model: model.model, prompt: `Run ${solo}` }),
            options: {},
            executeTool: () => Effect.succeed({ content: [{ type: "text", text: "done" }] }),
          },
          retry: () => Effect.succeed({ retry: false }),
          recoverContinuation: true,
          recoverOverflow: Effect.succeed(false),
        })
        expect(SessionStep.Outcome.$is("Completed")(outcome)).toBe(true)
      }
      const sequentialRequests = (yield* llm.requests()).length - beforeSequential
      const sequentialSummary = yield* SessionStatsTool.aggregate(sequentialID)
      expect(sequentialSummary.turns).toBe(2)
      expect(sequentialSummary.llmRequests).toBe(2)
      expect(sequentialSummary.toolCalls).toBe(2)
      expect(sequentialSummary.peakParallel).toBe(1)

      // Measured proof: batching saves an LLM request for the same tool work.
      expect(batchRequests).toBe(1)
      expect(sequentialRequests).toBe(2)
      expect(batchRequests).toBeLessThan(sequentialRequests)
      expect(batchSummary.llmRequests).toBeLessThan(sequentialSummary.llmRequests)
      yield* Effect.log(`measured proof: batch=${batchRequests} request for 2 tools, sequential=${sequentialRequests} requests for 2 tools`)
    }),
  )
})
