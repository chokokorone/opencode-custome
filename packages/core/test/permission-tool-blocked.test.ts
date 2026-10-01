import { describe, expect } from "bun:test"
import { DateTime, Deferred, Effect, Fiber, Layer, Ref } from "effect"
import { Agent } from "@opencode-ai/core/agent"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Bus } from "@opencode-ai/core/bus"
import { Location } from "@opencode-ai/core/location"
import { Model } from "@opencode-ai/core/model"
import { Permission } from "@opencode-ai/core/permission"
import { PermissionSaved } from "@opencode-ai/core/permission/saved"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { Provider } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageUpdater } from "@opencode-ai/core/session/message-updater"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const current = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory: AbsolutePath.make("/project") })),
)
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionStore.node, PermissionSaved.node, Agent.node, Permission.node]),
    [Location.node.replace(current)],
  ),
)

function setup(rules: Permission.Ruleset = [], sessionID = Session.ID.make("ses_test")) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "test",
        directory: "/project",
        title: "test",
        version: "test",
        agent: "test",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* setRules(rules)
  })
}

function setRules(rules: Permission.Ruleset) {
  return Effect.gen(function* () {
    const agents = yield* Agent.Service
    yield* agents.transform((editor) =>
      editor.update(Agent.ID.make("test"), (agent) => {
        agent.permissions = [...rules]
      }),
    )
  })
}

const toolOf = (assistant: SessionMessage.Assistant, id: string) =>
  assistant.content.find((item): item is SessionMessage.AssistantTool => item.type === "tool" && item.id === id)

function memoryAssistant(assistantID: SessionMessage.ID, toolID: string, input: Record<string, unknown>) {
  const created = DateTime.makeUnsafe(Date.now())
  let current = SessionMessage.Assistant.make({
    id: assistantID,
    type: "assistant",
    agent: Agent.ID.make("test"),
    model: { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") },
    time: { created },
    content: [
      SessionMessage.AssistantTool.make({
        type: "tool",
        id: toolID,
        name: "read",
        time: { created },
        state: SessionMessage.ToolStateRunning.make({ status: "running", input, metadata: {} }),
      }),
    ],
  })
  const adapter: SessionMessageUpdater.Adapter = {
    getAgent: () => Effect.succeed(undefined),
    getModel: () => Effect.succeed(undefined),
    getLocation: () => Effect.die(new Error("unused")),
    getCurrentAssistant: () => Effect.sync(() => current),
    getAssistant: (messageID) => Effect.sync(() => (messageID === current.id ? current : undefined)),
    getShell: () => Effect.succeed(undefined),
    getCompaction: () => Effect.succeed(undefined),
    updateAssistant: (next) =>
      Effect.sync(() => {
        current = next
      }),
    updateShell: () => Effect.void,
    updateCompaction: () => Effect.void,
    appendMessage: () => Effect.void,
  }
  return { adapter, current: () => current }
}

describe("Permission tool blocked", () => {
  it.effect("parks a tool fiber in blocked and resumes it on reply", () =>
    Effect.gen(function* () {
      yield* setup([])
      const bus = yield* Bus.Service
      const service = yield* Permission.Service
      const assistantID = SessionMessage.ID.create()
      const toolID = "call_blocked_1"
      const requestID = Permission.ID.create("per_blocked_1")
      const input = { path: "a.txt" }
      const store = memoryAssistant(assistantID, toolID, input)

      const asked = yield* Deferred.make<Permission.Request>()
      const blockedEvent = yield* Deferred.make<SessionEvent.Tool.Blocked>()
      const resumedEvent = yield* Deferred.make<SessionEvent.Tool.Resumed>()
      const unsubscribe = yield* bus.listen((event) => {
        if (event.type === Permission.Event.Asked.type) {
          const request = event.data as Permission.Request
          if (request.id === requestID) return Deferred.succeed(asked, request).pipe(Effect.asVoid)
        }
        if (event.type === SessionEvent.Tool.Blocked.type) {
          const payload = event as SessionEvent.Tool.Blocked
          if (payload.data.id === toolID) return Deferred.succeed(blockedEvent, payload).pipe(Effect.asVoid)
        }
        if (event.type === SessionEvent.Tool.Resumed.type) {
          const payload = event as SessionEvent.Tool.Resumed
          if (payload.data.id === toolID) return Deferred.succeed(resumedEvent, payload).pipe(Effect.asVoid)
        }
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const fiber = yield* service
        .assert({
          id: requestID,
          sessionID: Session.ID.make("ses_test"),
          action: "read",
          resources: ["a.txt"],
          source: { type: "tool", messageID: assistantID, id: toolID },
        })
        .pipe(Effect.forkScoped)

      const request = yield* Deferred.await(asked)
      expect(request.action).toBe("read")
      const blocked = yield* Deferred.await(blockedEvent)
      expect(blocked.data).toMatchObject({
        sessionID: Session.ID.make("ses_test"),
        assistantMessageID: assistantID,
        id: toolID,
        permission: { action: "read", resources: ["a.txt"] },
      })
      // The fiber parked: observed blocked but not yet settled.
      expect(fiber.pollUnsafe()).toBeUndefined()

      yield* SessionMessageUpdater.update(store.adapter, blocked)
      const pinned = toolOf(store.current(), toolID)
      expect(pinned?.state.status).toBe("blocked")
      if (pinned?.state.status !== "blocked") return yield* Effect.die("tool did not enter blocked")
      expect(pinned.state.input).toEqual(input)
      expect(pinned.state.permission).toEqual({ action: "read", resources: ["a.txt"] })

      yield* service.reply({ requestID: request.id, reply: "once" })
      const resumed = yield* Deferred.await(resumedEvent)
      expect(resumed.data).toMatchObject({ assistantMessageID: assistantID, id: toolID })
      yield* Fiber.join(fiber)

      yield* SessionMessageUpdater.update(store.adapter, resumed)
      const running = toolOf(store.current(), toolID)
      expect(running?.state.status).toBe("running")
      if (running?.state.status !== "running") return yield* Effect.die("tool did not resume to running")
      expect(running.state.input).toEqual(input)
    }),
  )

  it.effect("skips blocked and resumed for non-tool asks", () =>
    Effect.gen(function* () {
      yield* setup([])
      const bus = yield* Bus.Service
      const service = yield* Permission.Service
      const requestID = Permission.ID.create("per_question_1")

      const observed = yield* Ref.make<Array<string>>([])
      const asked = yield* Deferred.make<Permission.Request>()
      const unsubscribe = yield* bus.listen((event) => {
        if (event.type === Permission.Event.Asked.type) {
          const request = event.data as Permission.Request
          if (request.id === requestID) return Deferred.succeed(asked, request).pipe(Effect.asVoid)
        }
        if (event.type === SessionEvent.Tool.Blocked.type || event.type === SessionEvent.Tool.Resumed.type)
          return Ref.update(observed, (list) => [...list, event.type]).pipe(Effect.asVoid)
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const fiber = yield* service
        .assert({
          id: requestID,
          sessionID: Session.ID.make("ses_test"),
          action: "question",
          resources: ["*"],
        })
        .pipe(Effect.forkScoped)
      const request = yield* Deferred.await(asked)
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* service.reply({ requestID: request.id, reply: "once" })
      yield* Fiber.join(fiber)
      expect(yield* Ref.get(observed)).toEqual([])
    }),
  )

  it.effect("denies immediately without entering blocked", () =>
    Effect.gen(function* () {
      yield* setup([{ action: "read", resource: "*", effect: "deny" }])
      const bus = yield* Bus.Service
      const service = yield* Permission.Service

      const observed = yield* Ref.make<Array<string>>([])
      const unsubscribe = yield* bus.listen((event) => {
        if (event.type === SessionEvent.Tool.Blocked.type || event.type === SessionEvent.Tool.Resumed.type)
          return Ref.update(observed, (list) => [...list, event.type]).pipe(Effect.asVoid)
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const failure = yield* service
        .assert({
          id: Permission.ID.create("per_deny_1"),
          sessionID: Session.ID.make("ses_test"),
          action: "read",
          resources: ["a.txt"],
          source: { type: "tool", messageID: SessionMessage.ID.create(), id: "call_deny_1" },
        })
        .pipe(Effect.flip)
      expect(failure).toBeInstanceOf(Permission.BlockedError)
      expect(yield* service.list()).toEqual([])
      expect(yield* Ref.get(observed)).toEqual([])
    }),
  )
})
