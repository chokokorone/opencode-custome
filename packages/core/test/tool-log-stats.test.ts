import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { eq } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { Job } from "@opencode-ai/core/job"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Agent } from "@opencode-ai/core/agent"
import { Config } from "@opencode-ai/core/config"
import { Permission } from "@opencode-ai/core/permission"
import { Session } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { makeGlobalNode, makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Global } from "@opencode-ai/util/global"
import { PluginSupervisor } from "@opencode-ai/core/plugin/supervisor"
import { Plugin } from "@opencode-ai/core/plugin"
import { SessionStore } from "@opencode-ai/core/session/store"
import { LogTool } from "@opencode-ai/core/tool/plugin/log"
import { LogTable } from "@opencode-ai/core/log/sql"
import { Tool } from "@opencode-ai/core/tool"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { testEffect } from "./lib/effect"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

// Approval gates must never fire for log tools: any ask/assert/reply attempt
// is a test failure. Every tool call in this file runs behind this stub.
const throwingPermission = Layer.succeed(
  Permission.Service,
  Permission.Service.of({
    ask: () => Effect.die(new Error("permission.ask must not be called by log tools")),
    assert: () => Effect.die(new Error("permission.assert must not be called by log tools")),
    reply: () => Effect.die(new Error("permission.reply must not be called by log tools")),
    get: () => Effect.succeed(undefined),
    forSession: () => Effect.succeed([]),
    list: () => Effect.succeed([]),
  }),
)

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

const logPluginSupervisor = makeLocationNode({
  name: "test/log-plugins",
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      yield* registerToolPlugin(LogTool.Plugin)
    }),
  ),
  deps: [Agent.node, Config.node, Permission.node, Session.node, SessionTeam.node, Job.node, Tool.node, Database.node],
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

const it = testEffect(
  AppNodeBuilder.build(nodes, [
    SessionExecution.node.replace(executionNode),
    Global.node.replace(tempGlobalLayer),
    PluginSupervisor.node.replace(logPluginSupervisor),
    Permission.node.replace(throwingPermission),
  ]),
)

const text = (settled: { content?: ReadonlyArray<Tool.Content> }) =>
  (settled.content ?? []).map((part) => (part.type === "text" ? part.text : "")).join("\n")

describe("LogTool.log_stats", () => {
  it.live("counts rows by kind and team with project isolation", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dirA) => Effect.promise(() => dirA[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dirA) =>
        Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (dirB) => Effect.promise(() => dirB[Symbol.asyncDispose]()),
        ).pipe(
          Effect.flatMap((dirB) =>
            Effect.gen(function* () {
              const locationA = Location.Ref.make({ directory: AbsolutePath.make(dirA.path) })
              const locationB = Location.Ref.make({ directory: AbsolutePath.make(dirB.path) })
              const sessions = yield* Session.Service
              const sessionA = yield* sessions.create({ location: locationA, title: "a" })
              const sessionB = yield* sessions.create({ location: locationB, title: "b" })
              const locations = yield* LocationServiceMap.Service
              const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(locationA)))
              yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
                Effect.provide(locations.get(locationA)),
              )
              let calls = 0
              const call = (sessionID: Session.ID, name: string, input: Record<string, unknown>) =>
                executeTool(registry, {
                  sessionID,
                  ...toolIdentity,
                  call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name, input },
                })

              yield* call(sessionA.id, "log_add", { kind: "finding", summary: "alpha one" })
              yield* call(sessionA.id, "log_add", { kind: "finding", summary: "alpha two" })
              yield* call(sessionA.id, "log_add", { kind: "note", summary: "alpha three" })
              yield* call(sessionB.id, "log_add", { kind: "note", summary: "beta only" })

              const statsA = yield* call(sessionA.id, "log_stats", {})
              expect(statsA.status).toBe("completed")
              const outA = text(statsA)
              expect(outA).toContain("total: 3 (archived hidden)")
              expect(outA).toContain("finding 2")
              expect(outA).toContain("note 1")
              expect(outA).toContain("by team: - 3")
              expect(outA).toContain("alpha three")
              expect(outA).not.toContain("beta only")
              expect(outA).toMatch(/^latest: L\d+ \| /m)

              const statsB = yield* call(sessionB.id, "log_stats", {})
              expect(statsB.status).toBe("completed")
              expect(text(statsB)).toContain("total: 1 (archived hidden)")
              expect(text(statsB)).toContain("beta only")
              expect(text(statsB)).not.toContain("alpha")
            }),
          ),
        ),
      ),
    ),
  )

  it.live("honors kind and team filters", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const memberSession = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: memberSession.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
            Effect.provide(locations.get(location)),
          )
          let calls = 0
          const call = (sessionID: Session.ID, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${tool}-${(calls += 1)}`, name: tool, input },
            })

          yield* call(memberSession.id, "log_add", { kind: "finding", summary: "filter one" })
          yield* call(memberSession.id, "log_add", { kind: "question", summary: "filter two" })
          yield* call(parent.id, "log_add", { kind: "finding", summary: "filter three" })

          const all = yield* call(memberSession.id, "log_stats", {})
          expect(all.status).toBe("completed")
          expect(text(all)).toContain("total: 3 (archived hidden)")
          expect(text(all)).toContain("finding 2")
          expect(text(all)).toContain("question 1")
          expect(text(all)).toContain("survey 2")

          const byKind = yield* call(memberSession.id, "log_stats", { kind: "finding" })
          expect(byKind.status).toBe("completed")
          expect(text(byKind)).toContain("total: 2 (archived hidden)")
          expect(text(byKind)).toContain("finding 2")
          expect(text(byKind)).not.toContain("question")

          const byTeam = yield* call(memberSession.id, "log_stats", { team: "survey" })
          expect(byTeam.status).toBe("completed")
          expect(text(byTeam)).toContain("total: 2 (archived hidden)")
          expect(text(byTeam)).toContain("survey 2")
          expect(text(byTeam)).toContain("filter two")
          expect(text(byTeam)).not.toContain("filter three")

          const both = yield* call(memberSession.id, "log_stats", { kind: "finding", team: "survey" })
          expect(both.status).toBe("completed")
          expect(text(both)).toContain("total: 1 (archived hidden)")
        }),
      ),
    ),
  )

  it.live("excludes archived rows and handles empty projects", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dirA) => Effect.promise(() => dirA[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dirA) =>
        Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (dirB) => Effect.promise(() => dirB[Symbol.asyncDispose]()),
        ).pipe(
          Effect.flatMap((dirB) =>
            Effect.gen(function* () {
              const locationA = Location.Ref.make({ directory: AbsolutePath.make(dirA.path) })
              const locationB = Location.Ref.make({ directory: AbsolutePath.make(dirB.path) })
              const sessions = yield* Session.Service
              const sessionA = yield* sessions.create({ location: locationA, title: "a" })
              const sessionB = yield* sessions.create({ location: locationB, title: "b" })
              const locations = yield* LocationServiceMap.Service
              const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(locationA)))
              yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
                Effect.provide(locations.get(locationA)),
              )
              let calls = 0
              const call = (sessionID: Session.ID, name: string, input: Record<string, unknown>) =>
                executeTool(registry, {
                  sessionID,
                  ...toolIdentity,
                  call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name, input },
                })

              yield* call(sessionA.id, "log_add", { kind: "note", summary: "keep me" })
              const archived = yield* call(sessionA.id, "log_add", { kind: "note", summary: "archive me" })
              expect(archived.status).toBe("completed")
              const archivedSeq = Number(text(archived).slice(1))
              const database = yield* Database.Service
              yield* database.db
                .update(LogTable)
                .set({ archived_at: Date.now() })
                .where(eq(LogTable.seq, archivedSeq))
                .run()
                .pipe(Effect.orDie)

              const stats = yield* call(sessionA.id, "log_stats", {})
              expect(stats.status).toBe("completed")
              const out = text(stats)
              expect(out).toContain("total: 1 (archived hidden)")
              expect(out).toContain("keep me")
              expect(out).not.toContain("archive me")

              const empty = yield* call(sessionB.id, "log_stats", {})
              expect(empty.status).toBe("completed")
              const emptyOut = text(empty)
              expect(emptyOut).toContain("total: 0 (archived hidden)")
              expect(emptyOut).toContain("by kind: -")
              expect(emptyOut).toContain("by team: -")
              expect(emptyOut).toContain("latest: none")
            }),
          ),
        ),
      ),
    ),
  )
})
