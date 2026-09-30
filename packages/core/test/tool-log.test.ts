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

const err = (settled: { status: string; error?: { message?: string } }) =>
  settled.status === "error" ? (settled.error?.message ?? "") : ""

describe("LogTool", () => {
  it.live("adds rows with team attribution and validates input", () =>
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
          const database = yield* Database.Service
          let calls = 0
          const call = (sessionID: Session.ID, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${tool}-${(calls += 1)}`, name: tool, input },
            })

          const added = yield* call(memberSession.id, "log_add", {
            kind: "finding",
            summary: "要約です",
            body: "本文",
            tags: ["triage"],
          })
          expect(added.status).toBe("completed")
          expect(text(added)).toMatch(/^L\d+$/)

          const seq = Number(text(added).slice(1))
          const stored = yield* database.db.select().from(LogTable).where(eq(LogTable.seq, seq)).get().pipe(Effect.orDie)
          expect(stored?.agent).toBe("survey-1")
          expect(stored?.team).toBe("survey")
          expect(stored?.session_id).toBe(memberSession.id)

          const bossAdded = yield* call(parent.id, "log_add", { kind: "note", summary: "boss memo" })
          expect(bossAdded.status).toBe("completed")
          const bossStored = yield* database.db
            .select()
            .from(LogTable)
            .where(eq(LogTable.seq, Number(text(bossAdded).slice(1))))
            .get()
            .pipe(Effect.orDie)
          expect(bossStored?.agent).toBe("Boss")
          expect(bossStored?.team).toBeNull()

          for (const bad of [
            { input: { kind: "finding", summary: "a\nb" }, reason: "single line" },
            { input: { kind: "finding", summary: "x".repeat(101) }, reason: "100" },
            { input: { kind: "bogus", summary: "s" }, reason: "bogus" },
            { input: { kind: "note", summary: "s", tags: ["Upper"] }, reason: "Upper" },
            { input: { kind: "note", summary: "s", tags: Array.from({ length: 11 }, (_, i) => `t${i}`) }, reason: "10" },
            { input: { kind: "note", summary: "s", re: "L9999" }, reason: "L9999" },
            { input: { kind: "note", summary: "s", re: "nope" }, reason: "nope" },
          ]) {
            const failed = yield* call(memberSession.id, "log_add", bad.input)
            expect(failed.status).toBe("error")
            expect(err(failed)).toContain(bad.reason)
          }
        }),
      ),
    ),
  )

  it.live("keeps projects isolated across recent, search and get", () =>
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

              const inA = yield* call(sessionA.id, "log_add", { kind: "note", summary: "only in A" })
              const inB = yield* call(sessionB.id, "log_add", { kind: "note", summary: "only in B" })
              expect(inA.status).toBe("completed")
              expect(inB.status).toBe("completed")
              const idB = text(inB)

              const recent = yield* call(sessionA.id, "log_recent", {})
              expect(recent.status).toBe("completed")
              expect(text(recent)).toContain("only in A")
              expect(text(recent)).not.toContain("only in B")

              const search = yield* call(sessionA.id, "log_search", { query: "only" })
              expect(text(search)).toContain("only in A")
              expect(text(search)).not.toContain("only in B")

              const get = yield* call(sessionA.id, "log_get", { ids: [idB] })
              expect(get.status).toBe("error")
              expect(err(get)).toContain(idB)

              const reOther = yield* call(sessionA.id, "log_add", { kind: "note", summary: "x", re: idB })
              expect(reOther.status).toBe("error")
              expect(err(reOther)).toContain(idB)
            }),
          ),
        ),
      ),
    ),
  )

  it.live("filters recent and search by kind, team and tag with limits", () =>
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

          yield* call(memberSession.id, "log_add", { kind: "finding", summary: "filter me one", tags: ["triage"] })
          yield* call(memberSession.id, "log_add", { kind: "question", summary: "filter me two" })
          yield* call(parent.id, "log_add", { kind: "finding", summary: "filter me three" })

          const byKind = yield* call(memberSession.id, "log_recent", { kind: "question" })
          expect(text(byKind)).toContain("filter me two")
          expect(text(byKind)).not.toContain("filter me one")

          const byTeam = yield* call(memberSession.id, "log_recent", { team: "survey" })
          expect(text(byTeam)).toContain("filter me one")
          expect(text(byTeam)).not.toContain("filter me three")

          const byTag = yield* call(memberSession.id, "log_search", { query: "filter", tag: "triage" })
          expect(text(byTag)).toContain("filter me one")
          expect(text(byTag)).not.toContain("filter me two")

          const limited = yield* call(memberSession.id, "log_recent", { n: 1 })
          expect(text(limited).split("\n").filter((line) => line.length > 0)).toHaveLength(1)
        }),
      ),
    ),
  )

  it.live("searches FTS phrases safely and falls back to LIKE for short queries", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const session = yield* sessions.create({ location, title: "s" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
            Effect.provide(locations.get(location)),
          )
          let calls = 0
          const call = (name: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID: session.id,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: "log_search", input },
            })

          const add = (name: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID: session.id,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: "log_add", input },
            })
          yield* add("a", { kind: "finding", summary: "日本語の検索対象" })
          yield* add("b", { kind: "note", summary: "a%b literal" })
          yield* add("c", { kind: "note", summary: "axb decoy" })

          const fts = yield* call("fts", { query: "日本語の検索" })
          expect(fts.status).toBe("completed")
          expect(text(fts)).toContain("日本語の検索対象")

          const like = yield* call("like", { query: "日本" })
          expect(like.status).toBe("completed")
          expect(text(like)).toContain("日本語の検索対象")

          const escaped = yield* call("escaped", { query: "a%" })
          expect(escaped.status).toBe("completed")
          expect(text(escaped)).toContain("a%b literal")
          expect(text(escaped)).not.toContain("axb decoy")

          for (const tricky of ['"quoted" AND x -y', "a%b_c", ""]) {
            if (tricky === "") {
              const empty = yield* call("empty", { query: "" })
              expect(empty.status).toBe("error")
              continue
            }
            const result = yield* call(`tricky-${tricky.length}`, { query: tricky })
            expect(result.status).toBe("completed")
          }
        }),
      ),
    ),
  )

  it.live("gets entries in batch, chains re, and truncates long output", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const session = yield* sessions.create({ location, title: "s" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
            Effect.provide(locations.get(location)),
          )
          let calls = 0
          const add = (name: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID: session.id,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: "log_add", input },
            })
          const run = (name: string, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID: session.id,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: tool, input },
            })

          const first = yield* add("first", { kind: "finding", summary: "original", body: "details", tags: ["t"] })
          const id1 = text(first)
          const second = yield* add("second", { kind: "decision", summary: "correction", re: id1 })
          const id2 = text(second)

          const got = yield* run("get", "log_get", { ids: [id1, id2] })
          expect(got.status).toBe("completed")
          expect(text(got)).toContain(id1)
          expect(text(got)).toContain("  body: details")
          expect(text(got)).toContain("  tags: t")
          expect(text(got)).toContain(`  re: ${id1}`)

          const big = "z".repeat(6000)
          const b1 = text(yield* add("big-1", { kind: "note", summary: "big one", body: big }))
          const b2 = text(yield* add("big-2", { kind: "note", summary: "big two", body: big }))
          const b3 = text(yield* add("big-3", { kind: "note", summary: "big three", body: big }))
          const truncated = yield* run("truncated", "log_get", { ids: [b1, b2, b3] })
          expect(truncated.status).toBe("completed")
          expect(text(truncated)).toContain("truncated")
        }),
      ),
    ),
  )

  it.live("assigns distinct seq under parallel adds", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const session = yield* sessions.create({ location, title: "s" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(
            Effect.provide(locations.get(location)),
          )
          const results = yield* Effect.forEach(
            Array.from({ length: 10 }, (_, index) => index),
            (index) =>
              executeTool(registry, {
                sessionID: session.id,
                ...toolIdentity,
                call: {
                  type: "tool-call" as const,
                  id: `call-parallel-${index}`,
                  name: "log_add",
                  input: { kind: "note", summary: `parallel ${index}` },
                },
              }),
            { concurrency: "unbounded" },
          )
          const ids = results.map((result) => text(result))
          expect(results.every((result) => result.status === "completed")).toBe(true)
          expect(new Set(ids).size).toBe(10)
          expect(ids.every((id) => /^L\d+$/.test(id))).toBe(true)
        }),
      ),
    ),
  )
})
