import { describe, expect } from "bun:test"
import { Effect, Fiber, Layer, Stream } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Bus } from "@opencode-ai/core/bus"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { Database } from "@opencode-ai/core/database/database"
import { Job } from "@opencode-ai/core/job"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Agent } from "@opencode-ai/core/agent"
import { Environment } from "@opencode-ai/core/environment/index"
import { FileMutation } from "@opencode-ai/core/file-mutation"
import { Formatter } from "@opencode-ai/core/formatter"
import { LocationMutation } from "@opencode-ai/core/location-mutation"
import { Config } from "@opencode-ai/core/config"
import { Permission } from "@opencode-ai/core/permission"
import { Session } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { Git } from "@opencode-ai/core/git"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { makeGlobalNode, makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Global } from "@opencode-ai/util/global"
import { PluginSupervisor } from "@opencode-ai/core/plugin/supervisor"
import { Plugin } from "@opencode-ai/core/plugin"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SubagentTool } from "@opencode-ai/core/tool/plugin/subagent"
import { TeamTool } from "@opencode-ai/core/tool/plugin/team"
import { WriteTool } from "@opencode-ai/core/tool/plugin/write"
import { LogTool } from "@opencode-ai/core/tool/plugin/log"
import { Tool } from "@opencode-ai/core/tool"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { testEffect } from "./lib/effect"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

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

const e2ePluginSupervisor = makeLocationNode({
  name: "test/team-e2e-plugins",
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      yield* registerToolPlugin(SubagentTool.Plugin)
      yield* registerToolPlugin(TeamTool.Plugin)
      yield* registerToolPlugin(WriteTool.Plugin)
      yield* registerToolPlugin(LogTool.Plugin)
    }),
  ),
  deps: [
    Agent.node,
    Bus.node,
    Config.node,
    FSUtil.node,
    Permission.node,
    Session.node,
    SessionTeam.node,
    Job.node,
    Tool.node,
    Database.node,
    LocationMutation.node,
    FileMutation.node,
    Environment.node,
    Formatter.node,
    Location.node,
    Git.node,
  ],
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
    PluginSupervisor.node.replace(e2ePluginSupervisor),
  ]),
)

const text = (settled: { content?: ReadonlyArray<Tool.Content> }) =>
  (settled.content ?? []).map((part) => (part.type === "text" ? part.text : "")).join("\n")

const inboxTexts = (sessions: Session.Interface, sessionID: Session.ID) =>
  Effect.gen(function* () {
    return (yield* sessions.inbox(sessionID)).filter((item) => item.type === "user").map((item) => item.payload.text)
  })

const gitInit = (directory: string) =>
  Effect.promise(async () => {
    const run = (args: string[]) => {
      const proc = Bun.spawnSync(["git", ...args], { cwd: directory, env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" } })
      if (proc.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed`)
    }
    run(["init", "-q"])
    run(["config", "user.email", "test@test"])
    run(["config", "user.name", "test"])
    run(["commit", "-q", "--allow-empty", "-m", "init"])
  })

const worktreeList = (directory: string) =>
  Effect.promise(async () => {
    const proc = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], { cwd: directory })
    return proc.stdout.toString()
  })

describe("Team end-to-end workflow", () => {
  it.live("runs the full §41 flow: spawn, assign, work, report, clean up", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          yield* gitInit(dir.path)
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const bus = yield* Bus.Service
          const boss = yield* sessions.create({ location, title: "boss" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
          yield* Agent.Service.use((agents) =>
            agents.transform((editor) => {
              editor.update(toolIdentity.agent, (agent) => {
                agent.mode = "primary"
                agent.permissions.push({ action: "*", resource: "*", effect: "allow" })
              })
              editor.update(Agent.ID.make("reviewer"), (agent) => {
                agent.mode = "subagent"
              })
            }),
          ).pipe(Effect.provide(locations.get(location)))
          let calls = 0
          const call = (sessionID: Session.ID, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-e2e-${(calls += 1)}`, name: tool, input },
            })

          const sent = yield* bus.subscribe(SessionEvent.TeamMessageSent).pipe(
            Stream.filter((event) => event.data.teamID === "shop"),
            Stream.take(4),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )

          // 1. Boss spawns the team: leader first, then member.
          const spawnedLeader = yield* call(boss.id, "subagent", {
            agent: "reviewer",
            description: "lead research",
            team: "shop",
          })
          expect(spawnedLeader.status).toBe("completed")
          const leaderID = spawnedLeader.metadata?.sessionID as Session.ID
          const spawnedMember = yield* call(boss.id, "subagent", {
            agent: "reviewer",
            description: "do research",
            team: "shop",
          })
          expect(spawnedMember.status).toBe("completed")
          const memberID = spawnedMember.metadata?.sessionID as Session.ID
          const roster = yield* team.roster((yield* team.membership(leaderID))!)
          expect(roster.map((entry) => `${entry.name} (${entry.role})`)).toEqual([
            "shop-1 (leader)",
            "shop-2 (member)",
          ])
          expect(yield* worktreeList(dir.path)).toContain("workspace/shop/shop-2")

          // 2. Boss assigns the leader; the leader delegates to the member.
          expect((yield* call(boss.id, "message_to_peer", { to: "shop-1", text: "Research auth" })).status).toBe(
            "completed",
          )
          expect((yield* call(leaderID, "message_to_peer", { to: "shop-2", text: "Find auth files" })).status).toBe(
            "completed",
          )
          expect(yield* inboxTexts(sessions, memberID)).toEqual(["From shop-1 (leader):\nFind auth files"])

          // 3. The member works: writes in its workspace and logs the finding.
          const written = yield* call(memberID, "write", {
            path: "workspace/shop/shop-2/auth-notes.txt",
            content: "login lives in src/auth.ts",
          })
          expect(written.status).toBe("completed")
          const logged = yield* call(memberID, "log_add", {
            kind: "finding",
            summary: "auth entry point",
            body: "login lives in src/auth.ts",
            tags: ["auth"],
          })
          expect(logged.status).toBe("completed")
          expect(text(logged)).toMatch(/^L\d+$/)
          const found = yield* call(memberID, "log_search", { query: "auth entry" })
          expect(text(found)).toContain("auth entry point")

          // 4. Results flow back up: member to leader, leader to Boss.
          expect((yield* call(memberID, "message_to_peer", { to: "shop-1", text: "auth found" })).status).toBe(
            "completed",
          )
          expect((yield* call(leaderID, "message_to_peer", { to: "Boss", text: "auth integrated" })).status).toBe(
            "completed",
          )
          expect(yield* inboxTexts(sessions, boss.id)).toEqual(["From shop-1 (leader):\nauth integrated"])

          // 5. Four deliveries happened, all observed as events.
          const deliveries = Array.from(yield* Fiber.join(sent))
          expect(deliveries.map((event) => `${event.data.from} -> ${event.data.to}`).sort()).toEqual(
            ["Boss -> shop-1", "shop-1 (leader) -> shop-2", "shop-2 (member) -> shop-1", "shop-1 (leader) -> Boss"].sort(),
          )

          // 6. Removing the clean leader cleans its worktree; the member's
          // dirty tree (uncommitted notes) survives for manual care.
          yield* sessions.remove(leaderID)
          const after = yield* worktreeList(dir.path)
          expect(after).not.toContain("workspace/shop/shop-1")
          expect(after).toContain("workspace/shop/shop-2")
        }),
      ),
    ),
  )
})
