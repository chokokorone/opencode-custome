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
import { Shell } from "@opencode-ai/core/shell"
import { ShellSelect } from "@opencode-ai/core/shell/select"
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
import { EditTool } from "@opencode-ai/core/tool/plugin/edit"
import { ShellTool } from "@opencode-ai/core/tool/plugin/shell"
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

const teamPluginSupervisor = makeLocationNode({
  name: "test/team-plugins",
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      yield* registerToolPlugin(SubagentTool.Plugin)
      yield* registerToolPlugin(TeamTool.Plugin)
      yield* registerToolPlugin(WriteTool.Plugin)
      yield* registerToolPlugin(EditTool.Plugin)
      yield* registerToolPlugin(ShellTool.Plugin)
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
    Shell.node,
    ShellSelect.node,
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
    PluginSupervisor.node.replace(teamPluginSupervisor),
  ]),
)

const text = (settled: { content?: ReadonlyArray<Tool.Content> }) =>
  (settled.content ?? []).map((part) => (part.type === "text" ? part.text : "")).join("\n")

const inboxTexts = (sessions: Session.Interface, sessionID: Session.ID) =>
  Effect.gen(function* () {
    return (yield* sessions.inbox(sessionID)).filter((item) => item.type === "user").map((item) => item.payload.text)
  })

describe("TeamTool", () => {
  it.live("delivers peer messages between team members and the boss", () =>
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
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const call = (sessionID: Session.ID, id: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "message_to_peer", input },
            })

          const toMember = yield* call(leader.id, "call-leader-to-member", { to: "survey-2", text: "status?" })
          expect(toMember.status).toBe("completed")
          expect(text(toMember)).toContain("Message sent to survey-2.")
          expect(yield* inboxTexts(sessions, member.id)).toEqual(["From survey-1 (leader):\nstatus?"])

          const toLeader = yield* call(member.id, "call-member-to-leader", { to: "survey-1", text: "done" })
          expect(toLeader.status).toBe("completed")
          expect(yield* inboxTexts(sessions, leader.id)).toEqual(["From survey-2 (member):\ndone"])

          const toBoss = yield* call(leader.id, "call-leader-to-boss", { to: "Boss", text: "report" })
          expect(toBoss.status).toBe("completed")
          expect(yield* inboxTexts(sessions, parent.id)).toEqual(["From survey-1 (leader):\nreport"])

          const memberToBoss = yield* call(member.id, "call-member-to-boss", { to: "Boss", text: "hi" })
          expect(memberToBoss).toEqual({
            status: "error",
            error: { type: "tool.execution", message: expect.stringContaining("Only the leader can message Boss") },
          })
          expect(memberToBoss.error?.message).toContain("via the leader")

          const fromBoss = yield* call(parent.id, "call-boss-to-member", { to: "survey-2", text: "keep going" })
          expect(fromBoss.status).toBe("completed")
          expect(yield* inboxTexts(sessions, member.id)).toEqual([
            "From survey-1 (leader):\nstatus?",
            "From Boss:\nkeep going",
          ])

          const unknown = yield* call(leader.id, "call-unknown-peer", { to: "Nobody", text: "hello" })
          expect(unknown).toEqual({
            status: "error",
            error: { type: "tool.execution", message: expect.stringContaining('No roster entry named "Nobody"') },
          })
          expect(unknown.error?.message).toContain("- survey-2 (member)")

          const member2 = yield* sessions.create({ parentID: parent.id, title: "member2" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: member2.id })

          const memberToMember = yield* call(member.id, "call-member-to-member", {
            to: "survey-3",
            text: "heads up",
          })
          expect(memberToMember.status).toBe("completed")
          expect(text(memberToMember)).toContain("Message sent to survey-3.")
          expect(yield* inboxTexts(sessions, member2.id)).toEqual(["From survey-2 (member):\nheads up"])

          const bossToLeader = yield* call(parent.id, "call-boss-to-leader", { to: "survey-1", text: "noted" })
          expect(bossToLeader.status).toBe("completed")
          expect(yield* inboxTexts(sessions, leader.id)).toEqual([
            "From survey-2 (member):\ndone",
            "From Boss:\nnoted",
          ])
        }),
      ),
    ),
  )

  it.live("blocks Boss-addressing variants from members", () =>
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
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const variants = ["boss", "BOSS", " Boss ", parent.id, ""]
          yield* Effect.forEach(
            variants,
            (to, index) =>
              Effect.gen(function* () {
                const result = yield* executeTool(registry, {
                  sessionID: member.id,
                  ...toolIdentity,
                  call: {
                    type: "tool-call" as const,
                    id: `call-member-variant-${index}`,
                    name: "message_to_peer",
                    input: { to, text: "hi" },
                  },
                })
                expect(result.status).toBe("error")
              }),
            { discard: true },
          )
          expect(yield* inboxTexts(sessions, parent.id)).toEqual([])
        }),
      ),
    ),
  )

  it.live("renders the team roster per role and rejects non-participants", () =>
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
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          const outsider = yield* sessions.create({ location, title: "outsider" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const roster = (sessionID: Session.ID, id: string) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "team_roster", input: {} },
            })

          const memberRoster = yield* roster(member.id, "call-member-roster")
          const memberLines = text(memberRoster)
          expect(memberLines).toContain("Team survey:")
          expect(memberLines).toContain("- survey-1 (leader) — the only member who can message Boss")
          expect(memberLines).toContain("- survey-2 (member) — you")
          expect(memberLines).not.toContain("your manager")

          const leaderRoster = yield* roster(leader.id, "call-leader-roster")
          const leaderLines = text(leaderRoster)
          expect(leaderLines).toContain("- survey-1 (leader) — the only member who can message Boss — you")
          expect(leaderLines).toContain("- Boss — your manager; only you (the leader) can message it")

          const bossRoster = yield* roster(parent.id, "call-boss-roster")
          const bossLines = text(bossRoster)
          expect(bossLines).toContain("Team survey:")
          expect(bossLines).toContain("- survey-1 (leader)")
          expect(bossLines).toContain("- survey-2 (member)")
          expect(bossLines).not.toContain("— you")

          const outsiderRoster = yield* roster(outsider.id, "call-outsider-roster")
          expect(outsiderRoster).toEqual({
            status: "error",
            error: {
              type: "tool.execution",
              message: expect.stringContaining("team_roster is only available to members of a team"),
            },
          })
        }),
      ),
    ),
  )
})

describe("TeamTool rejection events", () => {
  it.live("publishes team.message.rejected when the router denies a message", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const bus = yield* Bus.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "survey", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const rejected = yield* bus.subscribe(SessionEvent.TeamMessageRejected).pipe(
            Stream.filter((event) => event.data.sessionID === member.id),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )
          const sent = yield* bus.subscribe(SessionEvent.TeamMessageSent).pipe(
            Stream.filter((event) => event.data.teamID === "survey"),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped({ startImmediately: true }),
          )
          const ok = yield* executeTool(registry, {
            sessionID: leader.id,
            ...toolIdentity,
            call: { type: "tool-call" as const, id: "call-sent-event", name: "message_to_peer", input: { to: "survey-2", text: "go" } },
          })
          expect(ok.status).toBe("completed")
          const sentEvents = Array.from(yield* Fiber.join(sent))
          expect(sentEvents).toHaveLength(1)
          expect(sentEvents[0]?.data).toMatchObject({ teamID: "survey", to: "survey-2" })
          const denied = yield* executeTool(registry, {
            sessionID: member.id,
            ...toolIdentity,
            call: { type: "tool-call" as const, id: "call-rejected-event", name: "message_to_peer", input: { to: "Boss", text: "hi" } },
          })
          expect(denied.status).toBe("error")
          const events = Array.from(yield* Fiber.join(rejected))
          expect(events).toHaveLength(1)
          expect(events[0]?.data).toMatchObject({
            teamID: "survey",
            from: "survey-2 (member)",
            to: "Boss",
          })
          expect(events[0]?.data.reason).toContain("Only the leader can message Boss")
        }),
      ),
    ),
  )
})

describe("TeamTool workspaces", () => {
  it.live("moves team members into per-member workspace directories", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, title: "boss" })
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

          const spawned = yield* executeTool(registry, {
            sessionID: parent.id,
            ...toolIdentity,
            call: {
              type: "tool-call" as const,
              id: "call-spawn-ws",
              name: "subagent",
              input: { agent: "reviewer", description: "workspace check", team: "site" },
            },
          })
          expect(spawned.status).toBe("completed")
          const childID = spawned.metadata?.sessionID as Session.ID
          expect(childID).toBeDefined()
          // The move is async: it waits in the inbox until the dormant member
          // first wakes. Assert the pending Move item and the directory itself.
          const inbox = yield* sessions.inbox(childID)
          const moves = inbox.filter((item) => item.type === "move")
          expect(moves).toHaveLength(1)
          const directory = (moves[0] as { payload: { location: { directory: string } } }).payload.location.directory
          expect(directory.endsWith("/workspace/site/site-1")).toBe(true)
          const stat = yield* Effect.promise(() => import("fs/promises").then((fs) => fs.stat(directory)))
          expect(stat.isDirectory()).toBe(true)
        }),
      ),
    ),
  )
})

describe("TeamTool path ownership", () => {
  it.live("confines member writes to their workspace and test areas", () =>
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
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "own", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "own", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
          yield* Agent.Service.use((agents) =>
            agents.transform((editor) => {
              editor.update(toolIdentity.agent, (agent) => {
                agent.permissions.push({ action: "*", resource: "*", effect: "allow" })
              })
            }),
          ).pipe(Effect.provide(locations.get(location)))
          let calls = 0
          const call = (sessionID: Session.ID, name: string, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: tool, input },
            })

          const wsFile = "workspace/own/own-2/notes.txt"
          const written = yield* call(member.id, "ws-write", "write", { path: wsFile, content: "member notes" })
          expect(written.status).toBe("completed")

          const edited = yield* call(member.id, "ws-edit", "edit", {
            path: wsFile,
            oldString: "member notes",
            newString: "member notes v2",
          })
          expect(edited.status).toBe("completed")

          const outside = yield* call(member.id, "outside", "write", { path: "shared.txt", content: "x" })
          expect(outside.status).toBe("error")
          expect(outside.error?.message).toContain("outside")

          const sibling = yield* call(member.id, "sibling", "write", {
            path: "workspace/own/own-1/other.txt",
            content: "x",
          })
          expect(sibling.status).toBe("error")
          expect(sibling.error?.message).toContain("Ask the leader")

          const testArea = yield* call(member.id, "test-area", "write", {
            path: "test/own/own-2/case.txt",
            content: "x",
          })
          expect(testArea.status).toBe("completed")

          const leaderWrite = yield* call(leader.id, "leader-write", "write", { path: "shared.txt", content: "x" })
          expect(leaderWrite.status).toBe("completed")

          const shellIn = yield* call(member.id, "shell-in", "shell", { command: "echo hi", workdir: "workspace/own/own-2" })
          expect(shellIn.status).toBe("completed")

          const shellOut = yield* call(member.id, "shell-out", "shell", { command: "echo hi", workdir: "." })
          expect(shellOut.status).toBe("error")
          expect(shellOut.error?.message).toContain("outside")
        }),
      ),
    ),
  )
})

describe("TeamTool prompt injection", () => {
  it.live("injects log, wait and path guidance into member context", () =>
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
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          const membership = yield* team.register({ parentID: parent.id, teamID: "guide", sessionID: member.id })
          const text = TeamTool.memberRules(membership)
          expect(text).toContain("log_add")
          expect(text).toContain("tool_wait")
          expect(text).toContain("workspace/guide/")
          expect(text).toContain(membership.name)
        }),
      ),
    ),
  )
})

describe("TeamTool git worktrees", () => {
  it.live("creates a linked worktree on spawn and removes it with the session", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          yield* Effect.promise(async () => {
            const run = (args: string[]) => {
              const proc = Bun.spawnSync(["git", ...args], {
                cwd: dir.path,
                env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" },
              })
              if (proc.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed`)
            }
            run(["init", "-q"])
            run(["config", "user.email", "test@test"])
            run(["config", "user.name", "test"])
            await Bun.write(`${dir.path}/README.md`, "repo\n")
            run(["add", "."])
            run(["commit", "-q", "-m", "init"])
          })
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, title: "boss" })
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
          const call = (sessionID: Session.ID, name: string, tool: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id: `call-${name}-${(calls += 1)}`, name: tool, input },
            })

          const spawned = yield* call(parent.id, "git-spawn", "subagent", {
            agent: "reviewer",
            description: "git worktree check",
            team: "repo",
          })
          expect(spawned.status).toBe("completed")
          const childID = spawned.metadata?.sessionID as Session.ID
          const worktrees = yield* Effect.promise(async () => {
            const proc = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], { cwd: dir.path })
            return proc.stdout.toString()
          })
          expect(worktrees).toContain("workspace/repo/repo-1")

          yield* sessions.remove(childID)
          const after = yield* Effect.promise(async () => {
            const proc = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], { cwd: dir.path })
            return proc.stdout.toString()
          })
          expect(after).not.toContain("workspace/repo/repo-1")
        }),
      ),
    ),
  )

  it.live("denies worktree creation when the policy says so", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          yield* Effect.promise(async () => {
            const run = (args: string[]) => {
              const proc = Bun.spawnSync(["git", ...args], {
                cwd: dir.path,
                env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" },
              })
              if (proc.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed`)
            }
            run(["init", "-q"])
            run(["config", "user.email", "test@test"])
            run(["config", "user.name", "test"])
            run(["commit", "-q", "--allow-empty", "-m", "init"])
          })
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
          yield* Agent.Service.use((agents) =>
            agents.transform((editor) => {
              editor.update(toolIdentity.agent, (agent) => {
                agent.mode = "primary"
                agent.permissions.push({ action: "*", resource: "*", effect: "allow" })
                agent.permissions.push({ action: "worktree", resource: "*", effect: "deny" })
              })
              editor.update(Agent.ID.make("reviewer"), (agent) => {
                agent.mode = "subagent"
              })
            }),
          ).pipe(Effect.provide(locations.get(location)))
          let calls = 0
          const denied = yield* executeTool(registry, {
            sessionID: parent.id,
            ...toolIdentity,
            call: {
              type: "tool-call" as const,
              id: `call-deny-${(calls += 1)}`,
              name: "subagent",
              input: { agent: "reviewer", description: "denied worktree", team: "repo" },
            },
          })
          expect(denied.status).toBe("error")
          expect(denied).toEqual({
            status: "error",
            error: { type: "permission.rejected", message: expect.stringContaining("Permission denied: worktree") },
          })
        }),
      ),
    ),
  )
})

describe("TeamTool worktree dirty handling", () => {
  it.live("keeps dirty worktrees on session removal", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          yield* Effect.promise(async () => {
            const run = (args: string[]) => {
              const proc = Bun.spawnSync(["git", ...args], {
                cwd: dir.path,
                env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" },
              })
              if (proc.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed`)
            }
            run(["init", "-q"])
            run(["config", "user.email", "test@test"])
            run(["config", "user.name", "test"])
            run(["commit", "-q", "--allow-empty", "-m", "init"])
          })
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, title: "boss" })
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
          const spawned = yield* executeTool(registry, {
            sessionID: parent.id,
            ...toolIdentity,
            call: {
              type: "tool-call" as const,
              id: `call-dirty-${(calls += 1)}`,
              name: "subagent",
              input: { agent: "reviewer", description: "dirty check", team: "repo" },
            },
          })
          expect(spawned.status).toBe("completed")
          const childID = spawned.metadata?.sessionID as Session.ID
          const worktree = `${dir.path}/workspace/repo/repo-1`
          yield* Effect.promise(() => Bun.write(`${worktree}/draft.txt`, "uncommitted work\n"))
          yield* sessions.remove(childID)
          const listed = yield* Effect.promise(async () => {
            const proc = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], { cwd: dir.path })
            return proc.stdout.toString()
          })
          expect(listed).toContain("workspace/repo/repo-1")
        }),
      ),
    ),
  )
})

describe("TeamTool §8 routing table", () => {
  it.live("covers every routing cell end-to-end through message_to_peer", () =>
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
          const leader = yield* sessions.create({ parentID: parent.id, title: "leader" })
          const member = yield* sessions.create({ parentID: parent.id, title: "member" })
          yield* team.register({ parentID: parent.id, teamID: "matrix", sessionID: leader.id })
          yield* team.register({ parentID: parent.id, teamID: "matrix", sessionID: member.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const call = (sessionID: Session.ID, id: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "message_to_peer", input },
            })

          // Boss → Leader (allowed).
          const bossToLeader = yield* call(parent.id, "call-matrix-boss-to-leader", {
            to: "matrix-1",
            text: "b-to-l",
          })
          expect(bossToLeader.status).toBe("completed")
          expect(text(bossToLeader)).toContain("Message sent to matrix-1.")
          expect(yield* inboxTexts(sessions, leader.id)).toEqual(["From Boss:\nb-to-l"])

          // Boss → Member (allowed).
          const bossToMember = yield* call(parent.id, "call-matrix-boss-to-member", {
            to: "matrix-2",
            text: "b-to-m",
          })
          expect(bossToMember.status).toBe("completed")
          expect(yield* inboxTexts(sessions, member.id)).toEqual(["From Boss:\nb-to-m"])

          // Leader → Boss (allowed).
          const leaderToBoss = yield* call(leader.id, "call-matrix-leader-to-boss", {
            to: "Boss",
            text: "l-to-b",
          })
          expect(leaderToBoss.status).toBe("completed")
          expect(yield* inboxTexts(sessions, parent.id)).toEqual(["From matrix-1 (leader):\nl-to-b"])

          // Leader → Member (allowed).
          const leaderToMember = yield* call(leader.id, "call-matrix-leader-to-member", {
            to: "matrix-2",
            text: "l-to-m",
          })
          expect(leaderToMember.status).toBe("completed")
          expect(yield* inboxTexts(sessions, member.id)).toEqual([
            "From Boss:\nb-to-m",
            "From matrix-1 (leader):\nl-to-m",
          ])

          // Member → Leader (allowed).
          const memberToLeader = yield* call(member.id, "call-matrix-member-to-leader", {
            to: "matrix-1",
            text: "m-to-l",
          })
          expect(memberToLeader.status).toBe("completed")
          expect(yield* inboxTexts(sessions, leader.id)).toEqual([
            "From Boss:\nb-to-l",
            "From matrix-2 (member):\nm-to-l",
          ])

          // Member → Member (allowed).
          const member2 = yield* sessions.create({ parentID: parent.id, title: "member2" })
          yield* team.register({ parentID: parent.id, teamID: "matrix", sessionID: member2.id })
          const memberToMember = yield* call(member.id, "call-matrix-member-to-member", {
            to: "matrix-3",
            text: "m-to-m",
          })
          expect(memberToMember.status).toBe("completed")
          expect(yield* inboxTexts(sessions, member2.id)).toEqual(["From matrix-2 (member):\nm-to-m"])

          // Member → Boss (denied with leader guidance).
          const memberToBoss = yield* call(member.id, "call-matrix-member-to-boss", {
            to: "Boss",
            text: "m-to-b-denied",
          })
          expect(memberToBoss.status).toBe("error")
          expect(memberToBoss.error?.message).toContain("Only the leader can message Boss")
          expect(memberToBoss.error?.message).toContain("via the leader")
          expect(yield* inboxTexts(sessions, parent.id)).toEqual(["From matrix-1 (leader):\nl-to-b"])
        }),
      ),
    ),
  )
})

describe("TeamTool §9 config override", () => {
  it.live("honors per-team rules end-to-end, including member to Boss allow", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          // The tool-team harness serves the real Config (Config.node via
          // LocationServiceMap), not Config.Test: nodes lack a
          // Config.node.replace(Config.testLayer()) unlike
          // test/plugin/fixture.ts:103. Seed rules through opencode.json
          // before the location layer first loads, mirroring
          // test/tool-subagent.test.ts config seeding. Config.Test.setEntries
          // coverage lives in test/config/team.test.ts.
          yield* Effect.promise(() =>
            Bun.write(
              `${dir.path}/opencode.json`,
              JSON.stringify({
                teams: [
                  { teamID: "open", rules: [{ from: "member", to: "boss", effect: "allow" }] },
                  { teamID: "locked", rules: [{ from: "member", to: "peer", effect: "deny" }] },
                ],
              }),
            ),
          )
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const openLeader = yield* sessions.create({ parentID: parent.id, title: "open-leader" })
          const openMember = yield* sessions.create({ parentID: parent.id, title: "open-member" })
          const lockedLeader = yield* sessions.create({ parentID: parent.id, title: "locked-leader" })
          const lockedMember = yield* sessions.create({ parentID: parent.id, title: "locked-member" })
          const lockedMember2 = yield* sessions.create({ parentID: parent.id, title: "locked-member2" })
          yield* team.register({ parentID: parent.id, teamID: "open", sessionID: openLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "open", sessionID: openMember.id })
          yield* team.register({ parentID: parent.id, teamID: "locked", sessionID: lockedLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "locked", sessionID: lockedMember.id })
          yield* team.register({ parentID: parent.id, teamID: "locked", sessionID: lockedMember2.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const entries = yield* Config.Service.pipe(
            Effect.flatMap((service) => service.entries()),
            Effect.provide(locations.get(location)),
          )
          const configured = Config.latest(entries, "teams")
          expect(configured?.find((entry) => entry.teamID === "open")?.rules).toHaveLength(1)
          expect(configured?.find((entry) => entry.teamID === "locked")?.rules).toHaveLength(1)

          const call = (sessionID: Session.ID, id: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "message_to_peer", input },
            })

          // Working override: locked member to member is denied end-to-end
          // (default allows, see §8 test). Proves per-team rules reach the tool.
          const lockedDenied = yield* call(lockedMember.id, "call-locked-member-to-member", {
            to: "locked-3",
            text: "hi",
          })
          expect(lockedDenied.status).toBe("error")
          expect(lockedDenied.error?.message).toContain("not allowed")
          expect(yield* inboxTexts(sessions, lockedMember2.id)).toEqual([])

          // Per-team scoping: the locked deny does not leak into the open team.
          const openAllowed = yield* call(openMember.id, "call-open-member-to-leader", {
            to: "open-1",
            text: "hi",
          })
          expect(openAllowed.status).toBe("completed")
          expect(yield* inboxTexts(sessions, openLeader.id)).toEqual(["From open-2 (member):\nhi"])

          // Config-allowed member to Boss delivers to the parent session.
          const openToBoss = yield* call(openMember.id, "call-open-member-to-boss", {
            to: "Boss",
            text: "open-hello",
          })
          expect(openToBoss.status).toBe("completed")
          expect(yield* inboxTexts(sessions, parent.id)).toEqual(["From open-2 (member):\nopen-hello"])
        }),
      ),
    ),
  )
})

describe("TeamTool §22 session separation", () => {
  it.live("keeps team A deliveries out of team B member inboxes", () =>
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
          const alphaLeader = yield* sessions.create({ parentID: parent.id, title: "alpha-leader" })
          const alphaMember = yield* sessions.create({ parentID: parent.id, title: "alpha-member" })
          const betaLeader = yield* sessions.create({ parentID: parent.id, title: "beta-leader" })
          const betaMember = yield* sessions.create({ parentID: parent.id, title: "beta-member" })
          yield* team.register({ parentID: parent.id, teamID: "alpha", sessionID: alphaLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "alpha", sessionID: alphaMember.id })
          yield* team.register({ parentID: parent.id, teamID: "beta", sessionID: betaLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "beta", sessionID: betaMember.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const call = (sessionID: Session.ID, id: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "message_to_peer", input },
            })

          const alpha = yield* call(alphaLeader.id, "call-alpha-to-member", {
            to: "alpha-2",
            text: "alpha-secret",
          })
          expect(alpha.status).toBe("completed")
          expect(yield* inboxTexts(sessions, alphaMember.id)).toEqual([
            "From alpha-1 (leader):\nalpha-secret",
          ])
          expect(yield* inboxTexts(sessions, betaMember.id)).toEqual([])
          expect(yield* inboxTexts(sessions, betaLeader.id)).toEqual([])

          const beta = yield* call(betaLeader.id, "call-beta-to-member", {
            to: "beta-2",
            text: "beta-secret",
          })
          expect(beta.status).toBe("completed")
          expect(yield* inboxTexts(sessions, betaMember.id)).toEqual([
            "From beta-1 (leader):\nbeta-secret",
          ])
          expect(yield* inboxTexts(sessions, alphaMember.id)).toEqual([
            "From alpha-1 (leader):\nalpha-secret",
          ])

          const bossToAlpha = yield* call(parent.id, "call-boss-to-alpha", {
            to: "alpha-2",
            text: "boss-to-alpha",
          })
          expect(bossToAlpha.status).toBe("completed")
          expect(yield* inboxTexts(sessions, alphaMember.id)).toEqual([
            "From alpha-1 (leader):\nalpha-secret",
            "From Boss:\nboss-to-alpha",
          ])
          expect(yield* inboxTexts(sessions, betaMember.id)).toEqual([
            "From beta-1 (leader):\nbeta-secret",
          ])

          const bossToBeta = yield* call(parent.id, "call-boss-to-beta", {
            to: "beta-2",
            text: "boss-to-beta",
          })
          expect(bossToBeta.status).toBe("completed")
          expect(yield* inboxTexts(sessions, betaMember.id)).toEqual([
            "From beta-1 (leader):\nbeta-secret",
            "From Boss:\nboss-to-beta",
          ])
          expect(yield* inboxTexts(sessions, alphaMember.id)).toEqual([
            "From alpha-1 (leader):\nalpha-secret",
            "From Boss:\nboss-to-alpha",
          ])
        }),
      ),
    ),
  )

  it.live("rejects cross-team peer messages with the sender roster only", () =>
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
          const alphaLeader = yield* sessions.create({ parentID: parent.id, title: "alpha-leader" })
          const alphaMember = yield* sessions.create({ parentID: parent.id, title: "alpha-member" })
          const betaLeader = yield* sessions.create({ parentID: parent.id, title: "beta-leader" })
          const betaMember = yield* sessions.create({ parentID: parent.id, title: "beta-member" })
          yield* team.register({ parentID: parent.id, teamID: "alpha", sessionID: alphaLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "alpha", sessionID: alphaMember.id })
          yield* team.register({ parentID: parent.id, teamID: "beta", sessionID: betaLeader.id })
          yield* team.register({ parentID: parent.id, teamID: "beta", sessionID: betaMember.id })
          const locations = yield* LocationServiceMap.Service
          const registry = yield* Tool.Service.pipe(Effect.provide(locations.get(location)))
          yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))

          const call = (sessionID: Session.ID, id: string, input: Record<string, unknown>) =>
            executeTool(registry, {
              sessionID,
              ...toolIdentity,
              call: { type: "tool-call" as const, id, name: "message_to_peer", input },
            })

          const cross = yield* call(alphaMember.id, "call-alpha-to-beta", {
            to: "beta-2",
            text: "sneaky",
          })
          expect(cross.status).toBe("error")
          expect(cross.error?.message).toContain('No roster entry named "beta-2"')
          expect(cross.error?.message).toContain("Team alpha:")
          expect(cross.error?.message).not.toContain("beta-2 (member)")
          expect(yield* inboxTexts(sessions, betaMember.id)).toEqual([])
          expect(yield* inboxTexts(sessions, betaLeader.id)).toEqual([])

          const leaderCross = yield* call(alphaLeader.id, "call-alpha-leader-to-beta", {
            to: "beta-1",
            text: "sneaky",
          })
          expect(leaderCross.status).toBe("error")
          expect(leaderCross.error?.message).toContain('No roster entry named "beta-1"')
          expect(yield* inboxTexts(sessions, betaLeader.id)).toEqual([])
        }),
      ),
    ),
  )
})
