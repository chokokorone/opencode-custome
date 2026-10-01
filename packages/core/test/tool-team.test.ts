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
