import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { eq } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { SessionTeamTable } from "@opencode-ai/core/session/sql"
import { globalProjectNode } from "./lib/project"
import { testEffect } from "./lib/effect"
import { tmpdirScoped } from "./fixture/tmpdir"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      Project.node,
      SessionProjector.node,
      SessionStore.node,
      Session.node,
      SessionTeam.node,
      LocationServiceMap.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

type DatabaseService = Database.Interface["db"]

/** positions are durable bookkeeping, not part of the public membership. */
const positions = (db: DatabaseService, parentID: Session.ID) =>
  db
    .select({ name: SessionTeamTable.name, position: SessionTeamTable.position })
    .from(SessionTeamTable)
    .where(eq(SessionTeamTable.parent_id, parentID))
    .all()
    .pipe(Effect.orDie)
    .pipe(Effect.map((rows) => rows.map((row) => row.position)))

describe("SessionTeam.register", () => {
  it.live("numbers sequential registrations from one and keeps roles in order", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const { db } = yield* Database.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const child = (title: string) => sessions.create({ parentID: parent.id, title })
          const add = (sessionID: Session.ID, teamID = "survey") =>
            team.register({ parentID: parent.id, teamID, sessionID })

          const first = yield* child("a").pipe(Effect.flatMap((created) => add(created.id)))
          const second = yield* child("b").pipe(Effect.flatMap((created) => add(created.id)))
          const third = yield* child("c").pipe(Effect.flatMap((created) => add(created.id)))

          expect([first.name, second.name, third.name]).toEqual(["survey-1", "survey-2", "survey-3"])
          expect([first.role, second.role, third.role]).toEqual(["leader", "member", "member"])
          expect(yield* positions(db, parent.id)).toEqual([0, 1, 2])
        }),
      ),
    ),
  )

  it.live("assigns unique names and positions under concurrent registration", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const { db } = yield* Database.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const created = yield* Effect.forEach(
            Array.from({ length: 6 }, (_, index) => index),
            (index) => sessions.create({ parentID: parent.id, title: `child-${index}` }),
          )

          const memberships = yield* Effect.forEach(
            created,
            (session) => team.register({ parentID: parent.id, teamID: "survey", sessionID: session.id }),
            { concurrency: "unbounded" },
          )

          const names = memberships.map((membership) => membership.name)
          expect(new Set(names).size).toBe(created.length)
          expect([...names].toSorted()).toEqual(created.map((_, index) => `survey-${index + 1}`).toSorted())
          const stored = yield* positions(db, parent.id)
          expect(new Set(stored).size).toBe(created.length)
          expect(memberships.filter((membership) => membership.role === "leader").length).toBe(1)
        }),
      ),
    ),
  )

  it.live("never reuses a name after a member is removed", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const { db } = yield* Database.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const child = (title: string) => sessions.create({ parentID: parent.id, title })
          const add = (sessionID: Session.ID) => team.register({ parentID: parent.id, teamID: "survey", sessionID })

          const first = yield* child("a").pipe(Effect.flatMap((created) => add(created.id)))
          const secondSession = yield* child("b")
          yield* add(secondSession.id)
          const third = yield* child("c").pipe(Effect.flatMap((created) => add(created.id)))
          expect([first.name, third.name]).toEqual(["survey-1", "survey-3"])

          yield* db.delete(SessionTeamTable).where(eq(SessionTeamTable.session_id, secondSession.id)).run()

          const fourth = yield* child("d").pipe(Effect.flatMap((created) => add(created.id)))
          expect(fourth.name).toBe("survey-4")
          const roster = yield* team.roster(fourth)
          expect(roster.map((entry) => entry.name)).toEqual(["survey-1", "survey-3", "survey-4"])
        }),
      ),
    ),
  )

  it.live("numbers each team independently under one parent", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const child = (title: string) => sessions.create({ parentID: parent.id, title })
          const add = (sessionID: Session.ID, teamID: string) => team.register({ parentID: parent.id, teamID, sessionID })

          const alpha1 = yield* child("a").pipe(Effect.flatMap((created) => add(created.id, "alpha")))
          const alpha2 = yield* child("b").pipe(Effect.flatMap((created) => add(created.id, "alpha")))
          const beta1 = yield* child("c").pipe(Effect.flatMap((created) => add(created.id, "beta")))

          expect([alpha1.name, alpha2.name, beta1.name]).toEqual(["alpha-1", "alpha-2", "beta-1"])
          expect(alpha1.role).toBe("leader")
          expect(beta1.role).toBe("leader")
        }),
      ),
    ),
  )

  it.live("promotes the next registration when the leader row is gone", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const team = yield* SessionTeam.Service
          const { db } = yield* Database.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const child = (title: string) => sessions.create({ parentID: parent.id, title })
          const add = (sessionID: Session.ID) => team.register({ parentID: parent.id, teamID: "survey", sessionID })

          const leaderSession = yield* child("a")
          const leader = yield* add(leaderSession.id)
          const member = yield* child("b").pipe(Effect.flatMap((created) => add(created.id)))
          expect([leader.role, member.role]).toEqual(["leader", "member"])

          yield* db.delete(SessionTeamTable).where(eq(SessionTeamTable.session_id, leaderSession.id)).run()

          const promoted = yield* child("c").pipe(Effect.flatMap((created) => add(created.id)))
          expect(promoted.role).toBe("leader")
          expect(promoted.name).toBe("survey-3")
          expect(member.role).toBe("member")
        }),
      ),
    ),
  )
})
