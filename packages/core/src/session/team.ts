export * as SessionTeam from "./team.js"

import { and, asc, eq } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import { makeGlobalNode } from "@opencode-ai/util/effect/app-node"
import { Database } from "../database/database.js"
import type { ConfigTeam } from "@opencode-ai/schema/config/team"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { SessionSchema } from "./schema.js"
import { SessionTeamTable } from "./sql.js"

export type Role = "leader" | "member"

export interface Entry {
  readonly sessionID: SessionSchema.ID
  readonly name: string
  readonly role: Role
}

export interface Team {
  readonly teamID: string
  readonly entries: readonly Entry[]
}

export interface Membership {
  readonly parentID: SessionSchema.ID
  readonly teamID: string
  readonly sessionID: SessionSchema.ID
  readonly name: string
  readonly role: Role
}

export interface RegisterInput {
  readonly parentID: SessionSchema.ID
  readonly teamID: string
  readonly sessionID: SessionSchema.ID
}

export interface Interface {
  /** Registers a child session in its parent's team, assigning name and role. */
  readonly register: (input: RegisterInput) => Effect.Effect<Membership>
  readonly membership: (sessionID: SessionSchema.ID) => Effect.Effect<Membership | undefined>
  /** Ordered roster entries for the member's team, including the caller. */
  readonly roster: (membership: Membership) => Effect.Effect<readonly Entry[]>
  /** All teams spawned by the session, grouped by team id, ordered by position. */
  readonly teamsOf: (parentID: SessionSchema.ID) => Effect.Effect<readonly Team[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionTeam") {}

const fromRow = (row: typeof SessionTeamTable.$inferSelect): Membership => ({
  parentID: row.parent_id,
  teamID: row.team_id,
  sessionID: row.session_id,
  name: row.name,
  role: row.role,
})

const entry = (row: typeof SessionTeamTable.$inferSelect): Entry => ({
  sessionID: row.session_id,
  name: row.name,
  role: row.role,
})

const teamKey = (input: RegisterInput) => `${input.parentID}/${input.teamID}`

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    const locks = KeyedMutex.makeUnsafe<string>()

    const register: Interface["register"] = (input) =>
      locks.withLock(teamKey(input))(
        Effect.gen(function* () {
          const existing = yield* db
            .select()
            .from(SessionTeamTable)
            .where(eq(SessionTeamTable.session_id, input.sessionID))
            .get()
            .pipe(Effect.orDie)
          if (existing) return fromRow(existing)
          const rows = yield* db
            .select()
            .from(SessionTeamTable)
            .where(eq(SessionTeamTable.parent_id, input.parentID))
            .pipe(Effect.orDie)
          const siblings = rows.filter((row) => row.team_id === input.teamID)
          const suffixes = siblings.flatMap((row) => {
            const rest = row.name.startsWith(`${input.teamID}-`) ? row.name.slice(input.teamID.length + 1) : undefined
            return rest !== undefined && /^\d+$/.test(rest) ? [Number(rest)] : []
          })
          const next = suffixes.length > 0 ? Math.max(...suffixes) + 1 : 1
          const positions = siblings.map((row) => row.position)
          const position = positions.length > 0 ? Math.max(...positions) + 1 : 0
          const leaderExists = siblings.some((row) => row.role === "leader")
          return yield* db
            .insert(SessionTeamTable)
            .values({
              id: crypto.randomUUID(),
              parent_id: input.parentID,
              team_id: input.teamID,
              session_id: input.sessionID,
              name: `${input.teamID}-${next}`,
              role: leaderExists ? "member" : "leader",
              position,
              time_created: Date.now(),
            })
            .returning()
            .get()
            .pipe(Effect.orDie, Effect.map(fromRow))
        }),
      )

    const membership: Interface["membership"] = Effect.fn("SessionTeam.membership")(function* (sessionID) {
      const row = yield* db
        .select()
        .from(SessionTeamTable)
        .where(eq(SessionTeamTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      return row ? fromRow(row) : undefined
    })

    const roster: Interface["roster"] = Effect.fn("SessionTeam.roster")(function* (membership) {
      const rows = yield* db
        .select()
        .from(SessionTeamTable)
        .where(
          and(eq(SessionTeamTable.parent_id, membership.parentID), eq(SessionTeamTable.team_id, membership.teamID)),
        )
        .orderBy(asc(SessionTeamTable.position))
        .all()
        .pipe(Effect.orDie)
      return rows.map(entry)
    })

    const teamsOf: Interface["teamsOf"] = Effect.fn("SessionTeam.teamsOf")(function* (parentID) {
      const rows = yield* db
        .select()
        .from(SessionTeamTable)
        .where(eq(SessionTeamTable.parent_id, parentID))
        .orderBy(asc(SessionTeamTable.team_id), asc(SessionTeamTable.position))
        .all()
        .pipe(Effect.orDie)
      const teams = new Map<string, Entry[]>()
      for (const row of rows) {
        const group = teams.get(row.team_id)
        const item = entry(row)
        if (group) group.push(item)
        else teams.set(row.team_id, [item])
      }
      return [...teams].map(([teamID, entries]) => ({ teamID, entries }))
    })

    return Service.of({ register, membership, roster, teamsOf })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Database.node] })

export interface Structure {
  readonly teamID: string
  readonly leader: string | undefined
  readonly members: readonly string[] | undefined
  readonly reportsTo: "boss" | undefined
  readonly rules: readonly ConfigTeam.Rule[]
}

export type ResolveSuccess = {
  readonly ok: true
  readonly structure: Structure | undefined
}

export type ResolveFailure = {
  readonly ok: false
  readonly error: string
}

export type ResolveResult = ResolveSuccess | ResolveFailure

const findEntry = (config: readonly ConfigTeam.Info[], teamID: string): ConfigTeam.Info | undefined =>
  config.find((entry) => entry.teamID === teamID)

const leaderError = (entry: ConfigTeam.Info): string | undefined => {
  if (entry.leader === undefined) return undefined
  if (entry.members === undefined) return undefined
  if (entry.members.includes(entry.leader)) return undefined
  return `leader "${entry.leader}" must appear in members for team "${entry.teamID}"`
}

const toStructure = (entry: ConfigTeam.Info): Structure => ({
  teamID: entry.teamID,
  leader: entry.leader,
  members: entry.members,
  reportsTo: entry.reports_to,
  rules: entry.rules,
})

export function resolveStructure(config: readonly ConfigTeam.Info[], teamID: string): ResolveResult {
  const entry = findEntry(config, teamID)
  if (entry === undefined) return { ok: true, structure: undefined }
  const error = leaderError(entry)
  if (error !== undefined) return { ok: false, error }
  return { ok: true, structure: toStructure(entry) }
}
