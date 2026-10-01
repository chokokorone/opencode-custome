export * as LogTool from "./log.js"

import { ToolFailure } from "@opencode-ai/ai"
import type { Context } from "@opencode-ai/plugin/effect/plugin"
import { and, desc, eq, inArray, isNull, or, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Agent } from "../../agent.js"
import { Database } from "../../database/database.js"
import { Session } from "../../session.js"
import { SessionTeam } from "../../session/team.js"
import { LogKinds, LogTable, type LogKind } from "../../log/sql.js"

const SUMMARY_MAX = 100
const BODY_MAX = 8000
const TAGS_MAX = 10
const TAG_PATTERN = /^[a-z0-9_-]+$/
const REFS_MAX = 20
const REF_MAX = 200
const GET_MAX = 20
const GET_CHARS_MAX = 16000

export const formatRow = (row: {
  seq: number
  kind: string
  team: string | null
  time_created: number
  summary: string
}) => {
  const at = new Date(row.time_created)
  const pad = (value: number) => value.toString().padStart(2, "0")
  const stamp = `${pad(at.getUTCMonth() + 1)}-${pad(at.getUTCDate())} ${pad(at.getUTCHours())}:${pad(at.getUTCMinutes())}`
  return `L${row.seq} | ${row.kind} | ${row.team ?? "-"} | ${stamp} | ${row.summary}`
}

const parseSeq = (value: string | number): number | undefined => {
  if (typeof value === "number") return Number.isInteger(value) && value > 0 ? value : undefined
  const match = /^L(\d+)$/.exec(value.trim())
  return match ? Number(match[1]) : undefined
}

type LogRow = typeof LogTable.$inferSelect
type Db = Database.Interface["db"]

const eqSeqProject = (seq: number, projectID: string) =>
  and(eq(LogTable.seq, seq), eq(LogTable.project_id, projectID))

const recentWhere = (projectID: string, kind: string | undefined, team: string | undefined) =>
  and(
    eq(LogTable.project_id, projectID),
    isNull(LogTable.archived_at),
    kind === undefined ? undefined : eq(LogTable.kind, kind as LogKind),
    team === undefined ? undefined : eq(LogTable.team, team),
  )

const getWhere = (projectID: string, seqs: number[]) =>
  and(eq(LogTable.project_id, projectID), inArray(LogTable.seq, seqs))

const searchFts = (
  db: Db,
  projectID: string,
  query: string,
  kind: string | undefined,
  team: string | undefined,
  tag: string | undefined,
  limit: number,
) => {
  // One double-quoted phrase per term, AND-joined: user input can never act as
  // FTS syntax (quotes inside a term become two quotes, which FTS reads literally).
  const match = query
    .split(/\s+/)
    .filter((term) => term.length > 0)
    .map((term) => `"${term.replaceAll('"', '""')}"`)
    .join(" AND ")
  return db
    .select({ log: LogTable })
    .from(LogTable)
    .innerJoin(sql`log_fts`, sql`log.seq = log_fts.rowid`)
    .where(
      and(
        eq(LogTable.project_id, projectID),
        isNull(LogTable.archived_at),
        sql`log_fts MATCH ${match}`,
        kind === undefined ? undefined : eq(LogTable.kind, kind as LogKind),
        team === undefined ? undefined : eq(LogTable.team, team),
        tag === undefined ? undefined : sql`EXISTS (SELECT 1 FROM json_each(log.tags) WHERE value = ${tag})`,
      ),
    )
    .orderBy(sql`rank`, desc(LogTable.seq))
    .limit(limit)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => rows.map((row) => row.log)),
    )
}

const escapeLike = (term: string) => term.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_")

const searchLike = (
  db: Db,
  projectID: string,
  query: string,
  kind: string | undefined,
  team: string | undefined,
  tag: string | undefined,
  limit: number,
) => {
  const pattern = `%${escapeLike(query)}%`
  return db
    .select()
    .from(LogTable)
    .where(
      and(
        eq(LogTable.project_id, projectID),
        isNull(LogTable.archived_at),
        or(sql`summary LIKE ${pattern} ESCAPE '\\'`, sql`body LIKE ${pattern} ESCAPE '\\'`),
        kind === undefined ? undefined : eq(LogTable.kind, kind as LogKind),
        team === undefined ? undefined : eq(LogTable.team, team),
        tag === undefined ? undefined : sql`EXISTS (SELECT 1 FROM json_each(log.tags) WHERE value = ${tag})`,
      ),
    )
    .orderBy(desc(LogTable.seq))
    .limit(limit)
    .all()
    .pipe(Effect.orDie)
}

const formatEntry = (row: LogRow) => {
  const lines = [formatRow(row)]
  if (row.body) lines.push(`  body: ${row.body}`)
  if (row.tags && row.tags.length > 0) lines.push(`  tags: ${row.tags.join(", ")}`)
  if (row.refs && row.refs.length > 0) lines.push(`  refs: ${row.refs.join(", ")}`)
  if (row.re !== null && row.re !== undefined) lines.push(`  re: L${row.re}`)
  return lines.join("\n")
}

export const Plugin = {
  id: "opencode.tool.log",
  effect: Effect.fn("LogTool.Plugin")(function* (ctx: Context) {
    const sessions = yield* Session.Service
    const team = yield* SessionTeam.Service
    const agents = yield* Agent.Service
    const db = (yield* Database.Service).db

    // No permission.assert here by design: background members must never stall
    // on approval while appending to the shared log.

    const fail = (message: string) => Effect.fail(new ToolFailure({ message }))

    const resolveScope = (sessionID: Session.ID, agentID: Agent.ID) =>
      Effect.gen(function* () {
        const session = yield* sessions
          .get(sessionID)
          .pipe(Effect.mapError((error) => new ToolFailure({ message: `Session not found: ${sessionID}`, error })))
        const membership = yield* team.membership(sessionID)
        if (membership) return { projectID: session.projectID, agent: membership.name, team: membership.teamID }
        const teams = yield* team.teamsOf(sessionID)
        if (teams.length > 0) return { projectID: session.projectID, agent: "Boss", team: null as string | null }
        const agent = yield* agents.resolve(agentID)
        return { projectID: session.projectID, agent: agent?.name ?? String(agentID), team: null as string | null }
      })

    const checkRe = (projectID: string, re: string | number | undefined) =>
      Effect.gen(function* () {
        if (re === undefined) return undefined
        const seq = parseSeq(re)
        if (seq === undefined) return yield* fail(`Invalid re "${re}": use an entry id like L123.`)
        const target = yield* db
          .select({ seq: LogTable.seq })
          .from(LogTable)
          .where(eqSeqProject(seq, projectID))
          .get()
          .pipe(Effect.orDie)
        if (!target) return yield* fail(`Unknown re "L${seq}": no such entry in this project.`)
        return seq
      })

    yield* ctx.tool
      .transform((editor) => {
        editor.add({
          name: "log_add",
          options: { codemode: false },
          description:
            "Appends one row to the shared project log. Returns its id (L<seq>). All roles may write; there is no update or delete.",
          input: Schema.Struct({
            kind: Schema.String.annotate({
              description: "One of: note, finding, question, failure, decision, request, digest.",
            }),
            summary: Schema.String.annotate({ description: "Single line, at most 100 characters." }),
            body: Schema.optionalKey(Schema.String).annotate({ description: "Full text, at most 8000 characters." }),
            tags: Schema.optionalKey(Schema.Array(Schema.String)).annotate({
              description: "Lowercase alphanumeric plus _-, at most 10.",
            }),
            refs: Schema.optionalKey(Schema.Array(Schema.String)).annotate({
              description: "Free references, at most 20 of 200 characters each.",
            }),
            re: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])).annotate({
              description: "Entry this row corrects or resolves, as L<seq>. Must exist in this project.",
            }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if (!LogKinds.includes(input.kind as LogKind))
                return yield* fail(`Unknown kind "${input.kind}". Must be one of: ${LogKinds.join(", ")}.`)
              if (input.summary.trim().length === 0) return yield* fail("Summary must not be empty.")
              if (/[\r\n]/.test(input.summary)) return yield* fail("Summary must be a single line.")
              if ([...input.summary].length > SUMMARY_MAX)
                return yield* fail(`Summary is too long (${[...input.summary].length} characters, max ${SUMMARY_MAX}).`)
              if (input.body !== undefined && [...input.body].length > BODY_MAX)
                return yield* fail(`Body is too long (${[...input.body].length} characters, max ${BODY_MAX}).`)
              if (input.tags !== undefined) {
                if (input.tags.length > TAGS_MAX)
                  return yield* fail(`Too many tags (${input.tags.length}, max ${TAGS_MAX}).`)
                for (const tag of input.tags)
                  if (!TAG_PATTERN.test(tag))
                    return yield* fail(`Invalid tag "${tag}": lowercase alphanumeric plus _- only.`)
              }
              if (input.refs !== undefined) {
                if (input.refs.length > REFS_MAX)
                  return yield* fail(`Too many refs (${input.refs.length}, max ${REFS_MAX}).`)
                for (const ref of input.refs)
                  if ([...ref].length > REF_MAX)
                    return yield* fail(`Ref is too long (${[...ref].length} characters, max ${REF_MAX}).`)
              }
              const scope = yield* resolveScope(context.sessionID, context.agent)
              const re = yield* checkRe(scope.projectID, input.re)
              const values: typeof LogTable.$inferInsert = {
                project_id: scope.projectID,
                session_id: context.sessionID,
                team: scope.team,
                agent: scope.agent,
                kind: input.kind as LogKind,
                summary: input.summary,
                body: input.body,
                tags: input.tags ? [...input.tags] : undefined,
                refs: input.refs ? [...input.refs] : undefined,
                re,
              }
              const inserted = yield* db
                .insert(LogTable)
                .values(values)
                .returning({ seq: LogTable.seq })
                .get()
                .pipe(Effect.orDie)
              const id = `L${inserted.seq}`
              return { output: { output: id }, content: id, metadata: { id } }
            }),
        })
        editor.add({
          name: "log_recent",
          options: { codemode: false },
          description: "Lists recent log rows, newest first. Archived rows are hidden.",
          input: Schema.Struct({
            n: Schema.optionalKey(Schema.Number).annotate({ description: "How many rows. Default 20, max 50." }),
            kind: Schema.optionalKey(Schema.String),
            team: Schema.optionalKey(Schema.String),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              const scope = yield* resolveScope(context.sessionID, context.agent)
              const limit = Math.min(Math.max(Math.floor(input.n ?? 20), 1), 50)
              const rows = yield* db
                .select()
                .from(LogTable)
                .where(recentWhere(scope.projectID, input.kind, input.team))
                .orderBy(desc(LogTable.seq))
                .limit(limit)
                .all()
                .pipe(Effect.orDie)
              const text = rows.map((row) => formatRow(row)).join("\n")
              return { output: { output: text }, content: text, metadata: { count: rows.length } }
            }),
        })
        editor.add({
          name: "log_stats",
          options: { codemode: false },
          description: "Counts log rows by kind and team for cost/usage awareness. Archived rows are hidden.",
          input: Schema.Struct({
            kind: Schema.optionalKey(Schema.String),
            team: Schema.optionalKey(Schema.String),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              const scope = yield* resolveScope(context.sessionID, context.agent)
              const rows = yield* db
                .select()
                .from(LogTable)
                .where(recentWhere(scope.projectID, input.kind, input.team))
                .orderBy(desc(LogTable.seq))
                .all()
                .pipe(Effect.orDie)
              const byKind = new Map<string, number>()
              const byTeam = new Map<string, number>()
              for (const row of rows) {
                byKind.set(row.kind, (byKind.get(row.kind) ?? 0) + 1)
                const teamKey = row.team ?? "-"
                byTeam.set(teamKey, (byTeam.get(teamKey) ?? 0) + 1)
              }
              const rank = (entries: ReadonlyArray<readonly [string, number]>) =>
                [...entries].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
              const kindText = rank([...byKind.entries()])
                .map(([kind, count]) => `${kind} ${count}`)
                .join(", ")
              const teamText = rank([...byTeam.entries()])
                .map(([teamName, count]) => `${teamName} ${count}`)
                .join(", ")
              const latest = rows[0]
              const lines = [
                `total: ${rows.length} (archived hidden)`,
                `by kind: ${kindText === "" ? "-" : kindText}`,
                `by team: ${teamText === "" ? "-" : teamText}`,
                latest ? `latest: ${formatRow(latest)}` : "latest: none",
              ]
              const text = lines.join("\n")
              return { output: { output: text }, content: text, metadata: { count: rows.length } }
            }),
        })
        editor.add({
          name: "log_search",
          options: { codemode: false },
          description:
            "Searches the log. Three or more characters use trigram FTS; one or two use LIKE. Archived rows are hidden.",
          input: Schema.Struct({
            query: Schema.String.annotate({
              description: "Search text. Quoted phrases are safe: syntax is escaped.",
            }),
            kind: Schema.optionalKey(Schema.String),
            team: Schema.optionalKey(Schema.String),
            tag: Schema.optionalKey(Schema.String),
            limit: Schema.optionalKey(Schema.Number).annotate({ description: "Default 10, max 30." }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if ([...input.query].length === 0) return yield* fail("Query must not be empty.")
              const scope = yield* resolveScope(context.sessionID, context.agent)
              const limit = Math.min(Math.max(Math.floor(input.limit ?? 10), 1), 30)
              const rows =
                [...input.query].length >= 3
                  ? yield* searchFts(db, scope.projectID, input.query, input.kind, input.team, input.tag, limit)
                  : yield* searchLike(db, scope.projectID, input.query, input.kind, input.team, input.tag, limit)
              const text = rows.map((row) => formatRow(row)).join("\n")
              return { output: { output: text }, content: text, metadata: { count: rows.length } }
            }),
        })
        editor.add({
          name: "log_get",
          options: { codemode: false },
          description: "Fetches full entries by id (L<seq>), at most 20. Long output is truncated with a notice.",
          input: Schema.Struct({
            ids: Schema.Array(Schema.String).annotate({ description: "Entry ids like L123." }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if (input.ids.length === 0) return yield* fail("Provide at least one id.")
              if (input.ids.length > GET_MAX)
                return yield* fail(`Too many ids (${input.ids.length}, max ${GET_MAX}).`)
              const scope = yield* resolveScope(context.sessionID, context.agent)
              const seqs: number[] = []
              const bad: string[] = []
              for (const id of input.ids) {
                const seq = parseSeq(id)
                if (seq === undefined) bad.push(id)
                else seqs.push(seq)
              }
              if (bad.length > 0) return yield* fail(`Invalid ids: ${bad.join(", ")}. Use L<seq> form.`)
              const rows = yield* db
                .select()
                .from(LogTable)
                .where(getWhere(scope.projectID, seqs))
                .all()
                .pipe(Effect.orDie)
              const found = new Set(rows.map((row) => row.seq))
              const missing = seqs.filter((seq) => !found.has(seq))
              if (missing.length > 0)
                return yield* fail(`Entries not found in this project: ${missing.map((seq) => `L${seq}`).join(", ")}.`)
              const bySeq = new Map(rows.map((row) => [row.seq, row] as const))
              const parts = seqs.map((seq) => formatEntry(bySeq.get(seq)!))
              let text = ""
              const kept: string[] = []
              for (const part of parts) {
                if (text.length + part.length > GET_CHARS_MAX) break
                kept.push(part)
                text += (text.length > 0 ? "\n" : "") + part
              }
              if (kept.length < parts.length)
                text += `\n(truncated: output exceeded ${GET_CHARS_MAX} characters, ${parts.length - kept.length} entr${parts.length - kept.length === 1 ? "y" : "ies"} omitted)`
              return { output: { output: text }, content: text, metadata: { count: kept.length } }
            }),
        })
      })
      .pipe(Effect.orDie)
  }),
}
