import { describe, expect, test } from "bun:test"
import path from "path"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { SqlClient } from "effect/unstable/sql"
import { sql } from "drizzle-orm"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import { EffectDrizzleSqlite } from "@opencode-ai/core/database/drizzle"
import { Global } from "@opencode-ai/util/global"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient | Global.Service>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provideService(
        Global.Service,
        Global.make({ data: path.join(process.cwd(), ".test-data") }),
      ),
      Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      Effect.scoped,
    ),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()

const objects = (db: Effect.Success<typeof makeDb>, like: string) =>
  db.all<{ name: string; type: string }>(
    sql`SELECT name, type FROM sqlite_master WHERE name LIKE ${like} ORDER BY name`,
  )

const validRow = {
  project_id: "global",
  session_id: "ses_log_test",
  team: "survey",
  agent: "member-1",
  kind: "note",
  summary: "日本語の要約",
  body: "本文です",
}

describe("log schema (stage 1)", () => {
  test("fresh and upgraded databases both expose log, log_fts and the three triggers", async () => {
    await run(
      Effect.gen(function* () {
        // Fresh bootstrap path: schema.up + seeded journal, no per-migration up() runs.
        const fresh = yield* makeDb
        yield* DatabaseMigration.apply(fresh)
        const freshObjects = (yield* objects(fresh, "log%")).map((row) => row.name)
        expect(freshObjects).toContain("log")
        expect(freshObjects).toContain("log_fts")
        // FTS5 shadow tables (data/idx/config/docsize) share the prefix; only the base names matter.
        expect(
          (yield* fresh.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN ('log', 'log_fts') ORDER BY name`)).map(
            (row) => row.name,
          ),
        ).toEqual(["log_fts_delete", "log_fts_insert", "log_no_content_update"])

        // Upgrade path: everything but the log migration, then the full registry.
        const upgraded = yield* makeDb
        yield* DatabaseMigration.applyOnly(upgraded, migrations.slice(0, -1))
        yield* DatabaseMigration.applyOnly(upgraded, migrations)
        const upgradedObjects = (yield* objects(upgraded, "log%")).map((row) => row.name)
        expect(upgradedObjects).toContain("log")
        expect(upgradedObjects).toContain("log_fts")
        const fts = yield* upgraded.all<{ rowid: number }>(sql`SELECT rowid FROM log_fts LIMIT 1`)
        expect(fts).toEqual([])
      }),
    )
  })

  test("inserted rows are searchable through FTS, including Japanese", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* db.run(sql`INSERT INTO log (project_id, session_id, team, agent, kind, summary, body, time_created, time_updated) VALUES (${validRow.project_id}, ${validRow.session_id}, ${validRow.team}, ${validRow.agent}, ${validRow.kind}, ${validRow.summary}, ${validRow.body}, 1, 1)`)
        const hits = yield* db.all<{ seq: number }>(
          sql`SELECT seq FROM log WHERE seq IN (SELECT rowid FROM log_fts WHERE log_fts MATCH ${"日本語"})`,
        )
        expect(hits).toHaveLength(1)
      }),
    )
  })

  test("content UPDATE is rejected while archived_at UPDATE is allowed", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* db.run(sql`INSERT INTO log (project_id, kind, summary, time_created, time_updated) VALUES ('global', 'note', 's', 1, 1)`)
        const row = yield* db.get<{ seq: number }>(sql`SELECT seq FROM log LIMIT 1`)
        expect(row).toBeDefined()
        const seq = row!.seq
        const contentUpdate = db.run(sql`UPDATE log SET summary = 'changed' WHERE seq = ${seq}`).pipe(Effect.exit)
        expect((yield* contentUpdate)._tag).toBe("Failure")
        const archiveUpdate = db.run(sql`UPDATE log SET archived_at = 1 WHERE seq = ${seq}`).pipe(Effect.exit)
        expect((archiveUpdate && (yield* archiveUpdate)._tag)).toBe("Success")
        expect((yield* db.get<{ summary: string }>(sql`SELECT summary FROM log WHERE seq = ${seq}`))?.summary).toBe(
          "s",
        )
      }),
    )
  })

  test("deleted rows disappear from FTS results", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* db.run(sql`INSERT INTO log (project_id, kind, summary, time_created, time_updated) VALUES ('global', 'finding', ${"削除確認用の要約"}, 1, 1)`)
        expect(
          (yield* db.all(sql`SELECT rowid FROM log_fts WHERE log_fts MATCH ${"削除確認"}`)).length,
        ).toBe(1)
        yield* db.run(sql`DELETE FROM log WHERE summary = ${"削除確認用の要約"}`)
        expect(
          (yield* db.all(sql`SELECT rowid FROM log_fts WHERE log_fts MATCH ${"削除確認"}`)).length,
        ).toBe(0)
      }),
    )
  })

  test("CHECK rejects overlong summaries and unknown kinds", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        const longSummary = "x".repeat(101)
        const longInsert = db
          .run(sql`INSERT INTO log (project_id, kind, summary, time_created, time_updated) VALUES ('global', 'note', ${longSummary}, 1, 1)`)
          .pipe(Effect.exit)
        expect((yield* longInsert)._tag).toBe("Failure")
        const badKind = db
          .run(sql`INSERT INTO log (project_id, kind, summary, time_created, time_updated) VALUES ('global', 'bogus', 's', 1, 1)`)
          .pipe(Effect.exit)
        expect((yield* badKind)._tag).toBe("Failure")
        // Boundary: exactly 100 characters is accepted.
        yield* db.run(sql`INSERT INTO log (project_id, kind, summary, time_created, time_updated) VALUES ('global', 'decision', ${"y".repeat(100)}, 1, 1)`)
        expect((yield* db.all(sql`SELECT seq FROM log`)).length).toBe(1)
      }),
    )
  })
})

describe("log schema consistency", () => {
  test("fresh bootstrap and incremental migration produce identical sqlite_master", async () => {
    await run(
      Effect.gen(function* () {
        const normalize = (value: string | null) => (value ?? "").replace(/\s+/g, " ").trim()
        const dump = (db: Effect.Success<typeof makeDb>) =>
          db
            .all<{ type: string; name: string; tbl_name: string; sql: string | null }>(
              sql`SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`,
            )
            .pipe(
              Effect.map((rows) =>
                rows.map((row) => `${row.type}|${row.name}|${row.tbl_name}|${normalize(row.sql)}`),
              ),
            )

        const fresh = yield* makeDb
        yield* DatabaseMigration.apply(fresh)

        const upgraded = yield* makeDb
        yield* DatabaseMigration.applyOnly(upgraded, migrations.slice(0, -1))
        yield* DatabaseMigration.applyOnly(upgraded, migrations)

        const [freshDump, upgradedDump] = yield* Effect.all([dump(fresh), dump(upgraded)])
        expect(upgradedDump).toEqual(freshDump)
        // The log objects are part of the compared surface, not just fellow travelers.
        expect(freshDump.some((line) => line.includes("|log|"))).toBe(true)
        expect(freshDump.some((line) => line.includes("log_fts"))).toBe(true)
      }),
    )
  })
})
