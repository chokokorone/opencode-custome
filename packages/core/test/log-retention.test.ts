import { describe, expect, test } from "bun:test"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { LogRetention } from "@opencode-ai/core/log/retention"
import { LogTable } from "@opencode-ai/core/log/sql"
import { EffectDrizzleSqlite } from "@opencode-ai/core/database/drizzle"
import { Global } from "@opencode-ai/util/global"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { SqlClient } from "effect/unstable/sql"
import { Reactivity } from "effect/unstable/reactivity"
import { eq } from "drizzle-orm"
import path from "path"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient | Global.Service>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provideService(Global.Service, Global.make({ data: path.join(process.cwd(), ".test-data") })),
      Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      Effect.scoped,
    ),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const DAY = 24 * 60 * 60 * 1000

const insert = (
  db: Effect.Success<typeof makeDb>,
  row: { summary: string; time_created: number; re?: number },
) =>
  db
    .insert(LogTable)
    .values({ project_id: "global", kind: "note", summary: row.summary, time_created: row.time_created, time_updated: row.time_created, re: row.re })
    .returning({ seq: LogTable.seq })
    .get()

const archived = (db: Effect.Success<typeof makeDb>, seq: number) =>
  db
    .select({ archived_at: LogTable.archived_at })
    .from(LogTable)
    .where(eq(LogTable.seq, seq))
    .get()
    .pipe(Effect.map((row) => row?.archived_at ?? null))

describe("LogRetention", () => {
  test("archives old unreferenced rows and keeps referenced and fresh ones", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        // now = 100 days after epoch; retention window is 30 days.
        const now = 100 * DAY
        const old = yield* insert(db, { summary: "old and forgotten", time_created: 10 * DAY })
        const target = yield* insert(db, { summary: "old but referenced", time_created: 10 * DAY })
        const correction = yield* insert(db, { summary: "correction", time_created: 90 * DAY, re: target.seq })
        const fresh = yield* insert(db, { summary: "fresh work", time_created: 90 * DAY })

        yield* LogRetention.archiveStaleLogs(db, { now })

        expect(yield* archived(db, old.seq)).toBe(now)
        expect(yield* archived(db, target.seq)).toBeNull()
        // Fresh and referenced rows stay visible to recent/search.
        const visible = yield* db.all<{ seq: number }>(
          sql`SELECT seq FROM log WHERE archived_at IS NULL ORDER BY seq`,
        )
        expect(visible.map((row) => row.seq)).toEqual([target.seq, correction.seq, fresh.seq])
      }),
    )
  })

  test("honors an explicit window and leaves FTS rows untouched", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        const now = 100 * DAY
        const row = yield* insert(db, { summary: "boundary check", time_created: 10 * DAY })
        // A 200-day window keeps everything.
        yield* LogRetention.archiveStaleLogs(db, { now, olderThanDays: 200 })
        expect(yield* archived(db, row.seq)).toBeNull()
        // Archiving hides the row from search but the FTS entry stays consistent.
        yield* LogRetention.archiveStaleLogs(db, { now })
        expect(yield* archived(db, row.seq)).toBe(now)
        expect(
          (yield* db.all(sql`SELECT rowid FROM log_fts WHERE log_fts MATCH 'boundary'`)).length,
        ).toBe(1)
        expect(
          (
            yield* db.all(
              sql`SELECT seq FROM log WHERE seq IN (SELECT rowid FROM log_fts WHERE log_fts MATCH 'boundary') AND archived_at IS NULL`,
            )
          ).length,
        ).toBe(0)
      }),
    )
  })
})
