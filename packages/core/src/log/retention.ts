export * as LogRetention from "./retention.js"

import { and, isNull, lt, notInArray } from "drizzle-orm"
import { Duration, Effect, Layer, Schedule } from "effect"
import { makeGlobalNode } from "@opencode-ai/util/effect/app-node"
import { Database } from "../database/database.js"
import { LogTable } from "./sql.js"

/** Rows older than this with no `re` pointing at them are archived. */
export const RETENTION_DAYS = 30

/**
 * Archives stale, unreferenced rows. Referenced means targeted by another
 * row's `re` (correction chains stay readable); free-text `refs` are not
 * parsed. Archived rows leave FTS and storage untouched — recent and search
 * already filter them out — so no index synchronization is needed.
 */
export const archiveStaleLogs = (
  db: Database.Interface["db"],
  input: { readonly now: number; readonly olderThanDays?: number },
) =>
  Effect.gen(function* () {
    const cutoff = input.now - (input.olderThanDays ?? RETENTION_DAYS) * 24 * 60 * 60 * 1000
    const referenced = yield* db
      .select({ target: LogTable.re })
      .from(LogTable)
      .pipe(Effect.orDie)
    const targets = referenced.flatMap((row) => (row.target === null ? [] : [row.target]))
    const stale = and(
      isNull(LogTable.archived_at),
      lt(LogTable.time_created, cutoff),
      ...(targets.length > 0 ? [notInArray(LogTable.seq, targets)] : []),
    )
    yield* db.update(LogTable).set({ archived_at: input.now }).where(stale).run().pipe(Effect.orDie)
  })

const cleanupLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const database = yield* Database.Service
    const run = archiveStaleLogs(database.db, { now: Date.now() }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("log retention sweep failed", { cause })),
    )
    yield* run.pipe(Effect.repeat(Schedule.spaced(Duration.days(1))), Effect.forkScoped)
  }),
)

export const cleanupNode = makeGlobalNode({
  name: "log-retention-cleanup",
  layer: cleanupLayer,
  deps: [Database.node],
})
