import { check, index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { sql } from "drizzle-orm"
import { Timestamps } from "../database/schema.sql.js"

export const LogKinds = ["note", "finding", "question", "failure", "decision", "request", "digest"] as const
export type LogKind = (typeof LogKinds)[number]

export const LogTable = sqliteTable(
  "log",
  {
    seq: integer().primaryKey({ autoIncrement: true }),
    project_id: text().notNull(),
    session_id: text(),
    team: text(),
    agent: text(),
    kind: text().$type<LogKind>().notNull(),
    summary: text().notNull(),
    body: text(),
    tags: text({ mode: "json" }).$type<string[]>(),
    refs: text({ mode: "json" }).$type<number[]>(),
    re: integer(),
    archived_at: integer(),
    ...Timestamps,
  },
  (table) => [
    index("log_project_idx").on(table.project_id),
    index("log_project_kind_idx").on(table.project_id, table.kind),
    check("log_kind_check", sql`kind IN ('note', 'finding', 'question', 'failure', 'decision', 'request', 'digest')`),
    check("log_summary_len_check", sql`length(summary) <= 100`),
  ],
)
