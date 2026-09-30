import { Effect } from "effect"
// HARNESS: log FTS — hand-appended call, see docs/design-log.md
import { LogFts } from "../../log/fts.js"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20260930132925_fat_vargas",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`log\` (
          \`seq\` integer PRIMARY KEY AUTOINCREMENT,
          \`project_id\` text NOT NULL,
          \`session_id\` text,
          \`team\` text,
          \`agent\` text,
          \`kind\` text NOT NULL,
          \`summary\` text NOT NULL,
          \`body\` text,
          \`tags\` text,
          \`refs\` text,
          \`re\` integer,
          \`archived_at\` integer,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT "log_kind_check" CHECK(kind IN ('note', 'finding', 'question', 'failure', 'decision', 'request', 'digest')),
          CONSTRAINT "log_summary_len_check" CHECK(length(summary) <= 100)
        );
      `)
      yield* tx.run(`CREATE INDEX \`log_project_idx\` ON \`log\` (\`project_id\`);`)
      yield* tx.run(`CREATE INDEX \`log_project_kind_idx\` ON \`log\` (\`project_id\`,\`kind\`);`)
      // HARNESS: log FTS — FTS objects live in LogFts.ensureLogFts, shared with the
      // fresh-database bootstrap (which never runs this body). Kept as a call, not
      // SQL, so the definition has a single source.
      yield* LogFts.ensureLogFts(tx)
    })
  },
}

export default migration
