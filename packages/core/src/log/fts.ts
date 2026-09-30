export * as LogFts from "./fts.js"

import { Effect } from "effect"
import type { DatabaseMigration } from "../database/migration.js"

type Transaction = Parameters<DatabaseMigration.Migration["up"]>[0]

/**
 * Full-text index over `log`, shared by both bootstrap paths.
 *
 * `DatabaseMigration.apply` seeds the journal on a fresh database without
 * running per-migration `up()` bodies, so anything that only lives in a
 * migration file never reaches a new database. This module is the single
 * source of the FTS objects: the bootstrap calls it after `schema.up(tx)`,
 * and the log migration calls it after creating the table.
 *
 * Idempotent (`IF NOT EXISTS`): safe to call on either path and to re-run.
 * Never rewrite this function for a schema change — add `ensureLogFtsV2`
 * and call it only from the new migration (old migrations keep calling V1).
 *
 * Design (see docs/investigation-log.md):
 * - external content (`content='log'`) so trigram postings don't duplicate `body`
 * - no UPDATE sync trigger; content columns reject UPDATE outright (append-only),
 *   which also keeps the index consistent without application code
 * - the delete trigger uses the FTS5 'delete' command form for external content
 */
export const ensureLogFts = (tx: Transaction) =>
  Effect.gen(function* () {
    yield* tx.run(`
      CREATE VIRTUAL TABLE IF NOT EXISTS \`log_fts\` USING fts5(
        summary,
        body,
        content='log',
        content_rowid='seq',
        tokenize=trigram
      );
    `)
    yield* tx.run(`
      CREATE TRIGGER IF NOT EXISTS \`log_fts_insert\` AFTER INSERT ON \`log\` BEGIN
        INSERT INTO \`log_fts\`(rowid, summary, body) VALUES (new.seq, new.summary, new.body);
      END;
    `)
    yield* tx.run(`
      CREATE TRIGGER IF NOT EXISTS \`log_no_content_update\` BEFORE UPDATE OF
        project_id, session_id, team, agent, kind, summary, body, tags, refs, re
      ON \`log\` BEGIN
        SELECT RAISE(ABORT, 'log is append-only');
      END;
    `)
    yield* tx.run(`
      CREATE TRIGGER IF NOT EXISTS \`log_fts_delete\` AFTER DELETE ON \`log\` BEGIN
        INSERT INTO \`log_fts\`(log_fts, rowid, summary, body)
        VALUES ('delete', old.seq, old.summary, old.body);
      END;
    `)
  })
