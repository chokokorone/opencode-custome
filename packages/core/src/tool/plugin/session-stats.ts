export * as SessionStatsTool from "./session-stats.js"

import { ToolFailure } from "@opencode-ai/ai"
import type { Context } from "@opencode-ai/plugin/effect/plugin"
import { Event } from "@opencode-ai/schema/event"
import { and, eq } from "drizzle-orm"
import { Effect, Option, Schema } from "effect"
import { Database } from "../../database/database.js"
import { EventTable } from "../../event/sql.js"
import { SessionEvent } from "../../session/event.js"

export interface Summary {
  readonly sessionID: string
  readonly turns: number
  readonly llmRequests: number
  readonly input: number
  readonly output: number
  readonly cached: number
  readonly cost: number
  readonly toolCalls: number
  readonly peakParallel: number
}

const decodeEnded = Schema.decodeUnknownOption(SessionEvent.Step.Ended.data)
const endedType = Event.versionedType(SessionEvent.Step.Ended.type, SessionEvent.Step.Ended.durable.version)

export const aggregateWithDb = Effect.fn("SessionStatsTool.aggregateWithDb")(function* (
  db: Database.Interface["db"],
  sessionID: string,
) {
  const rows = yield* db
    .select({ data: EventTable.data })
    .from(EventTable)
    .where(and(eq(EventTable.aggregate_id, sessionID), eq(EventTable.type, endedType)))
    .all()
    .pipe(Effect.orDie)
  let turns = 0
  let input = 0
  let output = 0
  let cached = 0
  let cost = 0
  let toolCalls = 0
  let peakParallel = 0
  for (const row of rows) {
    const decoded = decodeEnded(row.data)
    if (Option.isNone(decoded)) continue
    const data = decoded.value
    turns += 1
    input += data.tokens.input
    output += data.tokens.output
    cached += data.tokens.cache.read + data.tokens.cache.write
    cost += data.cost
    toolCalls += data.metrics?.toolCalls ?? 0
    const peak = data.metrics?.maxParallelTools ?? 0
    if (peak > peakParallel) peakParallel = peak
  }
  const summary: Summary = {
    sessionID,
    turns,
    llmRequests: turns,
    input,
    output,
    cached,
    cost,
    toolCalls,
    peakParallel,
  }
  return summary
})

export const aggregate = Effect.fn("SessionStatsTool.aggregate")(function* (sessionID: string) {
  const db = (yield* Database.Service).db
  return yield* aggregateWithDb(db, sessionID)
})

export const format = (summary: Summary): string => {
  const line1 = `session: ${summary.sessionID} | turns: ${summary.turns} | llm_requests: ${summary.llmRequests}`
  const line2 = `tokens: in=${summary.input} out=${summary.output} cached=${summary.cached} | cost=${summary.cost}`
  const line3 = `tools: ${summary.toolCalls} calls, peak parallel ${summary.peakParallel}`
  return `${line1}\n${line2}\n${line3}`
}

export const Plugin = {
  id: "opencode.tool.session-stats",
  effect: Effect.fn("SessionStatsTool.Plugin")(function* (ctx: Context) {
    // Observability only: no permission gate, same as log tools.
    const db = (yield* Database.Service).db
    const fail = (message: string) => Effect.fail(new ToolFailure({ message }))
    yield* ctx.tool
      .transform((editor) => {
        editor.add({
          name: "session_stats",
          options: { codemode: false },
          description:
            "Aggregates durable step metrics for a session: turns, tokens, cost and tool parallelism. Compact text, no JSON.",
          input: Schema.Struct({
            sessionID: Schema.optionalKey(Schema.String).annotate({
              description: "Session id. Omit for the current session.",
            }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              const target = input.sessionID ?? context.sessionID
              if (target.trim().length === 0) return yield* fail("Session id must not be empty.")
              const summary = yield* aggregateWithDb(db, target)
              const text = format(summary)
              return { output: { output: text }, content: text, metadata: { turns: summary.turns } }
            }),
        })
      })
      .pipe(Effect.orDie)
  }),
}
