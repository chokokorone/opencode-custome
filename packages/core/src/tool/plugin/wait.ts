export * as ToolWaitTool from "./wait.js"

import { ToolFailure } from "@opencode-ai/ai"
import type { Context } from "@opencode-ai/plugin/effect/plugin"
import { Effect, Schema } from "effect"
import { Job } from "../../job.js"

const IDS_MAX = 20
const OUTPUT_MAX = 2000

const summarize = (output: string | undefined) => {
  if (output === undefined || output.length === 0) return "-"
  return output.length > OUTPUT_MAX ? `${output.slice(0, OUTPUT_MAX)}\n(truncated)` : output
}

export const Plugin = {
  id: "opencode.tool.wait",
  effect: Effect.fn("ToolWaitTool.Plugin")(function* (ctx: Context) {
    const jobs = yield* Job.Service

    // Observability only: waiting never mutates anything, so no permission gate.
    const fail = (message: string) => Effect.fail(new ToolFailure({ message }))

    const waitOne = (sessionID: string, id: string, timeout: number | undefined) =>
      Effect.gen(function* () {
        const info = yield* jobs.get(id)
        if (!info) return `${id} | unknown | no such job`
        const owner = info.metadata?.sessionID
        if (typeof owner === "string" && owner !== sessionID)
          return `${id} | forbidden | job belongs to another session`
        const waited = yield* jobs.wait({ id, timeout })
        if (waited.timedOut || !waited.info) return `${id} | timeout | still ${info.status}`
        const done = waited.info
        if (done.status === "completed") return `${id} | completed | ${summarize(done.output)}`
        return `${id} | ${done.status} | ${summarize(done.error)}`
      })

    yield* ctx.tool
      .transform((editor) => {
        editor.add({
          name: "tool_wait",
          options: { codemode: false },
          description:
            "Waits for background jobs started by this session and returns their results. Do not poll: call once and wait; completions are also delivered to the session inbox.",
          input: Schema.Struct({
            ids: Schema.Array(Schema.String).annotate({ description: "Background job ids, at most 20." }),
            timeout: Schema.optionalKey(Schema.Number).annotate({
              description: "Milliseconds to wait per job. Omit to wait until done.",
            }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if (input.ids.length === 0) return yield* fail("Provide at least one job id.")
              if (input.ids.length > IDS_MAX)
                return yield* fail(`Too many ids (${input.ids.length}, max ${IDS_MAX}).`)
              if (input.timeout !== undefined && !(input.timeout > 0))
                return yield* fail("Timeout must be a positive number of milliseconds.")
              const lines = yield* Effect.forEach(input.ids, (id) => waitOne(context.sessionID, id, input.timeout), {
                concurrency: "unbounded",
              })
              const text = lines.join("\n")
              return { output: { output: text }, content: text, metadata: { count: lines.length } }
            }),
        })
      })
      .pipe(Effect.orDie)
  }),
}
