export * as ResultReadTool from "./result-read.js"

import path from "path"
import { ToolFailure } from "@opencode-ai/ai"
import type { Context } from "@opencode-ai/plugin/effect/plugin"
import { Effect, Schema } from "effect"
import { Global } from "@opencode-ai/util/global"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { ToolOutput } from "../../tool-output.js"

const LIMIT_DEFAULT = 200
const LIMIT_MAX = 2000

const fail = (message: string) => Effect.fail(new ToolFailure({ message }))

/**
 * Reads a spilled tool-output file back in pages (spec §21: result_ref).
 *
 * The ref is either the bare spilled id (`tool_<12 hex>`) or the full path
 * from a truncation marker. Anything else is refused: this tool must never
 * become a general file reader, so the resolved path is confined to the
 * tool-output directory and the file name must match the spilled-id shape.
 */
export const resolveRef = (directory: string, ref: string): string | undefined => {
  const base = path.basename(ref.trim())
  if (!/^tool_[0-9a-f]{12}/.test(base)) return undefined
  const resolved = path.resolve(directory, base)
  if (resolved !== directory && !resolved.startsWith(directory + path.sep)) return undefined
  return resolved
}

export const Plugin = {
  id: "opencode.tool.result-read",
  effect: Effect.fn("ResultReadTool.Plugin")(function* (ctx: Context) {
    const global = yield* Global.Service
    const fs = yield* FSUtil.Service
    const directory = path.join(global.data, ToolOutput.DIRECTORY)

    yield* ctx.tool
      .transform((editor) => {
        editor.add({
          name: "result_read",
          options: { codemode: false },
          description:
            "Reads a truncated tool result back in pages. The ref is the file path from a truncation marker.",
          input: Schema.Struct({
            ref: Schema.String.annotate({ description: "Spilled output id or path from a truncation marker." }),
            offset: Schema.optionalKey(Schema.Number).annotate({ description: "First line, 0-based. Default 0." }),
            limit: Schema.optionalKey(Schema.Number).annotate({
              description: `Lines per page. Default ${LIMIT_DEFAULT}, max ${LIMIT_MAX}.`,
            }),
          }),
          output: Schema.Struct({ output: Schema.String }),
          execute: (input) =>
            Effect.gen(function* () {
              const file = resolveRef(directory, input.ref)
              if (!file) return yield* fail(`Invalid ref "${input.ref}": expected a spilled tool-output id.`)
              const offset = input.offset ?? 0
              if (!Number.isInteger(offset) || offset < 0)
                return yield* fail("Offset must be a non-negative integer.")
              const limit = input.limit ?? LIMIT_DEFAULT
              if (!Number.isInteger(limit) || limit < 1)
                return yield* fail("Limit must be a positive integer.")
              if (limit > LIMIT_MAX) return yield* fail(`Limit too large (${limit}, max ${LIMIT_MAX}).`)
              const text = yield* fs.readFileStringSafe(file).pipe(
                Effect.mapError((error) => new ToolFailure({ message: `Unable to read spilled output: ${input.ref}`, error })),
              )
              if (text === undefined) return yield* fail(`No spilled output for ref "${input.ref}".`)
              const lines = text.split("\n")
              if (text.endsWith("\n")) lines.pop()
              const page = lines.slice(offset, offset + limit)
              const head = `lines ${offset + 1}-${offset + page.length} of ${lines.length} (${file})`
              const output = page.length > 0 ? `${head}\n${page.join("\n")}` : `${head}\n(empty)`
              return { output: { output }, content: output, metadata: { total: lines.length } }
            }),
        })
      })
      .pipe(Effect.orDie)
  }),
}
