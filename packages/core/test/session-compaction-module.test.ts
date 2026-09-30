import { describe, expect, test } from "bun:test"
import { LanguageModel } from "@opencode-ai/ai"
import { OpenAIChat } from "@opencode-ai/ai/protocols"
import { SessionCompaction } from "@opencode-ai/core/session/compaction"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { Money } from "@opencode-ai/schema/money"
import { Effect } from "effect"

const model = LanguageModel.make({
  id: "summary-model",
  provider: "test",
  route: OpenAIChat.route,
})
const resolved = SessionRunnerModel.resolved(model, {
  capabilities: { tools: true, input: ["text", "image"], output: ["text"] },
  cost: [
    {
      input: Money.USDPerMillionTokens.make(1),
      output: Money.USDPerMillionTokens.make(2),
      cache: {
        read: Money.USDPerMillionTokens.make(0.1),
        write: Money.USDPerMillionTokens.make(0.5),
      },
    },
  ],
  limit: { context: 200_000, output: 32_000 },
})

describe("SessionCompaction module boundary", () => {
  test("a replacement implementation serves consumers through the Service tag", () =>
    Effect.gen(function* () {
      const seen: unknown[] = []
      const replacement = SessionCompaction.Service.of({
        transform: () => Effect.succeed({ dispose: Effect.void }),
        reload: () => Effect.void,
        enabled: () => {
          seen.push("enabled")
          return false
        },
        required: (input) => {
          seen.push(["required", input.messages.length])
          return true
        },
        compact: () => Effect.succeed({ status: "completed" as const }),
        compactManual: () => Effect.succeed({ status: "completed" as const }),
      })
      const outcome = yield* Effect.gen(function* () {
        const compaction = yield* SessionCompaction.Service
        const enabled = compaction.enabled()
        const required = compaction.required({ messages: [], resolved, context: undefined as never })
        const done = yield* compaction.compactManual({
          session: undefined as never,
          messages: [],
          inputID: undefined as never,
          resolveContext: () => Effect.die(new Error("unused")),
          prepare: (() => Effect.die(new Error("unused"))) as never,
        })
        return { enabled, required, done }
      }).pipe(Effect.provideService(SessionCompaction.Service, replacement))

      expect(seen).toEqual(["enabled", ["required", 0]])
      expect(outcome).toEqual({ enabled: false, required: true, done: { status: "completed" } })
    }).pipe(Effect.runPromise),
  )
})
