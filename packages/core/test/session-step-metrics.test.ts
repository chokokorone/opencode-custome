import { describe, expect } from "bun:test"
import { LanguageModel, LLM, LLMEvent } from "@opencode-ai/ai"
import { OpenAIChat } from "@opencode-ai/ai/protocols/openai-chat"
import { TestLLM } from "@opencode-ai/ai/testing"
import { Agent } from "@opencode-ai/core/agent"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { SessionStep } from "@opencode-ai/core/session/runner/step"
import { SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { ToolOutput } from "@opencode-ai/core/tool-output"
import { Money } from "@opencode-ai/schema/money"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { testEffect } from "./lib/effect"

const it = testEffect(
  Layer.merge(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node]), [
      Bus.node.replace(Bus.configured({ persist: true })),
    ]),
    TestLLM.testLayer(),
  ),
)

describe("SessionStep metrics", () => {
  it.effect("records per-turn tokens, tool counts, parallelism and latency", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const llm = yield* TestLLM.Test
      const bus = yield* Bus.Service
      const sessionID = Session.ID.create()
      const assistantMessageID = SessionMessage.ID.create()
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
      yield* db
        .insert(SessionTable)
        .values({ id: sessionID, project_id: Project.ID.global, slug: "metrics", directory: "/project", version: "test" })
        .run()
      const model = SessionRunnerModel.resolved(
        LanguageModel.make({ id: "test-model", provider: "test", route: OpenAIChat.route }),
        {
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          limit: { context: 100_000, output: 1_000 },
          cost: [
            {
              input: Money.USDPerMillionTokens.make(1),
              output: Money.USDPerMillionTokens.make(2),
              cache: { read: Money.USDPerMillionTokens.make(0.1), write: Money.USDPerMillionTokens.make(0.5) },
            },
          ],
        },
      )
      const gate = yield* Deferred.make<void>()
      const steps = yield* SessionStep.make.pipe(
        Effect.provide(
          Layer.mock(Snapshot.Service)({
            capture: () => Effect.succeed(Snapshot.ID.make("snap")),
            files: () => Effect.succeed([]),
          }),
        ),
      )
      // Two tools: the first blocks until the second starts, proving peak
      // parallelism of 2 without any timing assumptions.
      let started = 0
      const releaseSecond = yield* Deferred.make<void>()
      yield* llm.push(
        TestLLM.complete(
          {
            reason: { normalized: "stop" },
            usage: {
              inputTokens: 15,
              outputTokens: 6,
              nonCachedInputTokens: 10,
              cacheReadInputTokens: 3,
              cacheWriteInputTokens: 2,
              reasoningTokens: 2,
            },
          },
          LLMEvent.toolCall({ id: "call-one", name: "test", input: {} }),
          LLMEvent.toolCall({ id: "call-two", name: "test", input: {} }),
        ),
      )
      const ended = yield* bus.subscribe(SessionEvent.Step.Ended).pipe(
        Stream.filter((event) => event.data.sessionID === sessionID),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped({ startImmediately: true }),
      )
      const result = yield* steps.attempt({
        sessionID,
        assistantMessageID,
        agent: Agent.defaultID,
        model,
        prepared: {
          retry: () => Effect.void,
          request: LLM.request({ model: model.model, prompt: "Run two tools" }),
          options: {},
          executeTool: (call) =>
            Effect.gen(function* () {
              started += 1
              if (call.call.id === "call-one") yield* Deferred.await(gate)
              if (started === 2) yield* Deferred.succeed(releaseSecond, undefined)
              if (call.call.id === "call-two") {
                yield* Deferred.await(releaseSecond)
                yield* Deferred.succeed(gate, undefined)
              }
              return { content: [{ type: "text", text: "done" }] }
            }),
        },
        retry: () => Effect.succeed({ retry: false }),
        recoverContinuation: true,
        recoverOverflow: Effect.succeed(false),
      })
      expect(SessionStep.Outcome.$is("Completed")(result)).toBe(true)
      const events = Array.from(yield* Fiber.join(ended))
      expect(events).toHaveLength(1)
      expect(events[0]?.data.metrics).toMatchObject({ toolCalls: 2, maxParallelTools: 2 })
      expect(events[0]?.data.metrics?.stepLatencyMs).toBeGreaterThanOrEqual(0)
      expect(events[0]?.data.metrics?.contextBytes).toBeGreaterThan(0)
      expect(typeof events[0]?.data.tokens?.input).toBe("number")
      expect(typeof events[0]?.data.tokens?.output).toBe("number")
    }),
  )
})
