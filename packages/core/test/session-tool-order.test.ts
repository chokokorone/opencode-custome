import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Agent } from "@opencode-ai/core/agent"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageUpdater } from "@opencode-ai/core/session/message-updater"
import type { SessionEvent } from "@opencode-ai/core/session/event"
import { Session } from "@opencode-ai/schema/session"
import { Model } from "@opencode-ai/schema/model"
import { Provider } from "@opencode-ai/schema/provider"

const sessionID = Session.ID.make("ses_tool_order")
const messageID = SessionMessage.ID.make("msg_tool_order")

const seed: SessionMessage.Assistant = {
  id: messageID,
  type: "assistant",
  agent: Agent.ID.make("build"),
  model: { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") },
  content: [],
  time: { created: Date.now() },
} as unknown as SessionMessage.Assistant

const started = (id: string, name: string) =>
  ({
    type: "session.tool.input.started",
    created: Date.now(),
    data: { sessionID, assistantMessageID: messageID, id, name },
  }) as unknown as SessionEvent.DurableEvent

const called = (id: string, name: string) =>
  ({
    type: "session.tool.called",
    created: Date.now(),
    data: {
      sessionID,
      assistantMessageID: messageID,
      id,
      name,
      input: {},
      executed: false,
    },
  }) as unknown as SessionEvent.DurableEvent

const succeeded = (id: string, name: string, text: string) =>
  ({
    type: "session.tool.success",
    created: Date.now(),
    data: {
      sessionID,
      assistantMessageID: messageID,
      id,
      name,
      content: [{ type: "text", text }],
      executed: false,
    },
  }) as unknown as SessionEvent.DurableEvent

describe("tool result ordering", () => {
  test("keeps issue order when completions arrive out of order", () =>
    Effect.gen(function* () {
      let current: SessionMessage.Assistant = seed
      const adapter: SessionMessageUpdater.Adapter = {
        getAgent: () => Effect.succeed(undefined),
        getModel: () => Effect.succeed(undefined),
        getLocation: () => Effect.die(new Error("unused")),
        getCurrentAssistant: () => Effect.succeed(current),
        getAssistant: () => Effect.succeed(current),
        getShell: () => Effect.succeed(undefined),
        getCompaction: () => Effect.succeed(undefined),
        updateAssistant: (assistant) => Effect.sync(() => void (current = assistant)),
        updateShell: () => Effect.void,
        updateCompaction: () => Effect.void,
        appendMessage: () => Effect.void,
      }
      // Issue order is A then B; B finishes first.
      yield* SessionMessageUpdater.update(adapter, started("call-a", "read"))
      yield* SessionMessageUpdater.update(adapter, started("call-b", "read"))
      yield* SessionMessageUpdater.update(adapter, called("call-a", "read"))
      yield* SessionMessageUpdater.update(adapter, called("call-b", "read"))
      yield* SessionMessageUpdater.update(adapter, succeeded("call-b", "read", "result-b"))
      yield* SessionMessageUpdater.update(adapter, succeeded("call-a", "read", "result-a"))

      const ids = current.content.flatMap((item) => (item.type === "tool" ? [item.id] : []))
      expect(ids).toEqual(["call-a", "call-b"])
      const texts = current.content.flatMap((item) =>
        item.type === "tool" && item.state.status === "completed"
          ? item.state.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
          : [],
      )
      expect(texts).toEqual(["result-a", "result-b"])
    }).pipe(Effect.runPromise),
  )
})
