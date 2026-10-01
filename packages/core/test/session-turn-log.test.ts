import { describe, expect, test } from "bun:test"
import { SessionTurnLog } from "@opencode-ai/core/session/turn-log"
import type { SessionMessage } from "@opencode-ai/core/session/message"

const user = (text: string): SessionMessage.Info =>
  ({
    id: `msg-${text.length}`,
    type: "user",
    text,
    time: { created: 1 },
  }) as unknown as SessionMessage.Info

const tool = (
  id: string,
  name: string,
  status: "completed" | "error" | "running",
  input: Record<string, unknown>,
): SessionMessage.AssistantTool =>
  ({
    type: "tool",
    id,
    name,
    state:
      status === "completed"
        ? { status, input, content: [{ type: "text", text: "ok" }] }
        : status === "error"
          ? { status, input, error: { _tag: "Tool.Error", message: "bad" } }
          : { status, input: {}, metadata: {} },
    time: { created: 1 },
  }) as unknown as SessionMessage.AssistantTool

const assistant = (...tools: SessionMessage.AssistantTool[]): SessionMessage.Info =>
  ({
    id: `msg-assistant-${tools.length}`,
    type: "assistant",
    content: tools,
    time: { created: 1 },
  }) as unknown as SessionMessage.Info

describe("SessionTurnLog", () => {
  test("derives goal, tools and files per turn in order", () => {
    const logs = SessionTurnLog.turnLogs([
      user("investigate auth\nsecond line"),
      assistant(
        tool("call-1", "read", "completed", { path: "src/auth.ts" }),
        tool("call-2", "grep", "completed", { path: "src", pattern: "login" }),
      ),
      user("fix it"),
      assistant(tool("call-3", "edit", "error", { path: "src/auth.ts", oldString: "a", newString: "b" })),
    ])
    expect(logs).toHaveLength(2)
    expect(logs[0]).toEqual({
      turn: 1,
      goal: "investigate auth",
      done: [
        { tool: "read", status: "completed" },
        { tool: "grep", status: "completed" },
      ],
      files: ["src/auth.ts", "src"],
    })
    expect(logs[1]).toEqual({
      turn: 2,
      goal: "fix it",
      done: [{ tool: "edit", status: "error" }],
      files: ["src/auth.ts"],
    })
  })

  test("dedupes files and skips streaming inputs", () => {
    const logs = SessionTurnLog.turnLogs([
      user("go"),
      assistant(
        tool("call-1", "write", "completed", { path: "a.txt" }),
        {
          type: "tool",
          id: "call-2",
          name: "shell",
          state: { status: "streaming", input: "sleep 60" },
          time: { created: 1 },
        } as unknown as SessionMessage.AssistantTool,
      ),
    ])
    expect(logs[0]?.files).toEqual(["a.txt"])
    expect(logs[0]?.done.map((item) => item.tool)).toEqual(["write", "shell"])
  })

  test("ignores non-assistant turns", () => {
    expect(SessionTurnLog.turnLogs([user("hello")])).toEqual([])
    expect(SessionTurnLog.turnLogs([])).toEqual([])
  })
})

describe("SessionTurnLog.renderTurnLogs", () => {
  test("renders nothing without assistant turns", () => {
    expect(SessionTurnLog.renderTurnLogs([])).toBeUndefined()
  })

  test("renders one line per turn", () => {
    const block = SessionTurnLog.renderTurnLogs([
      user("investigate auth"),
      assistant(tool("call-1", "read", "completed", { path: "src/auth.ts" })),
    ])
    expect(block).toContain("<turn-logs>")
    expect(block).toContain("Turn 1 | goal: investigate auth | done: read(completed) | files: src/auth.ts")
  })

  test("caps the block to recent turns", () => {
    const messages: SessionMessage.Info[] = []
    for (let turn = 1; turn <= 3; turn += 1) {
      messages.push(user(`goal ${turn}`))
      messages.push(assistant(tool(`call-${turn}`, "read", "completed", { path: `f${turn}.ts` })))
    }
    const block = SessionTurnLog.renderTurnLogs(messages, 2)
    expect(block).toContain("Turn 2")
    expect(block).toContain("Turn 3")
    expect(block).not.toContain("Turn 1 |")
  })
})
