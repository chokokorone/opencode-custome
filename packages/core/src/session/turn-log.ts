export * as SessionTurnLog from "./turn-log.js"

import type { SessionMessage } from "./message.js"

/**
 * Rule-based turn log (spec §31, recording half).
 *
 * Each assistant turn derives one record from history alone — no model call:
 * the goal is the triggering user text, done lists the executed tools, and
 * files collects touched paths from tool inputs. `findings` and `next` need
 * judgment, so the compacting model writes those at compaction time from
 * these records plus recent messages. Persisting records is a later step;
 * until then the history itself is the store.
 */
export interface TurnDone {
  readonly tool: string
  readonly status: string
}

export interface TurnLog {
  readonly turn: number
  readonly goal: string
  readonly done: ReadonlyArray<TurnDone>
  readonly files: ReadonlyArray<string>
}

const PATH_KEYS = ["path", "directory", "workdir", "cwd", "file", "filepath"] as const

const pathsFromInput = (input: unknown): string[] => {
  if (typeof input !== "object" || input === null) return []
  const record = input as Record<string, unknown>
  const found: string[] = []
  for (const key of PATH_KEYS) {
    const value = record[key]
    if (typeof value === "string" && value.length > 0) found.push(value)
  }
  return found
}

const toolInput = (tool: SessionMessage.AssistantTool): unknown => {
  // Streaming input is still raw text; only settled states carry decoded arguments.
  if (tool.state.status === "streaming") return undefined
  return tool.state.input
}

export function summarizeTurn(turn: number, userText: string, assistant: SessionMessage.Assistant): TurnLog {
  const goal = userText.split("\n", 1)[0] ?? ""
  const done: TurnDone[] = []
  const files: string[] = []
  for (const item of assistant.content) {
    if (item.type !== "tool") continue
    done.push({ tool: item.name, status: item.state.status })
    const input = toolInput(item)
    if (input === undefined) continue
    for (const path of pathsFromInput(input)) {
      if (!files.includes(path)) files.push(path)
    }
  }
  return { turn, goal, done, files }
}

/** Derives one record per assistant turn, in history order. */
export function turnLogs(messages: ReadonlyArray<SessionMessage.Info>): TurnLog[] {  const logs: TurnLog[] = []
  let pendingGoal = ""
  let turn = 0
  for (const message of messages) {
    if (message.type === "user") {
      pendingGoal = message.text
      continue
    }
    if (message.type !== "assistant") continue
    turn += 1
    logs.push(summarizeTurn(turn, pendingGoal, message))
    pendingGoal = ""
  }
  return logs
}

/**
 * Renders turn records for a compaction prompt. Returns undefined when there
 * is nothing to list, so callers can omit the block. Capped to the most
 * recent turns: the head being summarized is old by definition, and the block
 * itself must stay small.
 */
export function renderTurnLogs(messages: ReadonlyArray<SessionMessage.Info>, maxTurns = 50): string | undefined {
  const logs = turnLogs(messages).slice(-maxTurns)
  if (logs.length === 0) return undefined
  const lines = logs.map((log) => {
    const done = log.done.map((item) => `${item.tool}(${item.status})`).join(", ") || "-"
    const files = log.files.join(", ") || "-"
    return `Turn ${log.turn} | goal: ${log.goal || "-"} | done: ${done} | files: ${files}`
  })
  return [
    "The <turn-logs> block lists each summarized turn in order: its triggering goal, executed tools, and touched files.",
    "<turn-logs>",
    ...lines,
    "</turn-logs>",
  ].join("\n")
}
