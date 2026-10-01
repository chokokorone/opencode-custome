import { Effect } from "effect"
import { Permission } from "@opencode-ai/core/permission"
import { Session } from "@opencode-ai/core/session"
import { Tool } from "@opencode-ai/core/tool"
import { permissionLayer } from "./permission"
import { executeTool, toolIdentity } from "./tool"

export const contentText = (settled: { content?: ReadonlyArray<Tool.Content> }): string =>
  (settled.content ?? []).map((part) => (part.type === "text" ? part.text : "")).join("\n")

export const errorMessage = (settled: { status: string; error?: { message?: string } }): string =>
  settled.status === "error" ? (settled.error?.message ?? "") : ""

export const throwingPermission = (reason: string) =>
  permissionLayer({
    ask: () => Effect.die(new Error(`permission.ask must not be called by ${reason}`)),
    assert: () => Effect.die(new Error(`permission.assert must not be called by ${reason}`)),
    reply: () => Effect.die(new Error(`permission.reply must not be called by ${reason}`)),
    get: () => Effect.succeed(undefined as Permission.Request | undefined),
    forSession: () => Effect.succeed([] as ReadonlyArray<Permission.Request>),
    list: () => Effect.succeed([] as ReadonlyArray<Permission.Request>),
  })

export const toolCall = (
  registry: Tool.Interface,
  sessionID: Session.ID,
  tool: string,
  input: Record<string, unknown>,
  calls: { calls: number },
  idPrefix?: string,
) =>
  executeTool(registry, {
    sessionID,
    ...toolIdentity,
    call: {
      type: "tool-call" as const,
      id: `call-${idPrefix ?? tool}-${(calls.calls += 1)}`,
      name: tool,
      input,
    },
  })
