import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { ToolOutput } from "@opencode-ai/core/tool-output"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { Global } from "@opencode-ai/util/global"
import { Permission } from "@opencode-ai/core/permission"
import { Session } from "@opencode-ai/core/session"
import { Tool } from "@opencode-ai/core/tool"
import { ResultReadTool } from "@opencode-ai/core/tool/plugin/result-read"
import { makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { permissionLayer } from "./lib/permission"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

const resultReadNode = makeLocationNode({
  name: "test/result-read-plugin",
  layer: Layer.effectDiscard(registerToolPlugin(ResultReadTool.Plugin)),
  deps: [Tool.node, Global.node, FSUtil.node],
})

const fixture = () =>
  permissionLayer({
    ask: () => Effect.die(new Error("permission.ask must not be called by result_read")),
    assert: () => Effect.die(new Error("permission.assert must not be called by result_read")),
  })

const withStore = <A, E>(
  body: (output: ToolOutput.Interface, registry: Tool.Interface) => Effect.Effect<A, E, any>,
  limits?: { maxLines?: number; maxBytes?: number },
) =>
  Effect.acquireUseRelease(
    Effect.promise(() => tmpdir()),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    (tmp) =>
      Effect.gen(function* () {
        const output = yield* ToolOutput.Service
        const registry = yield* Tool.Service
        if (limits) yield* output.transform((editor) => editor.configure(limits))
        return yield* body(output, registry)
      }).pipe(
        Effect.provide(
          AppNodeBuilder.build(LayerNode.group([ToolOutput.node, FSUtil.node, Tool.node, resultReadNode]), [
            Global.node.replace(Global.layerWith({ data: tmp.path })),
            Permission.node.replace(fixture()),
          ]),
        ),
      ),
  )

const it = testEffect(Layer.empty)

const text = (settled: { content?: ReadonlyArray<Tool.Content> }) =>
  (settled.content ?? []).map((part) => (part.type === "text" ? part.text : "")).join("\n")

const err = (settled: { status: string; error?: { message?: string } }) =>
  settled.status === "error" ? (settled.error?.message ?? "") : ""

const sessionID = Session.ID.make("ses_result_read_test")

describe("ResultReadTool", () => {
  it.live("reads spilled output back in pages", () =>
    withStore(
      (output, registry) =>
      Effect.gen(function* () {
        let calls = 0
        const call = (input: Record<string, unknown>) =>
          executeTool(registry, {
            sessionID,
            ...toolIdentity,
            call: { type: "tool-call" as const, id: `call-read-${(calls += 1)}`, name: "result_read", input },
          })
        const spilled = yield* output.truncate({
          output: null,
          content: [{ type: "text", text: ["one", "two", "three", "four", "five"].join("\n") }],
        })
        const ref = spilled.metadata?.outputPath
        expect(typeof ref).toBe("string")
        if (typeof ref !== "string") return

        const full = yield* call({ ref })
        expect(full.status).toBe("completed")
        expect(text(full)).toContain("lines 1-5 of 5")
        expect(text(full)).toContain("three")

        const page = yield* call({ ref, offset: 3, limit: 10 })
        expect(text(page)).toContain("lines 4-5 of 5")
        expect(text(page)).toContain("four")
        expect(text(page)).not.toContain("three\n")

        const pastEnd = yield* call({ ref, offset: 99 })
        expect(text(pastEnd)).toContain("(empty)")
      }),
      { maxLines: 2, maxBytes: 1_000 },
    ),
  )

  it.live("refuses anything but spilled ids and reports missing files", () =>
    withStore((_output, registry) =>
      Effect.gen(function* () {
        let calls = 0
        const call = (input: Record<string, unknown>) =>
          executeTool(registry, {
            sessionID,
            ...toolIdentity,
            call: { type: "tool-call" as const, id: `call-refuse-${(calls += 1)}`, name: "result_read", input },
          })
        for (const ref of ["../../etc/passwd", "/etc/passwd", "not-a-ref", "", "tool_CUSTOM", "src/index.ts"]) {
          const denied = yield* call({ ref })
          expect(denied.status).toBe("error")
          expect(err(denied)).toContain("Invalid ref")
        }
        const missing = yield* call({ ref: "tool_0123456789ab" })
        expect(missing.status).toBe("error")
        expect(err(missing)).toContain("No spilled output")

        for (const bad of [{ ref: "tool_0123456789ab", offset: -1 }, { ref: "tool_0123456789ab", limit: 0 }]) {
          const failed = yield* call(bad)
          expect(failed.status).toBe("error")
        }
        const capped = yield* call({ ref: "tool_0123456789ab", limit: 5000 })
        expect(capped.status).toBe("error")
        expect(err(capped)).toContain("max 2000")
      }),
    ),
  )
})
