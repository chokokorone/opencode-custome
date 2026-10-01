import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Job } from "@opencode-ai/core/job"
import { Permission } from "@opencode-ai/core/permission"
import { Session } from "@opencode-ai/core/session"
import { Tool } from "@opencode-ai/core/tool"
import { ToolWaitTool } from "@opencode-ai/core/tool/plugin/wait"
import { makeLocationNode } from "@opencode-ai/util/effect/app-node"
import { testEffect } from "./lib/effect"
import { registerToolPlugin } from "./lib/tool"
import { contentText, errorMessage, throwingPermission, toolCall } from "./lib/tool-extra"

const waitToolNode = makeLocationNode({
  name: "test/wait-tool-plugin",
  layer: Layer.effectDiscard(registerToolPlugin(ToolWaitTool.Plugin)),
  deps: [Tool.node, Job.node],
})

const fixture = () => ({ permission: throwingPermission("tool_wait") })

const withTool = <A, E>(
  f: ReturnType<typeof fixture>,
  body: (registry: Tool.Interface) => Effect.Effect<A, E, any>,
) =>
  Effect.gen(function* () {
    const registry = yield* Tool.Service
    return yield* body(registry)
  }).pipe(
    Effect.provide(
      AppNodeBuilder.build(LayerNode.group([Tool.node, Job.node, waitToolNode]), [
        Permission.node.replace(f.permission),
      ]),
    ),
  )

const it = testEffect(Layer.empty)

describe("ToolWaitTool", () => {
  it.live("waits for background jobs and reports each outcome", () =>
    withTool(fixture(), (registry) =>
      Effect.gen(function* () {
        const jobs = yield* Job.Service
        const sessionID = Session.ID.make("ses_tool_wait_owner")
        const calls = { calls: 0 }
        const call = (name: string, input: Record<string, unknown>) =>
          toolCall(registry, sessionID, "tool_wait", input, calls, name)

        const latch = yield* Deferred.make<void>()
        const done = yield* jobs.start({
          id: "job-wait-done",
          type: "test",
          metadata: { sessionID },
          run: Deferred.await(latch).pipe(Effect.as("finished work")),
        })
        yield* jobs.background(done.id)
        const slow = yield* jobs.start({
          id: "job-wait-slow",
          type: "test",
          metadata: { sessionID },
          run: Effect.never,
        })
        yield* jobs.background(slow.id)

        const waiter = yield* Effect.forkScoped(
          Effect.gen(function* () {
            const settled = yield* call("wait", { ids: [done.id, slow.id, "job-wait-missing"], timeout: 50 })
            expect(settled.status).toBe("completed")
            return contentText(settled)
          }),
        )
        // Release the latch only after the waiter is blocked, proving the
        // waiter observes completion instead of polling for it.
        yield* Effect.sleep("50 millis")
        yield* Deferred.succeed(latch, undefined)
        const report = yield* Fiber.join(waiter)
        expect(report).toContain("job-wait-done | completed | finished work")
        expect(report).toMatch(/job-wait-slow \| timeout \| still running/)
        expect(report).toContain("job-wait-missing | unknown | no such job")
        yield* jobs.cancel(slow.id)
      }),
    ),
  )

  it.live("refuses jobs owned by another session and validates input", () =>
    withTool(fixture(), (registry) =>
      Effect.gen(function* () {
        const jobs = yield* Job.Service
        const sessionID = Session.ID.make("ses_tool_wait_owner")
        const otherID = Session.ID.make("ses_tool_wait_other")
        const calls = { calls: 0 }
        const call = (name: string, input: Record<string, unknown>) =>
          toolCall(registry, sessionID, "tool_wait", input, calls, name)

        const foreign = yield* jobs.start({
          id: "job-wait-foreign",
          type: "test",
          metadata: { sessionID: otherID },
          run: Effect.succeed("secret"),
        })
        yield* jobs.background(foreign.id)
        const refused = yield* call("refused", { ids: [foreign.id] })
        expect(refused.status).toBe("completed")
        expect(contentText(refused)).toContain("job-wait-foreign | forbidden | job belongs to another session")

        for (const bad of [
          { input: { ids: [] }, reason: "at least one" },
          { input: { ids: Array.from({ length: 21 }, (_, i) => `job-${i}`) }, reason: "max 20" },
          { input: { ids: ["job-wait-foreign"], timeout: 0 }, reason: "positive" },
        ]) {
          const failed = yield* call("bad", bad.input)
          expect(failed.status).toBe("error")
          expect(errorMessage(failed)).toContain(bad.reason)
        }
      }),
    ),
  )
})
