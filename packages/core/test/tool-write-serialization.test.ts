import { describe, expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { WriteSerialization } from "@opencode-ai/core/tool/write-serialization"
import { Session } from "@opencode-ai/schema/session"

const session = (id: string) => Session.ID.make(id)

const sleep = (ms: number) => Effect.promise(() => Bun.sleep(ms))

describe("WriteSerialization", () => {
  test("serializes concurrent writes in one session", () =>
    Effect.gen(function* () {
      let running = 0
      let peak = 0
      const order: string[] = []
      const work = (name: string, release: Deferred.Deferred<void>) =>
        WriteSerialization.serialized(
          session("ses_write_a"),
          Effect.gen(function* () {
            running += 1
            peak = Math.max(peak, running)
            order.push(`start:${name}`)
            yield* Deferred.await(release)
            order.push(`end:${name}`)
            running -= 1
            return name
          }),
        )
      const scope = yield* Effect.scope
      const firstGate = yield* Deferred.make<void>()
      const secondGate = yield* Deferred.make<void>()
      const first = yield* work("first", firstGate).pipe(Effect.forkIn(scope))
      const second = yield* work("second", secondGate).pipe(Effect.forkIn(scope))
      // Let both fibers reach the lock, then release in order.
      yield* sleep(25)
      yield* Deferred.succeed(firstGate, undefined)
      yield* Fiber.join(first)
      yield* Deferred.succeed(secondGate, undefined)
      yield* Fiber.join(second)
      expect(peak).toBe(1)
      expect(order).toEqual(["start:first", "end:first", "start:second", "end:second"])
    }).pipe(Effect.scoped, Effect.runPromise),
  )

  test("lets different sessions write in parallel", () =>
    Effect.gen(function* () {
      let running = 0
      let peak = 0
      const scope = yield* Effect.scope
      const gate = yield* Deferred.make<void>()
      const work = (id: string) =>
        WriteSerialization.serialized(
          session(id),
          Effect.gen(function* () {
            running += 1
            peak = Math.max(peak, running)
            yield* Deferred.await(gate)
            running -= 1
          }),
        )
      const a = yield* work("ses_write_b").pipe(Effect.forkIn(scope))
      const b = yield* work("ses_write_c").pipe(Effect.forkIn(scope))
      yield* sleep(25)
      expect(peak).toBe(2)
      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.join(a)
      yield* Fiber.join(b)
      expect(peak).toBe(2)
    }).pipe(Effect.scoped, Effect.runPromise),
  )
})
