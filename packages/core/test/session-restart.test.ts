import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { makeGlobalNode } from "@opencode-ai/util/effect/app-node"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { Job } from "@opencode-ai/core/job"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionRestart } from "@opencode-ai/core/session/execution/restart"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { globalProjectNode } from "./lib/project"
import { promptLocationNode } from "./fixture/prompt-location"
import { testEffect } from "./lib/effect"
import { tmpdirScoped } from "./fixture/tmpdir"

/** Wakes observed by the stub execution, so the sweep needs no real drain. */
const woken: Session.ID[] = []
const active: Set<Session.ID> = new Set()

const trackingExecutionNode = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.succeed(
    SessionExecution.Service,
    SessionExecution.Service.of({
      active: Effect.sync(() => new Set(active)),
      isActive: (sessionID) => Effect.sync(() => active.has(sessionID)),
      resume: () => Effect.void,
      wake: (sessionID) =>
        Effect.sync(() => {
          woken.push(sessionID)
        }),
      interrupt: () => Effect.succeed(false),
      awaitIdle: () => Effect.void,
    }),
  ),
  deps: [Bus.node, SessionStore.node],
})

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      Job.node,
      Project.node,
      SessionProjector.node,
      SessionStore.node,
      Session.node,
      SessionRestart.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      LocationServiceMap.node.replace(promptLocationNode),
      SessionExecution.node.replace(trackingExecutionNode),
    ],
  ),
)

describe("SessionRestart.resumeSuspendedSessions", () => {
  it.live("wakes idle sessions holding pending inbox work and skips active or claimed ones", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          woken.length = 0
          active.clear()
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const store = yield* SessionStore.Service
          const restart = yield* SessionRestart.Service

          const idle = yield* sessions.create({ location, title: "idle-pending" })
          const running = yield* sessions.create({ location, title: "running-pending" })
          const claimed = yield* sessions.create({ location, title: "claimed-pending" })
          const untouched = yield* sessions.create({ location, title: "untouched" })

          // resume:false admits durable work without waking anything, as a lost wake would.
          yield* sessions.prompt({ sessionID: idle.id, text: "queued", resume: false })
          yield* sessions.prompt({ sessionID: running.id, text: "in flight", resume: false })
          yield* sessions.prompt({ sessionID: claimed.id, text: "orphaned", resume: false })
          yield* store.claim(claimed.id)
          active.add(running.id)

          expect(woken).toEqual([])
          yield* restart.resumeSuspendedSessions

          // Active and claimed Sessions are left to their own recovery paths.
          expect(woken).toEqual([idle.id])
          expect(woken).not.toContain(untouched.id)
        }),
      ),
    ),
  )
})
