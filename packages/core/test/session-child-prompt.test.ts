import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { ChildPromptError } from "../src/session/error.js"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { Location } from "@opencode-ai/core/location"
import { testEffect } from "./lib/effect"
import { globalProjectNode } from "./lib/project"
import { promptLocationNode } from "./fixture/prompt-location"
import { tmpdirScoped } from "./fixture/tmpdir"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      Project.node,
      SessionProjector.node,
      SessionStore.node,
      Session.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      LocationServiceMap.node.replace(promptLocationNode),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

describe("Session child prompts", () => {
  it.live("rejects direct user prompts to child sessions and admits agent pathways", () =>
    tmpdirScoped().pipe(
      Effect.flatMap((tmp) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(tmp.path) })
          const sessions = yield* Session.Service
          const parent = yield* sessions.create({ location, title: "boss" })
          const child = yield* sessions.create({ parentID: parent.id, title: "member" })

          // Top-level sessions stay directly addressable.
          yield* sessions.prompt({ sessionID: parent.id, text: "hello boss", resume: false })

          // A user prompt (no agent source) to a child is rejected with guidance.
          const rejected = yield* sessions
            .prompt({ sessionID: child.id, text: "hello member", resume: false })
            .pipe(Effect.flip)
          expect(rejected).toBeInstanceOf(ChildPromptError)
          expect(rejected).toMatchObject({ _tag: "Session.ChildPromptError", sessionID: child.id })

          // Agent pathways carry a source marker and keep working.
          yield* sessions.prompt({
            sessionID: child.id,
            text: "From leader:\nwork",
            metadata: { source: "message_to_peer" },
            resume: false,
          })
          yield* sessions.prompt({
            sessionID: child.id,
            text: "spawned work",
            metadata: { source: "subagent" },
            resume: false,
          })
          const inbox = yield* sessions.inbox(child.id)
          expect(inbox.map((item) => (item.type === "user" ? item.payload.text : ""))).toEqual([
            "From leader:\nwork",
            "spawned work",
          ])
        }),
      ),
    ),
  )
})
