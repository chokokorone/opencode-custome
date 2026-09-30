import { describe, expect } from "bun:test"
import { Context, Effect, Exit, Layer, LayerMap, Schema, Scope } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { makeGlobalNode } from "@opencode-ai/util/effect/app-node"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { Instance } from "@opencode-ai/core/instance/service"
import { Job } from "@opencode-ai/core/job"
import { KV } from "@opencode-ai/core/kv"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import type { LocationServices } from "@opencode-ai/core/location-services"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Session } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionRestart } from "@opencode-ai/core/session/execution/restart"
import { SessionInbox } from "@opencode-ai/core/session/inbox"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionRunner } from "@opencode-ai/core/session/runner/index"
import { SessionInboxTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { testEffect } from "./lib/effect"
import { tmpdirScoped } from "./fixture/tmpdir"

/** Wakes observed by the stub execution, so the sweep needs no real drain. */
const woken: Session.ID[] = []

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionStore.node, SessionInbox.node, Job.node, KV.node, Session.node]),
  ),
)

/**
 * The restart service needs an execution and a Session whose `synthetic` honours
 * `resume`, both of which the bare graph above leaves out. Execution drains are
 * counted rather than run, so nothing here depends on wall-clock time.
 */
const restarted = Effect.gen(function* () {
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  const store = yield* SessionStore.Service
  const jobs = yield* Job.Service
  const sessions = yield* Session.Service
  const scope = yield* Scope.make()
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  const drained: Session.ID[] = []
  const sessionLayer = Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const execution = yield* SessionExecution.Service
      return Session.Service.of({
        ...sessions,
        synthetic: (input) =>
          sessions
            .synthetic({ ...input, resume: false })
            .pipe(Effect.tap(() => (input.resume === false ? Effect.void : execution.wake(input.sessionID)))),
      })
    }),
  )
  const runner = Layer.succeed(
    SessionRunner.Service,
    SessionRunner.Service.of({
      drain: (input) =>
        Effect.sync(() => void drained.push(input.sessionID)).pipe(
          Effect.andThen(SessionInbox.promote(database.db, bus, input.sessionID, "steer")),
          Effect.as(SessionRunner.DrainResult.Complete()),
        ),
    }),
  )
  const stubExecution = Layer.succeed(
    SessionExecution.Service,
    SessionExecution.Service.of({
      active: Effect.succeed(new Set<Session.ID>()),
      isActive: () => Effect.succeed(false),
      resume: (sessionID) => Effect.sync(() => void drained.push(sessionID)),
      wake: (sessionID) => Effect.sync(() => void woken.push(sessionID)),
      interrupt: () => Effect.succeed(false),
      awaitIdle: () => Effect.void,
    }),
  )
  const locations = Layer.effect(
    LocationServiceMap.Service,
    LayerMap.make(
      () =>
        // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
        runner as unknown as Layer.Layer<LocationServices>,
    ),
  )
  const context = yield* Layer.buildWithScope(
    SessionRestart.layer().pipe(
      Layer.provideMerge(sessionLayer),
      Layer.provideMerge(stubExecution),
      Layer.provide(Layer.succeed(Database.Service, database)),
      Layer.provide(Layer.succeed(Bus.Service, bus)),
      Layer.provide(Layer.succeed(SessionStore.Service, store)),
      Layer.provide(Layer.succeed(Job.Service, jobs)),
      Layer.provide(locations),
      // Do not reuse the outer harness's selector with its already-captured Location map.
      Layer.provide(
        AppNodeBuilder.build(Instance.node, [LocationServiceMap.node.replace(locations)]).pipe(Layer.fresh),
      ),
    ),
    scope,
  )
  return {
    restart: Context.get(context, SessionRestart.Service),
    execution: Context.get(context, SessionExecution.Service),
    store,
    drained,
  }
})

const seed = (
  database: Database.Service["Service"],
  ids: ReadonlyArray<Session.ID>,
  values: Partial<Pick<typeof SessionTable.$inferInsert, "time_suspended" | "parent_id">> = {},
) =>
  database.db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
    .pipe(
      Effect.andThen(
        database.db
          .insert(SessionTable)
          .values(
            ids.map((id) => ({
              id,
              project_id: Project.ID.global,
              slug: id,
              directory: "/project",
              title: id,
              version: "test",
              ...values,
            })),
          )
          .run()
          .pipe(Effect.orDie),
      ),
    )

const encodeUser = Schema.encodeSync(SessionInbox.UserPayload)

/** Writes one pending user input, the state a lost wake leaves behind. */
const admit = (database: Database.Service["Service"], sessionID: Session.ID, text: string) =>
  database.db
    .insert(SessionInboxTable)
    .values({
      id: SessionMessage.ID.create(),
      session_id: sessionID,
      type: "user",
      payload: encodeUser({ text }),
      delivery: "steer",
      enqueued_seq: 1,
    })
    .run()
    .pipe(Effect.orDie)

/** Notices the restart sweep left in a parent's inbox, oldest first. */
const notices = (database: Database.Service["Service"], parent: Session.ID) =>
  SessionInbox.list(database.db, parent).pipe(
    Effect.map((items) =>
      items.map((item) => ({
        id: item.id,
        text: item.type === "synthetic" ? item.payload.text : "",
        childID: item.type === "synthetic" ? (item.payload.metadata?.childID as string | undefined) : undefined,
      })),
    ),
  )

describe("SessionRestart orphan notification", () => {
  it.effect("notifies the parent of a child that lost its claim and has no Job record", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const harness = yield* restarted
      const parent = Session.ID.make("ses_orphan_parent")
      const child = Session.ID.make("ses_orphan_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })

      yield* harness.restart.resumeSuspendedSessions

      const delivered = yield* notices(database, parent)
      expect(delivered).toHaveLength(1)
      expect(delivered[0]?.childID).toBe(child)
      expect(delivered[0]?.text).toContain("Server restarted while you were working")
      expect(delivered[0]?.text).toContain("will not be resumed automatically")
    }),
  )

  it.effect("leaves a child that the recovery loop resumes unnotified", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const harness = yield* restarted
      const parent = Session.ID.make("ses_recovered_parent")
      const child = Session.ID.make("ses_recovered_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })
      // A durable record makes this child recoverable, so it is not an orphan.
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Recovered work",
        },
        run: Effect.never,
      })
      yield* jobs.background(child)

      yield* harness.restart.resumeSuspendedSessions

      // The child was handed back to recoverSubagent, not terminalized: its durable
      // record is still running. (The resume itself is forked, so its drain is not
      // awaited here — asserting on it would reintroduce a timing dependency.)
      expect((yield* jobs.pendingBackground).map((job) => job.recovery.kind)).toEqual(["subagent"])
      expect(yield* jobs.get(child)).toMatchObject({ status: "running" })
      expect(yield* notices(database, parent)).toEqual([])
    }),
  )

  it.effect("does not notify for a child a Job record already reports", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const harness = yield* restarted
      const parent = Session.ID.make("ses_kv_parent")
      const child = Session.ID.make("ses_kv_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Durable work",
        },
        run: Effect.never,
      })
      yield* jobs.background(child)

      yield* harness.restart.resumeSuspendedSessions
      yield* harness.restart.resumeSuspendedSessions

      // recoverSubagent owns this outcome; the orphan sweep must not also report it.
      expect(yield* notices(database, parent)).toEqual([])
      expect((yield* jobs.pendingBackground).map((job) => job.recovery.kind)).toEqual(["subagent"])
    }),
  )

  it.effect("re-notifies an orphan only once across repeated sweeps", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const harness = yield* restarted
      const parent = Session.ID.make("ses_twice_parent")
      const child = Session.ID.make("ses_twice_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })

      yield* harness.restart.resumeSuspendedSessions
      const first = yield* notices(database, parent)
      yield* harness.restart.resumeSuspendedSessions
      yield* harness.restart.resumeSuspendedSessions
      const after = yield* notices(database, parent)

      expect(first).toHaveLength(1)
      expect(after).toHaveLength(1)
      // The same row rather than a second one: the id is derived from the child id.
      expect(after[0]?.id).toBe(first[0]?.id)
      expect(String(after[0]?.id)).toBe("msg_twice_child_orphan")
    }),
  )

  it.effect("does not wake a suspended parent while notifying it", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const harness = yield* restarted
      const store = yield* SessionStore.Service
      const parent = Session.ID.make("ses_idle_parent")
      const child = Session.ID.make("ses_idle_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })

      yield* harness.restart.resumeSuspendedSessions

      // resume:false keeps the parent claimed; only its inbox grew.
      expect(harness.drained).not.toContain(parent)
      expect(yield* harness.execution.active).not.toContain(parent)
      expect(yield* harness.store.listSuspended()).toContain(parent)
      expect(yield* notices(database, parent)).toHaveLength(1)
    }),
  )
})

describe("SessionRestart pending inbox sweep", () => {
  it.effect("wakes idle sessions holding pending inbox work and skips claimed ones", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const harness = yield* restarted

      const idle = Session.ID.make("ses_sweep_idle")
      const claimed = Session.ID.make("ses_sweep_claimed")
      const untouched = Session.ID.make("ses_sweep_untouched")
      yield* seed(database, [idle, claimed, untouched])
      // A claim marks the Session as the suspended sweep's responsibility.
      yield* store.claim(claimed)
      // Admit durable work directly, standing in for a wake lost to a crash: the rows
      // exist in the inbox but nothing ever woke their Sessions.
      yield* admit(database, idle, "queued")
      yield* admit(database, claimed, "orphaned")

      woken.length = 0
      yield* harness.restart.resumeSuspendedSessions

      // The inbox sweep is what wakes `idle`: it owns pending work and no claim. A
      // claimed Session is woken by the suspended sweep instead, and one with no
      // pending inbox is never woken at all.
      expect(woken).toContain(idle)
      expect(woken).not.toContain(untouched)
    }),
  )
})
