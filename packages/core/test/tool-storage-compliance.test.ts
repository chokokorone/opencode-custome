import { describe, expect } from "bun:test"
import { Context, Deferred, Effect, Exit, Layer, LayerMap, Scope } from "effect"
import { eq, like, sql } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Bus } from "@opencode-ai/core/bus"
import { Database } from "@opencode-ai/core/database/database"
import { Instance } from "@opencode-ai/core/instance/service"
import { Job } from "@opencode-ai/core/job"
import { KV } from "@opencode-ai/core/kv"
import { KVTable } from "@opencode-ai/core/kv/sql"
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
import { SessionRunner } from "@opencode-ai/core/session/runner/index"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import {
  SessionInboxTable,
  SessionMessageTable,
  SessionTable,
  SessionTeamTable,
} from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionTeam } from "@opencode-ai/core/session/team"
import { testEffect } from "./lib/effect"
import { globalProjectNode } from "./lib/project"
import { promptLocationNode } from "./fixture/prompt-location"

// Spec-compliance characterization for persistence & restart
// (§23 persisted entities, §47-10 job recovery). The restart matrix below
// drives the real SessionRestart service on a fresh boot object where
// feasible; the KV-durability leg additionally constructs a fresh Job
// registry over the same database (cf. job.test.ts "recovers a background
// marker after its process-local registry closes").
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionProjector.node,
      SessionStore.node,
      SessionInbox.node,
      Session.node,
      SessionTeam.node,
      Job.node,
      KV.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      LocationServiceMap.node.replace(promptLocationNode),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

// Builds the restart service over the given Job registry. Passing a freshly
// constructed Job interface simulates a successor process whose in-memory
// registry is empty but whose KV markers survived.
const restarted = (jobs: Job.Interface) =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    const bus = yield* Bus.Service
    const store = yield* SessionStore.Service
    const sessions = yield* Session.Service
    const scope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
    const drained: Array<Session.ID> = []
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
        wake: () => Effect.void,
        interrupt: () => Effect.succeed(false),
        awaitIdle: () => Effect.void,
      }),
    )
    const locations = Layer.effect(
      LocationServiceMap.Service,
      LayerMap.make(
        () =>
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
        Layer.provide(
          AppNodeBuilder.build(Instance.node, [LocationServiceMap.node.replace(locations)]).pipe(Layer.fresh),
        ),
      ),
      scope,
    )
    return {
      restart: Context.get(context, SessionRestart.Service),
      drained,
    }
  })

const seed = (
  database: Database.Interface,
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

// Notices the restart sweep left in a parent's inbox, oldest first.
const notices = (database: Database.Interface, parent: Session.ID) =>
  SessionInbox.list(database.db, parent).pipe(
    Effect.map((items) =>
      items.map((item) => ({
        id: item.id,
        text: item.type === "synthetic" ? item.payload.text : "",
        childID: item.type === "synthetic" ? (item.payload.metadata?.childID as string | undefined) : undefined,
      })),
    ),
  )

const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })

describe("tool-storage-compliance", () => {
  it.live("sessions, messages, and inbox rows survive in SQLite", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { db } = yield* Database.Service
      const bus = yield* Bus.Service
      const created = yield* sessions.create({ location, title: "storage-compliance" })

      const sessionRow = yield* db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, created.id))
        .get()
        .pipe(Effect.orDie)
      expect(sessionRow?.id).toBe(created.id)
      expect(sessionRow?.title).toBe("storage-compliance")

      yield* sessions.prompt({ sessionID: created.id, text: "persisted-question", resume: false })
      const inboxRows = yield* db
        .select()
        .from(SessionInboxTable)
        .where(eq(SessionInboxTable.session_id, created.id))
        .all()
        .pipe(Effect.orDie)
      expect(inboxRows).toHaveLength(1)
      expect(inboxRows[0]?.type).toBe("user")

      yield* SessionInbox.promote(db, bus, created.id, "steer")
      const messageRows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, created.id))
        .all()
        .pipe(Effect.orDie)
      expect(messageRows.some((row) => row.type === "user")).toBe(true)

      const messages = yield* sessions.messages({ sessionID: created.id })
      expect(messages.some((message) => message.type === "user" && message.text === "persisted-question")).toBe(true)

      yield* sessions.prompt({ sessionID: created.id, text: "queued-question", resume: false })
      const inbox = yield* sessions.inbox(created.id)
      expect(
        inbox.filter((item) => item.type === "user").map((item) => item.payload.text),
      ).toContain("queued-question")
      const queuedRows = yield* db
        .select()
        .from(SessionInboxTable)
        .where(eq(SessionInboxTable.session_id, created.id))
        .all()
        .pipe(Effect.orDie)
      expect(queuedRows.some((row) => row.type === "user")).toBe(true)
    }),
  )

  it.live("team roster rows survive in SQLite", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const team = yield* SessionTeam.Service
      const { db } = yield* Database.Service
      const parent = yield* sessions.create({ location, title: "boss" })
      const first = yield* sessions.create({ parentID: parent.id, title: "first" })
      const second = yield* sessions.create({ parentID: parent.id, title: "second" })
      const leader = yield* team.register({ parentID: parent.id, teamID: "storage", sessionID: first.id })
      const member = yield* team.register({ parentID: parent.id, teamID: "storage", sessionID: second.id })
      expect(leader.role).toBe("leader")
      expect(member.role).toBe("member")

      const rows = yield* db
        .select()
        .from(SessionTeamTable)
        .where(eq(SessionTeamTable.parent_id, parent.id))
        .all()
        .pipe(Effect.orDie)
      expect(rows.map((row) => row.name).sort()).toEqual(["storage-1", "storage-2"])
      expect(rows.map((row) => row.role).sort()).toEqual(["leader", "member"])

      expect(yield* team.membership(first.id)).toMatchObject({ name: "storage-1", role: "leader" })
      expect((yield* team.roster(leader)).map((entry) => entry.name)).toEqual(["storage-1", "storage-2"])
    }),
  )

  it.live("background job markers survive in the KV table", () =>
    Effect.gen(function* () {
      const jobs = yield* Job.Service
      const { db } = yield* Database.Service
      const gate = yield* Deferred.make<string>()
      const job = yield* jobs.start({
        type: "shell",
        recovery: {
          kind: "shell",
          sessionID: SessionSchema.ID.make("ses_storage_shell"),
          shellID: "shell_storage",
          command: "echo hi",
        },
        run: Deferred.await(gate),
      })
      const backgrounded = yield* jobs.background(job.id)
      if (!backgrounded?.notificationID) return yield* Effect.die(new Error("background marker missing"))

      const key = `job.background/${backgrounded.notificationID}`
      const raw = yield* db.select().from(KVTable).where(eq(KVTable.key, key)).get().pipe(Effect.orDie)
      expect(raw?.key).toBe(key)
      expect(raw?.value).toMatchObject({ id: job.id, status: "running" })

      const prefixed = yield* db
        .select()
        .from(KVTable)
        .where(like(KVTable.key, "job.background/%"))
        .all()
        .pipe(Effect.orDie)
      expect(prefixed.map((row) => row.key)).toContain(key)
      expect((yield* jobs.pendingBackground).find((item) => item.id === job.id)).toMatchObject({
        status: "running",
      })

      yield* Deferred.succeed(gate, "done")
      expect((yield* jobs.wait({ id: job.id })).info).toMatchObject({ status: "completed", output: "done" })
      const settled = yield* db.select().from(KVTable).where(eq(KVTable.key, key)).get().pipe(Effect.orDie)
      expect(settled?.value).toMatchObject({ id: job.id, status: "completed", output: "done" })

      yield* jobs.completeBackground(backgrounded.notificationID)
      expect(yield* db.select().from(KVTable).where(eq(KVTable.key, key)).get().pipe(Effect.orDie)).toBeUndefined()
    }),
  )

  it.live("restart sweep resumes a suspended session holding a claim", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* SessionStore.Service
      const jobs = yield* Job.Service
      const { db } = yield* Database.Service
      const created = yield* sessions.create({ location, title: "suspended" })
      yield* store.claim(created.id)
      expect(yield* store.listSuspended()).toContain(created.id)

      const harness = yield* restarted(jobs)
      yield* harness.restart.resumeSuspendedSessions

      const row = yield* db
        .select({ attempts: SessionTable.resume_attempts })
        .from(SessionTable)
        .where(eq(SessionTable.id, created.id))
        .get()
        .pipe(Effect.orDie)
      expect(row?.attempts).toBe(1)
      // The claim is never cleared by the sweep: only a terminal event
      // releases it, so a death inside the resumed turn leaves the same
      // orphaned claim for the next boot.
      expect(yield* store.listSuspended()).toContain(created.id)
      const messages = yield* sessions.messages({ sessionID: created.id })
      expect(
        messages.some((message) => message.type === "synthetic" && message.text.includes("Continue from where you left off")),
      ).toBe(true)
    }),
  )

  it.live("restart sweep re-attaches a KV-marked running subagent job on a fresh boot", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const kv = yield* KV.Service
      const parent = Session.ID.make("ses_storage_reattach_parent")
      const child = Session.ID.make("ses_storage_reattach_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })

      const previous = yield* Job.Service
      const job = yield* previous.start({
        id: "job_storage_reattach",
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Reattach work",
        },
        run: Effect.never,
      })
      const backgrounded = yield* previous.background(job.id)
      if (!backgrounded?.notificationID) return yield* Effect.die(new Error("background marker missing"))

      // Fresh boot object: an empty in-memory registry over the same database.
      const bootScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(bootScope, Exit.void))
      const freshJobs = yield* Job.make.pipe(Effect.provideService(KV.Service, kv), Scope.provide(bootScope))
      expect((yield* freshJobs.pendingBackground).map((item) => item.id)).toContain(job.id)

      const harness = yield* restarted(freshJobs)
      yield* harness.restart.resumeSuspendedSessions

      // recoverSubagent path: re-attached locally instead of orphan-notified.
      // Registration and resume accounting are synchronous in the sweep; the
      // resumed turn itself finishes on a forked fiber, so its terminal
      // status and marker cleanup are not asserted here.
      expect(yield* freshJobs.get(job.id)).toMatchObject({ id: job.id, notificationID: backgrounded.notificationID })
      const childRow = yield* database.db
        .select({ attempts: SessionTable.resume_attempts })
        .from(SessionTable)
        .where(eq(SessionTable.id, child))
        .get()
        .pipe(Effect.orDie)
      expect(childRow?.attempts).toBe(1)
      // The orphan sweep provably skipped this child: its marker made it
      // recoverable, so no orphan notice is ever delivered (a completion
      // notice from the forked re-attached turn is a different path).
      const delivered = yield* notices(database, parent)
      expect(delivered.filter((item) => item.text.includes("will not be resumed automatically"))).toEqual([])

      yield* freshJobs.completeBackground(backgrounded.notificationID)
    }),
  )

  it.live("restart sweep notifies the parent of an orphaned child without a marker", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const harness = yield* restarted(jobs)
      const parent = Session.ID.make("ses_storage_orphan_parent")
      const child = Session.ID.make("ses_storage_orphan_child")
      yield* seed(database, [parent], { time_suspended: Date.now() })
      yield* seed(database, [child], { parent_id: parent, time_suspended: Date.now() })

      yield* harness.restart.resumeSuspendedSessions

      // SubagentCompletion.deliver path: deterministic notification id, one row.
      const delivered = yield* notices(database, parent)
      expect(delivered).toHaveLength(1)
      expect(delivered[0]?.childID).toBe(child)
      expect(delivered[0]?.text).toContain("will not be resumed automatically")
      expect(String(delivered[0]?.id)).toBe("msg_storage_orphan_child_orphan")
    }),
  )

  it.live("todo table state reports current reality", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      // No todo table is bootstrapped and no Todo service exists: todos have
      // no observable SQLite persistence in the current schema.
      expect(
        yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'todo'`),
      ).toBeUndefined()
      expect(
        yield* db.all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'todo%'`,
        ),
      ).toEqual([])
    }),
  )
})
