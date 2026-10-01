export * as SessionTeamWorkspace from "./team-workspace.js"

import path from "path"
import { Effect } from "effect"
import { ToolFailure } from "@opencode-ai/ai"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { AbsolutePath } from "../schema.js"
import { Git } from "../git.js"
import { SessionSchema } from "./schema.js"
import { SessionStore } from "./store.js"
import { SessionTeam } from "./team.js"

/**
 * Lifecycle companion for team workspace directories (spec §19).
 *
 * Creation lives in the subagent tool (approval + rollback there). Removal
 * runs here, best-effort, when a team member or leader session is removed:
 * only registered git worktrees are removed, never forced, so dirty trees
 * survive for manual care. Plain (non-git) workspace directories are left
 * alone — deleting member work without a versioned safety net is out of scope.
 */
export const remove = (input: {
  team: SessionTeam.Interface
  git: Git.Interface
  store: SessionStore.Interface
  session: SessionSchema.Info
}) =>
  Effect.gen(function* () {
    const membership = yield* input.team.membership(input.session.id)
    if (!membership) return
    // Derive the workspace from the roster, not the session row: the row
    // still points at the parent directory until the pending move promotes.
    const parent = yield* input.store.get(membership.parentID)
    if (!parent) return
    const directory = AbsolutePath.make(path.join(parent.location.directory, "workspace", membership.teamID, membership.name))
    const repository = yield* input.git.repo
      .discover(parent.location.directory)
      .pipe(Effect.orElseSucceed(() => undefined))
    if (!repository) return
    const worktrees = yield* input.git.worktree.list(repository).pipe(Effect.orElseSucceed(() => []))
    if (!worktrees.some((worktree) => worktree.directory === directory)) return
    yield* input.git.worktree.remove({ repository, directory, force: false })
  })

export interface Prepared {
  readonly directory: AbsolutePath
  /** Removes a freshly created git worktree. No-op for plain directories. */
  readonly cleanup: Effect.Effect<void>
}

/**
 * Provisions one member workspace: a git worktree when the parent lives in a
 * repository (approval-gated), a plain directory otherwise. Members scaffold
 * here; path-ownership rules (not filesystem isolation) keep them from
 * colliding. Call `cleanup` when a later step fails so no droppings remain.
 */
export const prepare = (input: {
  fs: FSUtil.Interface
  git: Git.Interface
  repository: Git.Repository | undefined
  parentDir: AbsolutePath
  teamID: string
  name: string
}) =>
  Effect.gen(function* () {
    const directory = AbsolutePath.make(path.join(input.parentDir, "workspace", input.teamID, input.name))
    const repository = input.repository
    if (!repository) {
      yield* input.fs.ensureDir(directory).pipe(
        Effect.mapError(
          (error) => new ToolFailure({ message: `Failed to create workspace: ${directory}`, error }),
        ),
      )
      return { directory, cleanup: Effect.void } satisfies Prepared
    }
    yield* input.git.worktree
      .create({ repository, directory })
      .pipe(
        Effect.mapError((error) => new ToolFailure({ message: `Failed to create worktree: ${directory}`, error })),
      )
    return {
      directory,
      cleanup: input.git.worktree
        .remove({ repository, directory, force: true })
        .pipe(Effect.ignore),
    } satisfies Prepared
  })
