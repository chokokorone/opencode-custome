import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Git } from "@opencode-ai/core/git"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionTeamWorkspace } from "@opencode-ai/core/session/team-workspace"
import { FSUtil } from "@opencode-ai/util/fs-util"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Git.node, FSUtil.node])))

const git = (cwd: string, ...args: string[]) =>
  Effect.tryPromise(async () => {
    const proc = Bun.spawnSync(["git", ...args], { cwd, env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" } })
    if (proc.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${proc.stderr.toString()}`)
  })

const initRepo = (directory: string) =>
  Effect.gen(function* () {
    yield* git(directory, "init", "-q")
    yield* git(directory, "config", "user.email", "test@test")
    yield* git(directory, "config", "user.name", "test")
    yield* git(directory, "commit", "-q", "--allow-empty", "-m", "init")
  })

const prepare = (gitService: Git.Interface, fs: FSUtil.Interface, parentDir: string, name = "site-1") =>
  Effect.gen(function* () {
    const repository = yield* gitService.repo.discover(AbsolutePath.make(parentDir))
    return yield* SessionTeamWorkspace.prepare({
      fs,
      git: gitService,
      repository: repository ?? undefined,
      parentDir: AbsolutePath.make(parentDir),
      teamID: "site",
      name,
    })
  })

describe("SessionTeamWorkspace", () => {
  it.live("creates a git worktree in repos and cleans it up", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          yield* initRepo(dir.path)
          const gitService = yield* Git.Service
          const fs = yield* FSUtil.Service
          const prepared = yield* prepare(gitService, fs, dir.path)
          expect(prepared.directory.endsWith("/workspace/site/site-1")).toBe(true)
          const repository = yield* gitService.repo.discover(AbsolutePath.make(dir.path))
          expect(repository).toBeDefined()
          const listed = yield* gitService.worktree.list(repository!)
          expect(listed.map((entry) => entry.directory)).toContain(prepared.directory)

          yield* prepared.cleanup
          const after = yield* gitService.worktree.list(repository!)
          expect(after.map((entry) => entry.directory)).not.toContain(prepared.directory)
        }),
      ),
    ),
  )

  it.live("falls back to a plain directory outside repositories", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const gitService = yield* Git.Service
          const fs = yield* FSUtil.Service
          const prepared = yield* prepare(gitService, fs, dir.path)
          const stat = yield* Effect.promise(() => import("fs/promises").then((m) => m.stat(prepared.directory)))
          expect(stat.isDirectory()).toBe(true)
          // Plain directories have no worktree to clean.
          yield* prepared.cleanup
          const again = yield* Effect.promise(() =>
            import("fs/promises").then((m) => m.stat(prepared.directory)),
          )
          expect(again.isDirectory()).toBe(true)
        }),
      ),
    ),
  )
})
