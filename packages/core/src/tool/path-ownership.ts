export * as PathOwnership from "./path-ownership.js"

import path from "path"
import { Effect } from "effect"
import { ToolFailure } from "@opencode-ai/ai"
import { Session } from "../session.js"
import { SessionTeam } from "../session/team.js"

const within = (root: string, target: string) => {
  const relative = path.relative(root, target)
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative))
}

/**
 * Path ownership for team members (spec: writes are owned by path).
 *
 * A member may write only inside its own workspace directory
 * (`<project>/workspace/<team>/<name>/`, assigned at spawn) and its own test
 * area (`<project>/test/<team>/<name>/`). Everything else is denied with a
 * reason. Leaders, Boss, plain subagents and reads are unrestricted: this
 * gate is intentionally narrow so it cannot stall normal work.
 */
export const assertMemberWrite = (input: {
  sessions: Session.Interface
  team: SessionTeam.Interface
  sessionID: Session.ID
  absolutePath: string
}) =>
  Effect.gen(function* () {
    const membership = yield* input.team.membership(input.sessionID)
    if (!membership || membership.role !== "member") return
    const parent = yield* input.sessions.get(membership.parentID).pipe(
      Effect.mapError((error) => new ToolFailure({ message: `Parent session not found: ${membership.parentID}`, error })),
    )
    const projectDir = parent.location.directory
    const roots = [
      path.join(projectDir, "workspace", membership.teamID, membership.name),
      path.join(projectDir, "test", membership.teamID, membership.name),
    ]
    if (roots.some((root) => within(root, input.absolutePath))) return
    return yield* new ToolFailure({
      message: [
        `Path ${input.absolutePath} is outside ${membership.name}'s areas.`,
        `Members may write only inside workspace/${membership.teamID}/${membership.name}/ and test/${membership.teamID}/${membership.name}/.`,
        "Ask the leader to integrate changes elsewhere.",
      ].join(" "),
    })
  })
