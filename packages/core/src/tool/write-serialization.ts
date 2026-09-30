export * as WriteSerialization from "./write-serialization.js"

import { Effect } from "effect"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import type { SessionSchema } from "../session/schema.js"

const locks = KeyedMutex.makeUnsafe<string>()

/**
 * Serializes write tools (write, edit, patch, shell) per session.
 *
 * Reads stay parallel within and across sessions; writes in one session run
 * one at a time so concurrent turns cannot interleave filesystem mutations.
 * Different sessions still write in parallel — cross-agent conflicts are a
 * workspace-isolation concern (spec §19), not a scheduling one.
 */
export const serialized = <A, E, R>(sessionID: SessionSchema.ID, effect: Effect.Effect<A, E, R>) =>
  locks.withLock(`write:${sessionID}`)(effect)
