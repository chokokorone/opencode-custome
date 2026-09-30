export * as TeamPolicy from "./team-policy.js"

export type SenderRole = "boss" | "leader" | "member"

export type Recipient = { readonly kind: "boss" } | { readonly kind: "peer"; readonly name: string }

export interface Verdict {
  readonly allowed: boolean
  readonly reason?: string
}

export const MEMBER_TO_BOSS_REASON =
  "Only the leader can message Boss. Please report via the leader."

export function decide(sender: SenderRole, to: Recipient): Verdict {
  if (sender === "member" && to.kind === "boss") return { allowed: false, reason: MEMBER_TO_BOSS_REASON }
  return { allowed: true }
}
