export * as TeamPolicy from "./team-policy.js"

export type SenderRole = "boss" | "leader" | "member"

export type Recipient = { readonly kind: "boss" } | { readonly kind: "peer"; readonly name: string; readonly role: SenderRole }

export interface Rule {
  readonly from: SenderRole
  readonly to: SenderRole | "peer"
  readonly effect: "allow" | "deny"
}

export interface Verdict {
  readonly allowed: boolean
  readonly reason?: string
}

export const MEMBER_TO_BOSS_REASON = "Only the leader can message Boss. Please report via the leader."

const matchRole = (rule: Rule, sender: SenderRole, to: Recipient): boolean => {
  if (rule.from !== sender) return false
  if (to.kind === "boss") return rule.to === "boss"
  return rule.to === "peer" || rule.to === to.role
}

const describeRecipient = (to: Recipient) => (to.kind === "boss" ? "Boss" : to.name)

export function decide(sender: SenderRole, to: Recipient, rules: readonly Rule[] = []): Verdict {
  for (const rule of rules) {
    if (!matchRole(rule, sender, to)) continue
    if (rule.effect === "allow") return { allowed: true }
    return {
      allowed: false,
      reason:
        to.kind === "boss" && sender === "member"
          ? MEMBER_TO_BOSS_REASON
          : `Messaging ${describeRecipient(to)} is not allowed for ${sender}.`,
    }
  }
  if (sender === "member" && to.kind === "boss") return { allowed: false, reason: MEMBER_TO_BOSS_REASON }
  return { allowed: true }
}
