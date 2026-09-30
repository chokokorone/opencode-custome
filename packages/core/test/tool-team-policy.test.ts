import { describe, expect, test } from "bun:test"
import { TeamPolicy } from "@opencode-ai/core/session/team-policy"

describe("TeamPolicy.decide", () => {
  test("denies member to Boss with leader guidance", () => {
    const verdict = TeamPolicy.decide("member", { kind: "boss" })
    expect(verdict.allowed).toBe(false)
    expect(verdict.reason).toContain("Only the leader can message Boss")
    expect(verdict.reason).toContain("via the leader")
  })

  const allowed: Array<[TeamPolicy.SenderRole, TeamPolicy.Recipient]> = [
    ["leader", { kind: "boss" }],
    ["leader", { kind: "peer", name: "survey-2", role: "member" }],
    ["member", { kind: "peer", name: "survey-1", role: "leader" }],
    ["member", { kind: "peer", name: "survey-3", role: "member" }],
    ["boss", { kind: "boss" }],
    ["boss", { kind: "peer", name: "survey-1", role: "leader" }],
    ["boss", { kind: "peer", name: "survey-2", role: "member" }],
  ]
  for (const [sender, to] of allowed) {
    const target = to.kind === "boss" ? "Boss" : to.name
    test(`allows ${sender} to ${target}`, () => {
      expect(TeamPolicy.decide(sender, to)).toEqual({ allowed: true })
    })
  }

  test("configured rules take precedence over the default", () => {
    const open: TeamPolicy.Rule[] = [{ from: "member", to: "boss", effect: "allow" }]
    expect(TeamPolicy.decide("member", { kind: "boss" }, open)).toEqual({ allowed: true })

    const locked: TeamPolicy.Rule[] = [{ from: "member", to: "peer", effect: "deny" }]
    const verdict = TeamPolicy.decide("member", { kind: "peer", name: "survey-3", role: "member" }, locked)
    expect(verdict.allowed).toBe(false)
  })

  test("first matching rule wins", () => {
    const rules: TeamPolicy.Rule[] = [
      { from: "member", to: "boss", effect: "deny" },
      { from: "member", to: "boss", effect: "allow" },
    ]
    expect(TeamPolicy.decide("member", { kind: "boss" }, rules).allowed).toBe(false)
  })

  test("peer rules match by entry role", () => {
    const leadersOnly: TeamPolicy.Rule[] = [{ from: "member", to: "leader", effect: "allow" }]
    expect(
      TeamPolicy.decide("member", { kind: "peer", name: "survey-1", role: "leader" }, leadersOnly),
    ).toEqual({ allowed: true })
    // No matching rule: falls back to the default, which allows member to member.
    expect(
      TeamPolicy.decide("member", { kind: "peer", name: "survey-3", role: "member" }, leadersOnly),
    ).toEqual({ allowed: true })
  })
})
