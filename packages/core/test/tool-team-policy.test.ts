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

describe("TeamPolicy §8 routing table (decide)", () => {
  test("default allows Boss to leader and member peers", () => {
    expect(TeamPolicy.decide("boss", { kind: "peer", name: "survey-1", role: "leader" })).toEqual({
      allowed: true,
    })
    expect(TeamPolicy.decide("boss", { kind: "peer", name: "survey-2", role: "member" })).toEqual({
      allowed: true,
    })
  })

  test("default allows leader to Boss and to member peers", () => {
    expect(TeamPolicy.decide("leader", { kind: "boss" })).toEqual({ allowed: true })
    expect(TeamPolicy.decide("leader", { kind: "peer", name: "survey-2", role: "member" })).toEqual({
      allowed: true,
    })
  })

  test("default allows member to leader and member peers", () => {
    expect(TeamPolicy.decide("member", { kind: "peer", name: "survey-1", role: "leader" })).toEqual({
      allowed: true,
    })
    expect(TeamPolicy.decide("member", { kind: "peer", name: "survey-3", role: "member" })).toEqual({
      allowed: true,
    })
  })

  test("default denies only member to Boss, with leader guidance", () => {
    const verdict = TeamPolicy.decide("member", { kind: "boss" })
    expect(verdict.allowed).toBe(false)
    expect(verdict.reason).toContain("Only the leader can message Boss")
    expect(verdict.reason).toContain("via the leader")
  })
})

describe("TeamPolicy §9 config rules (decide)", () => {
  test("member to Boss allow override succeeds", () => {
    const rules: TeamPolicy.Rule[] = [{ from: "member", to: "boss", effect: "allow" }]
    expect(TeamPolicy.decide("member", { kind: "boss" }, rules)).toEqual({ allowed: true })
  })

  test("member to Boss explicit deny keeps leader guidance", () => {
    const rules: TeamPolicy.Rule[] = [{ from: "member", to: "boss", effect: "deny" }]
    const verdict = TeamPolicy.decide("member", { kind: "boss" }, rules)
    expect(verdict.allowed).toBe(false)
    expect(verdict.reason).toContain("Only the leader can message Boss")
    expect(verdict.reason).toContain("via the leader")
  })

  test("leader to Boss deny uses generic reason", () => {
    const rules: TeamPolicy.Rule[] = [{ from: "leader", to: "boss", effect: "deny" }]
    const verdict = TeamPolicy.decide("leader", { kind: "boss" }, rules)
    expect(verdict.allowed).toBe(false)
    expect(verdict.reason).toContain("not allowed")
    expect(verdict.reason).not.toContain("Only the leader can message Boss")
  })

  test("Boss to member deny uses generic reason", () => {
    const rules: TeamPolicy.Rule[] = [{ from: "boss", to: "member", effect: "deny" }]
    const verdict = TeamPolicy.decide(
      "boss",
      { kind: "peer", name: "survey-2", role: "member" },
      rules,
    )
    expect(verdict.allowed).toBe(false)
    expect(verdict.reason).toContain("not allowed")
  })

  test("leader to peer deny blocks member delivery", () => {
    const rules: TeamPolicy.Rule[] = [{ from: "leader", to: "peer", effect: "deny" }]
    const verdict = TeamPolicy.decide(
      "leader",
      { kind: "peer", name: "survey-2", role: "member" },
      rules,
    )
    expect(verdict.allowed).toBe(false)
  })

  test("per-team scoping lives in the tool, not decide (see tool-team.test.ts §9)", () => {
    // TeamPolicy.decide is team-agnostic: it only sees the caller's role,
    // the recipient, and one team's rule list. Per-team selection happens in
    // TeamTool.rulesFor (packages/core/src/tool/plugin/team.ts:97-103), which
    // loads Config entries and picks the entry matching the sender's teamID.
    // The end-to-end per-team test (open allows, closed denies) lives in
    // tool-team.test.ts; here we pin that the same rule list allows one call.
    const open: TeamPolicy.Rule[] = [{ from: "member", to: "boss", effect: "allow" }]
    expect(TeamPolicy.decide("member", { kind: "boss" }, open)).toEqual({ allowed: true })
    expect(TeamPolicy.decide("member", { kind: "boss" }, [])).not.toEqual({ allowed: true })
  })
})
