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
    ["leader", { kind: "peer", name: "survey-2" }],
    ["member", { kind: "peer", name: "survey-1" }],
    ["member", { kind: "peer", name: "survey-3" }],
    ["boss", { kind: "boss" }],
    ["boss", { kind: "peer", name: "survey-1" }],
    ["boss", { kind: "peer", name: "survey-2" }],
  ]
  for (const [sender, to] of allowed) {
    const target = to.kind === "boss" ? "Boss" : to.name
    test(`allows ${sender} to ${target}`, () => {
      expect(TeamPolicy.decide(sender, to)).toEqual({ allowed: true })
    })
  }
})
