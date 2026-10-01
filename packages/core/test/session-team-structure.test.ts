import { describe, expect, test } from "bun:test"
import { resolveStructure } from "@opencode-ai/core/session/team"
import type { ConfigTeam } from "@opencode-ai/schema/config/team"

const rules: readonly ConfigTeam.Rule[] = [{ from: "member", to: "boss", effect: "allow" }]

const full = (): ConfigTeam.Info => ({
  teamID: "research",
  rules: [...rules],
  leader: "research-1",
  members: ["research-1", "research-2"],
  reports_to: "boss",
})

describe("SessionTeam.resolveStructure", () => {
  test("returns undefined structure for an undeclared team", () => {
    const result = resolveStructure([full()], "unknown")
    expect(result).toEqual({ ok: true, structure: undefined })
  })

  test("resolves a full declaration", () => {
    const result = resolveStructure([full()], "research")
    expect(result.ok).toBe(true)
    if (!result.ok) throw new Error("expected ok")
    expect(result.structure).toMatchObject({
      teamID: "research",
      leader: "research-1",
      members: ["research-1", "research-2"],
      reportsTo: "boss",
    })
  })

  test("rejects a leader missing from members", () => {
    const config: readonly ConfigTeam.Info[] = [
      {
        teamID: "research",
        rules: [],
        leader: "research-1",
        members: ["research-2"],
      },
    ]
    const result = resolveStructure(config, "research")
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error("expected error")
    expect(result.error).toContain("research-1")
  })

  test("passes rules through untouched", () => {
    const result = resolveStructure([full()], "research")
    if (!result.ok || !result.structure) throw new Error("expected structure")
    expect(result.structure.rules).toEqual(rules)
  })

  test("returns undefined structure for empty config", () => {
    const result = resolveStructure([], "research")
    expect(result).toEqual({ ok: true, structure: undefined })
  })
})
