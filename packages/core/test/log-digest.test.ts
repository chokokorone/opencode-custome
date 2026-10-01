import { describe, expect, test } from "bun:test"
import { buildDigest, type DigestRow } from "@opencode-ai/core/log/digest"

const row = (overrides: Partial<DigestRow> & { seq: number }): DigestRow => ({
  kind: "note",
  team: "survey",
  agent: "member-1",
  summary: `summary ${overrides.seq}`,
  time_created: 1000 + overrides.seq,
  ...overrides,
})

describe("buildDigest", () => {
  test("groups sections in fixed kind order regardless of input order", () => {
    const rows = [
      row({ seq: 1, kind: "note", summary: "a note" }),
      row({ seq: 2, kind: "failure", summary: "a failure" }),
      row({ seq: 3, kind: "decision", summary: "a decision" }),
      row({ seq: 4, kind: "finding", summary: "a finding" }),
      row({ seq: 5, kind: "digest", summary: "an old digest" }),
      row({ seq: 6, kind: "request", summary: "a request" }),
      row({ seq: 7, kind: "question", summary: "a question" }),
    ]
    const out = buildDigest(rows, {})
    const headings = out.split("\n").filter((line) => line.startsWith("## "))
    expect(headings).toEqual([
      "## failure",
      "## finding",
      "## decision",
      "## question",
      "## request",
      "## note",
      "## digest",
    ])
  })

  test("formats entries as L-ids", () => {
    const out = buildDigest([row({ seq: 12, team: "survey", summary: "looked at logs" })], {})
    expect(out).toContain("- L12 | survey | looked at logs")
  })

  test("empty input renders a zero-entry header with no sections", () => {
    const out = buildDigest([], {})
    expect(out).toContain("0 entries")
    expect(out.split("\n").filter((line) => line.startsWith("## "))).toEqual([])
  })

  test("caps entries at maxEntries keeping the most recent rows", () => {
    const rows = Array.from({ length: 10 }, (_, i) => row({ seq: i + 1 }))
    const out = buildDigest(rows, { maxEntries: 3 })
    const entries = out.split("\n").filter((line) => line.startsWith("- L"))
    expect(entries).toHaveLength(3)
    expect(out).toContain("L8")
    expect(out).toContain("L10")
    expect(out).not.toContain("L1 |")
  })

  test("falls back to - for missing teams", () => {
    const rows = [
      row({ seq: 1, team: null, summary: "no team" }),
      row({ seq: 2, team: undefined, summary: "undefined team" }),
      row({ seq: 3, team: "", summary: "empty team" }),
    ]
    const out = buildDigest(rows, {})
    expect(out).toContain("- L1 | - | no team")
    expect(out).toContain("- L2 | - | undefined team")
    expect(out).toContain("- L3 | - | empty team")
  })

  test("header carries entry count and time range", () => {
    const rows = [row({ seq: 1, time_created: 1000 }), row({ seq: 2, time_created: 2000 })]
    const out = buildDigest(rows, {})
    const [header] = out.split("\n")
    expect(header).toContain("2 entries")
    expect(header).toContain(new Date(1000).toISOString())
    expect(header).toContain(new Date(2000).toISOString())
  })

  test("since filters out older rows", () => {
    const rows = [row({ seq: 1, time_created: 1000 }), row({ seq: 2, time_created: 3000 })]
    const out = buildDigest(rows, { since: 2000 })
    expect(out).toContain("L2")
    expect(out).not.toContain("L1 |")
  })
})
