import { describe, expect, test } from "bun:test"
import { peerMessageEntry, teamTimelineEntries } from "../src/component/team-timeline"

describe("team timeline entries", () => {
  test("parses peer labels from inbox text", () => {
    expect(peerMessageEntry("survey-2", { text: "From survey-1 (leader):\nstatus?", time: 100 })).toEqual({
      time: 100,
      from: "survey-1 (leader)",
      to: "survey-2",
      text: "status?",
    })
    expect(peerMessageEntry("survey-2", { text: "From Boss:\nkeep going", time: 200 })).toEqual({
      time: 200,
      from: "Boss",
      to: "survey-2",
      text: "keep going",
    })
  })

  test("ignores ordinary user messages", () => {
    expect(peerMessageEntry("boss", { text: "hello world", time: 1 })).toBeUndefined()
    expect(peerMessageEntry("boss", { text: "From nowhere", time: 1 })).toBeUndefined()
  })

  test("merges history with live events in time order", () => {
    const entries = teamTimelineEntries(
      [{ owner: "survey-2", text: "From survey-1 (leader):\nfirst", time: 300 }],
      [
        { time: 100, from: "survey-2 (member)", to: "Boss", text: "", rejected: "Only the leader can message Boss" },
        { time: 200, from: "Boss", to: "survey-1", text: "second" },
      ],
    )
    expect(entries.map((entry) => entry.text || entry.rejected)).toEqual([
      "Only the leader can message Boss",
      "second",
      "first",
    ])
    expect(entries[0]).toMatchObject({ from: "survey-2 (member)", to: "Boss" })
  })
})
