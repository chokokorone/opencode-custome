import { createMemo, createSignal, onCleanup, onMount } from "solid-js"
import type { SessionInfo } from "@opencode-ai/client"
import { useDialog } from "../ui/dialog"
import { DialogSelect } from "../ui/dialog-select"
import { Locale } from "../util/locale"
import { useData } from "../context/data"
import { useEvent } from "../context/event"
import { useRoute } from "../context/route"
import { teamTimelineEntries, type TimelineEntry } from "./team-timeline"

type LiveEntry = { time: number; from: string; to: string; text: string; rejected?: string }

function ownerName(session: SessionInfo, hasChildren: boolean): string {
  const match = /^([\w-]+) \((?:leader|member)\)/.exec(session.title ?? "")
  if (match) return match[1]!
  if (hasChildren) return "Boss"
  return session.title || session.id
}

function familyIDs(sessions: readonly SessionInfo[], sessionID: string): Set<string> {
  const byID = new Map(sessions.map((session) => [session.id, session]))
  const start = byID.get(sessionID)
  if (!start) return new Set()
  let root: SessionInfo = start
  for (;;) {
    const parent = root.parentID ? byID.get(root.parentID) : undefined
    if (!parent) break
    root = parent
  }
  const ids = new Set([root.id])
  let grown = true
  while (grown) {
    grown = false
    for (const session of sessions) {
      if (session.parentID && ids.has(session.parentID) && !ids.has(session.id)) {
        ids.add(session.id)
        grown = true
      }
    }
  }
  return ids
}

export function DialogTeamTimeline() {
  const dialog = useDialog()
  const route = useRoute()
  const data = useData()
  const events = useEvent()
  const [live, setLive] = createSignal<LiveEntry[]>([])

  onMount(() => {
    dialog.setSize("large")
  })

  onCleanup(
    events.on("team.message.sent", (event) => {
      setLive((entries) => [
        ...entries,
        { time: event.created, from: event.data.from, to: event.data.to, text: event.data.text },
      ])
    }),
  )
  onCleanup(
    events.on("team.message.rejected", (event) => {
      setLive((entries) => [
        ...entries,
        {
          time: event.created,
          from: event.data.from,
          to: event.data.to,
          text: "",
          rejected: event.data.reason,
        },
      ])
    }),
  )

  const options = createMemo(() => {
    const currentID = route.data.type === "session" ? route.data.sessionID : undefined
    if (!currentID) return []
    const sessions = data.session.list()
    const family = familyIDs(sessions, currentID)
    const byID = new Map(sessions.map((session) => [session.id, session]))
    const children = new Set(sessions.map((session) => session.parentID).filter((id) => id !== undefined))
    const deliveries: Array<{ owner: string; text: string; time: number }> = []
    const owners = new Map<string, string>()
    for (const id of family) {
      const session = byID.get(id)
      if (!session) continue
      const owner = ownerName(session, children.has(session.id))
      owners.set(id, owner)
      for (const message of data.session.message.list(id)) {
        if (message.type !== "user") continue
        deliveries.push({ owner, text: message.text, time: message.time.created })
      }
    }
    return teamTimelineEntries(deliveries, live()).map((entry: TimelineEntry, index: number) => ({
      title: entry.rejected
        ? `${entry.from} -X-> ${entry.to}: ${entry.rejected}`
        : `${entry.from} → ${entry.to}: ${entry.text.replace(/\n/g, " ")}`,
      value: `${entry.time}:${index}`,
      footer: Locale.time(entry.time),
    }))
  })

  return <DialogSelect title="Team timeline" options={options()} skipFilter renderFilter={false} />
}
