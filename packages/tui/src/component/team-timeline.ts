/**
 * Team timeline entries (spec §28): agent-to-agent communication only.
 *
 * Deliveries persist as inbox user messages labelled by the sender
 * (`From Boss:` / `From <name> (<role>):`), so history is rebuilt from them.
 * Live arrivals and rejections come from `team.message.sent` /
 * `team.message.rejected` events, which never reach an inbox.
 */
export interface TimelineEntry {
  readonly time: number
  readonly from: string
  readonly to: string
  readonly text: string
  readonly rejected?: string
}

const senderLabel = /^From (Boss|.+ \((?:leader|member)\)):\n?([\s\S]*)$/

export function peerMessageEntry(
  owner: string,
  message: { readonly text: string; readonly time: number },
): TimelineEntry | undefined {
  const match = senderLabel.exec(message.text)
  if (!match) return undefined
  return { time: message.time, from: match[1]!, to: owner, text: (match[2] ?? "").trimStart() }
}

export function teamTimelineEntries(
  deliveries: ReadonlyArray<{ owner: string; text: string; time: number }>,
  live: ReadonlyArray<{ time: number; from: string; to: string; text: string; rejected?: string }>,
): TimelineEntry[] {
  const history = deliveries.flatMap((item) => {
    const entry = peerMessageEntry(item.owner, { text: item.text, time: item.time })
    return entry ? [entry] : []
  })
  return [...history, ...live].toSorted((a, b) => a.time - b.time || (a.from < b.from ? -1 : 1))
}
