export * as LogDigest from "./digest.js"

export interface DigestRow {
  readonly seq: number
  readonly kind: string
  readonly team: string | null | undefined
  readonly agent: string
  readonly summary: string
  readonly time_created: number
}

export interface DigestInput {
  readonly projectID?: string
  readonly since?: number
  readonly maxEntries?: number
}

const KIND_ORDER = ["failure", "finding", "decision", "question", "request", "note", "digest"] as const

const DEFAULT_MAX_ENTRIES = 50

const kindRank = (kind: string): number => KIND_ORDER.indexOf(kind as (typeof KIND_ORDER)[number])

const teamLabel = (team: string | null | undefined): string =>
  team && team.length > 0 ? team : "-"

const formatEntry = (row: DigestRow): string => `- L${row.seq} | ${teamLabel(row.team)} | ${row.summary}`

const formatSection = (kind: string, rows: readonly DigestRow[]): string =>
  [`## ${kind}`, ...rows.map(formatEntry)].join("\n")

const timeRange = (rows: readonly DigestRow[]): string => {
  const times = rows.map((row) => row.time_created)
  const min = Math.min(...times)
  const max = Math.max(...times)
  return `${new Date(min).toISOString()}..${new Date(max).toISOString()}`
}

const selectRecent = (rows: readonly DigestRow[], maxEntries: number): DigestRow[] => {
  if (maxEntries <= 0) return []
  const sorted = [...rows].sort((a, b) => a.seq - b.seq)
  return sorted.slice(Math.max(0, sorted.length - maxEntries))
}

const groupByKind = (rows: readonly DigestRow[]): Map<string, DigestRow[]> => {
  const groups = new Map<string, DigestRow[]>()
  for (const row of rows) {
    const existing = groups.get(row.kind)
    if (existing) existing.push(row)
    else groups.set(row.kind, [row])
  }
  return groups
}

const orderedKinds = (groups: Map<string, DigestRow[]>): string[] => {
  const known = KIND_ORDER.filter((kind) => groups.has(kind))
  const extra = [...groups.keys()].filter((kind) => kindRank(kind) === -1)
  return [...known, ...extra]
}

/**
 * Extractive digest over project log rows. Groups pre-selected rows by kind
 * in fixed display order without summarization (LLM abstraction is future
 * work). Pure: no DB, no LLM, no Effect.
 */
export function buildDigest(rows: readonly DigestRow[], input?: DigestInput): string {
  const since = input?.since
  const maxEntries = input?.maxEntries ?? DEFAULT_MAX_ENTRIES
  const filtered = since === undefined ? [...rows] : rows.filter((row) => row.time_created >= since)
  const selected = selectRecent(filtered, maxEntries)
  const scope = input?.projectID ? ` for ${input.projectID}` : ""
  if (selected.length === 0) return `# Log digest${scope} (0 entries)`
  const header = `# Log digest${scope} (${selected.length} entries, ${timeRange(selected)})`
  const groups = groupByKind(selected)
  const sections = orderedKinds(groups).map((kind) => formatSection(kind, groups.get(kind) ?? []))
  return [header, ...sections].join("\n\n")
}
