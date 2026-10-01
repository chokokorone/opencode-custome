export * as ConfigTeam from "./team.js"

import { Schema } from "effect"
import { optional } from "../schema.js"

export const Role = Schema.Literals(["boss", "leader", "member"])
export type Role = typeof Role.Type

export const Rule = Schema.Struct({
  from: Role,
  to: Schema.Union([Role, Schema.Literal("peer")]),
  effect: Schema.Literals(["allow", "deny"]),
})
export type Rule = typeof Rule.Type

export const Info = Schema.Struct({
  teamID: Schema.String,
  rules: Schema.Array(Rule),
  leader: Schema.String.pipe(optional),
  members: Schema.Array(Schema.String).pipe(optional),
  reports_to: Schema.Literal("boss").pipe(optional),
})
export type Info = typeof Info.Type
