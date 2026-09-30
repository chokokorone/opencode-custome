export * as ConfigTeam from "./team.js"

import { Schema } from "effect"

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
})
export type Info = typeof Info.Type
