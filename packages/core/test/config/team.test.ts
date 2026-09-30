import { describe, expect } from "bun:test"
import { Config } from "@opencode-ai/core/config"
import { Document, Info } from "@opencode-ai/schema/config"
import { TeamPolicy } from "@opencode-ai/core/session/team-policy"
import { Effect, Schema } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "../plugin/fixture"

const it = testEffect(PluginTestLayer)
const decode = Schema.decodeUnknownSync(Info)

const document = (teams: unknown) =>
  new Document({ type: "document", info: decode({ teams }) })

describe("ConfigTeam", () => {
  it.live("loads team rules from config documents and applies them to routing", () =>
    Effect.gen(function* () {
      const config = yield* Config.Test
      yield* config.setEntries([
        document([
          {
            teamID: "survey",
            rules: [{ from: "member", to: "boss", effect: "allow" }],
          },
        ]),
      ])

      const entries = yield* config.entries()
      const teams = Config.latest(entries, "teams")
      expect(teams?.find((entry) => entry.teamID === "survey")?.rules).toHaveLength(1)

      const rules: TeamPolicy.Rule[] = (teams?.find((entry) => entry.teamID === "survey")?.rules ?? []).map(
        (rule) => ({ from: rule.from, to: rule.to, effect: rule.effect }),
      )
      expect(TeamPolicy.decide("member", { kind: "boss" }, rules)).toEqual({ allowed: true })
    }).pipe(Effect.provide(Config.testLayer([document([])]))),
  )
})
