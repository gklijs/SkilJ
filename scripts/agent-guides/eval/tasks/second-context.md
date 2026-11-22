Wallet holders earn reward points.

Add a second bounded context, `rewards`, as a module `rewards`
(`src/rewards.rs`), next to `wallet`:

- `EarnPoints` (payload `member_id`, `points`) records `PointsEarned`
  with the same fields; `RedeemPoints` (the same payload) records
  `PointsRedeemed`. The payload structs are `EarnPointsPayload`,
  `PointsEarnedPayload`, `RedeemPointsPayload` and
  `PointsRedeemedPayload`, the event enum `RewardsEvent`.
- Redeeming more points than the member has is rejected with kind
  `insufficient_points`.
- In local development the server creates and registers `rewards` when it
  starts, like `wallet`, and prints a token for each command of both
  bounded contexts, as `rewards/EarnPoints: <token>`. Both commands can be
  triggered over REST.
- `wallet` stays as it is.

Add tests, and make sure the project's checks pass.
