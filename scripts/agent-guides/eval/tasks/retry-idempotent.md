A payment provider's integration deposits into wallets, and retries.

Add a client for it in this project: `src/client.rs`, with

    pub async fn deposit(base_url: &str, token: &str, wallet_id: &str, amount: i64, deposit_id: &str) -> Result<String, ...>

that triggers a `Deposit` over REST (one trigger request, not a batch)
and returns the server's answer, the response body as text. Add a binary
`deposit` (`src/bin/deposit.rs`) that takes the same five values as
arguments, in that order, calls it and prints the answer.

- When a request fails on the way (a transport error, or the server
  answering 5xx), the client retries it.
- One deposit (one `deposit_id`) is never booked twice, however often it
  is retried or the binary is run again with it, as long as the retries
  stay within the time the server remembers a request by. Say in the
  client's documentation what that time is.
- A request the server refuses (4xx) is not retried.

Add tests, and make sure the project's checks pass.
