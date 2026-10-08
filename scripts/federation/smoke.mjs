// Drives a router serving a supergraph of skilj-demo (prefix `demo`) and
// accounts-subgraph.mjs, as scripts/federation-smoke.sh starts them
// (docs/architecture.md §194): a mutation, a projection read, a refusal
// without a credential, the Account.balance join through skilj's
// `_entities`, and - unless the WebSocket URL is `-` - a subscription.
//
// usage: ACCOUNT_IDS=a,b node smoke.mjs <http url> <ws url | -> <jwt>
// The accounts named in ACCOUNT_IDS must each hold a balance of 11.
import WebSocket from 'ws';
import { createClient } from 'graphql-ws';
const [http, wsUrl, jwt] = process.argv.slice(2);
const skipSubscription = wsUrl === '-';
const account = `acc-${Date.now()}`;
async function gql(query, variables) {
  const r = await fetch(http, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${jwt}` }, body: JSON.stringify({ query, variables }) });
  return r.json();
}
const deposit = (amount) => gql(`mutation($p: String!) { demoSubmitCommand(boundedContext: "banking", commandTypeName: "DepositMoney", payload: $p) { accepted } }`,
  { p: JSON.stringify({ account_id: account, amount }) });
let ok = true;
const check = (name, cond, detail) => { console.log(`${cond ? 'PASS' : 'FAIL'} ${name}${cond ? '' : ' ' + JSON.stringify(detail)}`); ok &&= cond; };

let r = await deposit(50);
check('mutation', r?.data?.demoSubmitCommand?.accepted === true, r);
r = await gql(`{ demoProjection(boundedContext: "banking", name: "AccountBalance", key: "${account}") { __typename ... on Demobanking_AccountBalance { projectionKey balance } } }`);
check('projection', r?.data?.demoProjection?.balance === 50 && r.data.demoProjection.projectionKey === account, r);
r = await (await fetch(http, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: `{ demoProjection(boundedContext: "banking", name: "AccountBalance", key: "${account}") { __typename } }` }) })).json();
check('no credential is refused', !r?.data?.demoProjection && Array.isArray(r?.errors), r);

const accountIds = (process.env.ACCOUNT_IDS ?? '').split(',').filter(Boolean);
r = await gql(`{ accounts { id balance { projectionKey balance } } }`);
check('Account.balance resolved through skilj _entities',
  JSON.stringify(r?.data?.accounts) === JSON.stringify(accountIds.map((id) => ({ id, balance: { projectionKey: id, balance: 11 } }))), r);

if (skipSubscription) { console.log('SKIP subscription'); process.exit(ok ? 0 : 1); }
class AuthWs extends WebSocket { constructor(url, protocols) { super(url, protocols, { headers: { authorization: `Bearer ${jwt}` } }); } }
const client = createClient({ url: wsUrl, webSocketImpl: AuthWs, connectionParams: { Authorization: `Bearer ${jwt}` }, retryAttempts: 0 });
const got = await new Promise((resolve) => {
  const timer = setTimeout(() => resolve({ timeout: true }), 15000);
  client.subscribe({ query: `subscription { demoAllEvents(boundedContext: "banking", eventTypes: ["MoneyDeposited"]) { sequence payload } }` }, {
    next: (v) => { if (v?.data?.demoAllEvents?.payload?.includes(account)) { clearTimeout(timer); resolve(v); } else if (v.errors) { clearTimeout(timer); resolve(v); } },
    error: (e) => { clearTimeout(timer); resolve({ error: String(e?.message ?? e?.reason ?? JSON.stringify(e)) }); },
    complete: () => {},
  });
  setTimeout(() => deposit(7), 1500);
});
check('subscription', got?.data?.demoAllEvents?.payload?.includes('"amount":7'), got);
await client.dispose();
process.exit(ok ? 0 : 1);
