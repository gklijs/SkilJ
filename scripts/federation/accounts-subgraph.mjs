// The other subgraph scripts/federation-smoke.sh composes with skilj-demo:
// an Account whose balance is skilj-demo's AccountBalance projection entity,
// so the router resolves it through skilj's `_entities`.
// usage: ACCOUNT_IDS=a,b PORT=18081 node accounts-subgraph.mjs
import http from 'node:http';
import { graphql } from 'graphql';
import gql from 'graphql-tag';
import { buildSubgraphSchema } from '@apollo/subgraph';
const typeDefs = gql`
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.5", import: ["@key"])
  type Query { accounts: [Account!]! }
  type Account @key(fields: "id") { id: ID! owner: String! balance: Demobanking_AccountBalance! }
  type Demobanking_AccountBalance @key(fields: "projectionKey", resolvable: false) { projectionKey: String! }
`;
const ids = (process.env.ACCOUNT_IDS ?? 'acc-a,acc-b').split(',');
const resolvers = {
  Query: { accounts: () => ids.map((id) => ({ id, owner: `owner of ${id}` })) },
  Account: { balance: (a) => ({ __typename: 'Demobanking_AccountBalance', projectionKey: a.id }) },
};
const schema = buildSubgraphSchema([{ typeDefs, resolvers }]);
http.createServer(async (req, res) => {
  let body = ''; for await (const c of req) body += c;
  const { query, variables, operationName } = JSON.parse(body || '{}');
  const result = await graphql({ schema, source: query, variableValues: variables, operationName });
  res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(result));
}).listen(Number(process.env.PORT ?? 18081), '127.0.0.1');
