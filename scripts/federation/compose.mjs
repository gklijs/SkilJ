// Composes subgraph SDL files with Apollo's composition (what Rover and
// GraphOS run) and Hive's (what Hive Console and Hive Router run), and
// checks the API schema a router would serve exposes only what skilj
// publishes (docs/architecture.md §194).
//
// usage: node compose.mjs <name>=<file> ...
import { readFileSync } from 'node:fs';
import { parse } from 'graphql';
import { composeServices as composeApollo } from '@apollo/composition';
import { composeServices as composeHive } from '@theguild/federation-composition';

const services = process.argv.slice(2).map((arg) => {
  const [name, file] = arg.split('=');
  return { name, url: `http://${name}/graphql`, typeDefs: parse(readFileSync(file, 'utf8')) };
});

let failed = false;
const fail = (message) => {
  console.error(`FAIL ${message}`);
  failed = true;
};

const apollo = composeApollo(services);
if (apollo.errors) {
  fail(`Apollo composition:\n  ${apollo.errors.map((e) => e.message).join('\n  ')}`);
} else {
  console.log('ok   Apollo composition');
  // Root fields per skilj service prefix, as the API schema has them.
  const api = apollo.schema.toAPISchema().toGraphQLJSSchema();
  const roots = [api.getQueryType(), api.getMutationType(), api.getSubscriptionType()]
    .filter(Boolean)
    .flatMap((type) => Object.keys(type.getFields()));
  for (const { name } of services.filter((s) => s.name !== 'other')) {
    const published = [
      'Projection', 'ProjectionSchema', 'Epoch', 'ListPrivateFieldGrants', 'SubmitCommand',
      'GrantPrivateFieldAccessForEvent', 'GrantPrivateFieldAccessForCommand',
      'RevokePrivateFieldAccess', 'AllEvents', 'EventsByType', 'ProjectionUpdates',
    ].map((field) => `${name}${field}`);
    const served = roots.filter((field) => field.startsWith(name));
    const missing = published.filter((field) => !served.includes(field));
    const extra = served.filter((field) => !published.includes(field));
    if (missing.length || extra.length) {
      fail(`${name}'s API schema root fields: missing ${missing}, unexpected ${extra}`);
    } else {
      console.log(`ok   ${name} serves exactly its published root fields`);
    }
  }
}

const hive = composeHive(services);
if (hive.errors) {
  fail(`Hive composition:\n  ${hive.errors.map((e) => e.message).join('\n  ')}`);
} else {
  console.log('ok   Hive composition');
}

process.exit(failed ? 1 : 0);
