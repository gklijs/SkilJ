// Composes a supergraph with Apollo's composition library, for
// federation-smoke.sh when Rover can't be downloaded (its host is blocked,
// or the machine is offline from it): the same composition Rover's
// supergraph plugin runs, from the npm registry instead. Prints the
// supergraph SDL a router loads.
//
// usage: node supergraph.mjs <name>=<sdl file>=<routing url> ...
import { readFileSync } from 'node:fs';
import { parse } from 'graphql';
import { composeServices } from '@apollo/composition';

const services = process.argv.slice(2).map((arg) => {
  const [name, file, url] = arg.split('=');
  return { name, url, typeDefs: parse(readFileSync(file, 'utf8')) };
});
const result = composeServices(services);
if (result.errors) {
  console.error(result.errors.map((e) => e.message).join('\n'));
  process.exit(1);
}
process.stdout.write(result.supergraphSdl);
