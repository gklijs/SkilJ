The team modeled savings goals in an Event Modeling tool, and exported
the model to `models/savings.em.json`.

Build the `savings` bounded context it describes, as a module `savings`
(`src/savings.rs`, its event enum `SavingsEvent`), with its own
`src/savings.skilj.toml` that keeps everything the model says: every
command and event with its fields, the tags and the rejections.

- Its commands follow the model's specifications, including the
  rejections they name.
- It is created and registered next to `wallet` when the server starts,
  the server prints a token for each of its commands, and they can be
  triggered over REST.
- `wallet` stays as it is.

Add tests, and make sure the project's checks pass.
