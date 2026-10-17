# Client Parity Fixtures

The shared inputs and expected behavioral observations live in
`tests/client-parity/fixtures.json`. It contains two create-and-withdraw
lifecycle cases, plus exact-decimal duration boundary cases. Amounts and
timestamps are decimal strings so clients cannot silently round them through
JavaScript numbers. The Rust contract test executes both lifecycles through the
Soroban test client and checks event XDR and final token balances. The CLI and
frontend tests compare their production operation builders with canonical
Stellar SDK XDR; the frontend also checks that the `createStream` duration path
builds the expected operation for each duration fixture.

This repository has two TypeScript operation-building clients in this matrix:
the frontend and `streampay-cli`. There is no separate TypeScript SDK package.
The matrix does not drive Freighter signing, submit operations to an RPC server,
or cover pause, resume, cancel, top-up, authorization failures, and zero-max
withdrawals. Fixture addresses are valid serialization inputs, not deployed
accounts or a shared sandbox deployment. This work advances
[issue #115](https://github.com/Neziahtech/streampay/issues/115), but is not a
four-client live end-to-end matrix.

Run the PR-sized checks with:

```sh
cargo test -p streampay-contract --locked client_parity_test::shared_client_parity_fixtures_match_event_xdr_and_balances
cd frontend && npm run test:parity
cd tools/streampay-cli && npm install && npm test
```

The scheduled `Nightly Client Parity` workflow runs all three paths and archives
the shared fixtures. To deliberately bless changed Rust event/balance
observations, run `node scripts/client-parity/bless.mjs`, inspect the fixture
diff, and run the normal checks again. Blessing is not performed by CI.

The Rust testutils client uses an in-process Soroban environment; it cannot
share a deployed RPC instance with the CLI or Freighter. The current matrix
therefore proves Rust behavioral goldens and byte-identical operation encoding,
but does not claim a live, signer-backed end-to-end run against one sandbox
deployment. That requires a funded sandbox identity and a headless
Freighter/signing adapter.
