# Client Parity Fixtures

The canonical inputs and expected behavioral observations live in
`tests/client-parity/fixtures.json`. Amounts and timestamps are decimal strings
so clients cannot silently round them through JavaScript numbers. Rust executes
the lifecycle through the Soroban test client and checks contract event XDR and
final token balances. The CLI and frontend tests build their production Soroban
operations and compare the encoded XDR with the Stellar SDK's canonical
operation encoding.

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
therefore proves Rust behavioral goldens and byte-identical client operation
encoding, but does not claim a live, signer-backed end-to-end run against one
sandbox deployment. That requires a funded sandbox identity and a headless
Freighter/signing adapter.