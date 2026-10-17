import { spawnSync } from "node:child_process";

const result = spawnSync(
  "cargo",
  [
    "test",
    "-p",
    "streampay-contract",
    "--locked",
    "client_parity_test::shared_client_parity_fixtures_match_event_xdr_and_balances",
  ],
  {
    stdio: "inherit",
    env: { ...process.env, STREAMPAY_PARITY_BLESS: "1" },
  }
);

if (result.error) throw result.error;
process.exitCode = result.status ?? 1;