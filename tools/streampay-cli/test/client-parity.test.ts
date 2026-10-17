import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { Address, Contract, nativeToScVal } from "@stellar/stellar-sdk";
import {
  buildCreateStreamOperation,
  buildWithdrawOperation,
} from "../src/cli.ts";
import { durationSecondsFromDays } from "../../../scripts/lib/duration.ts";

const fixtures = JSON.parse(readFileSync(new URL("../../../tests/client-parity/fixtures.json", import.meta.url), "utf8")) as {
  operationAddresses: { contract: string; token: string; sender: string; recipient: string };
  durationCases: Array<{ days: string; seconds: string }>;
  cases: Array<{
    deposit: string;
    startTime: string;
    endTime: string;
    withdrawAmount: string;
    cancelable: boolean;
  }>;
};

test("CLI operation builders match canonical Soroban XDR for shared fixtures", () => {
  const { contract, token, sender, recipient } = fixtures.operationAddresses;
  for (const scenario of fixtures.cases) {
    const create = buildCreateStreamOperation(
      contract,
      sender,
      recipient,
      token,
      BigInt(scenario.deposit),
      BigInt(scenario.startTime),
      BigInt(scenario.endTime),
      scenario.cancelable
    );
    const expectedCreate = new Contract(contract).call(
      "create_stream",
      nativeToScVal(Address.fromString(sender), { type: "address" }),
      nativeToScVal(Address.fromString(recipient), { type: "address" }),
      nativeToScVal(Address.fromString(token), { type: "address" }),
      nativeToScVal(BigInt(scenario.deposit), { type: "i128" }),
      nativeToScVal(BigInt(scenario.startTime), { type: "u64" }),
      nativeToScVal(BigInt(scenario.endTime), { type: "u64" }),
      nativeToScVal(scenario.cancelable)
    );
    assert.equal(create.toXDR().toString("base64"), expectedCreate.toXDR().toString("base64"));

    const withdraw = buildWithdrawOperation(contract, 1n, BigInt(scenario.withdrawAmount));
    const expectedWithdraw = new Contract(contract).call(
      "withdraw",
      nativeToScVal(1n, { type: "u64" }),
      nativeToScVal(BigInt(scenario.withdrawAmount), { type: "i128" })
    );
    assert.equal(withdraw.toXDR().toString("base64"), expectedWithdraw.toXDR().toString("base64"));
  }
});

test("exact decimal day conversion matches shared integer boundary fixtures", () => {
  for (const scenario of fixtures.durationCases) {
    assert.equal(durationSecondsFromDays(scenario.days).toString(), scenario.seconds);
  }
  assert.throws(() => durationSecondsFromDays("0"), RangeError);
  assert.throws(() => durationSecondsFromDays("36500.1"), RangeError);
  assert.throws(() => durationSecondsFromDays("1e-3"), RangeError);
});
