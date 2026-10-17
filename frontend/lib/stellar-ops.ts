/**
 * Signing-free Soroban operation builders for the StreamPay contract.
 *
 * This module must stay importable outside the browser bundle (parity tests,
 * headless tooling): no Freighter, no RPC, no env-derived state.
 */
import { Address, Contract, nativeToScVal, xdr } from "@stellar/stellar-sdk";
import { durationSecondsFromDays } from "../../scripts/lib/duration.ts";

export function scAddr(s: string) {
  return nativeToScVal(new Address(s), { type: "address" });
}

export function u64(id: string | number | bigint) {
  return nativeToScVal(BigInt(id), { type: "u64" });
}

export function i128(v: string) {
  return nativeToScVal(BigInt(v), { type: "i128" });
}

export function u32(v: number) {
  return nativeToScVal(BigInt(v), { type: "u32" });
}

export function buildCreateStreamOperation(
  contractId: string,
  sender: string,
  recipient: string,
  token: string,
  deposit: string,
  startTime: bigint,
  endTime: bigint,
  cancelable: boolean
): xdr.Operation {
  return new Contract(contractId).call(
    "create_stream",
    scAddr(sender),
    scAddr(recipient),
    scAddr(token),
    i128(deposit),
    u64(startTime),
    u64(endTime),
    nativeToScVal(cancelable)
  );
}

export function buildCreateStreamOperationForDays(
  contractId: string,
  sender: string,
  recipient: string,
  token: string,
  deposit: string,
  startTime: bigint,
  days: string,
  cancelable: boolean
): xdr.Operation {
  return buildCreateStreamOperation(
    contractId,
    sender,
    recipient,
    token,
    deposit,
    startTime,
    startTime + durationSecondsFromDays(days),
    cancelable
  );
}

export function buildWithdrawOperation(
  contractId: string,
  streamId: string,
  amount: string
): xdr.Operation {
  return new Contract(contractId).call(
    "withdraw",
    u64(streamId),
    i128(amount)
  );
}
