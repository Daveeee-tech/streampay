/**
 * Minimal Soroban integration for StreamPay.
 *
 * Flow for every mutating call: build the contract operation → simulate
 * (validity + resource estimate) → Freighter signs → send → poll until
 * inclusion, then decode the contract's return value. Read-only calls
 * simulate and decode the return value without signing.
 */
import {
  Account,
  Address,
  nativeToScVal,
  Networks,
  rpc,
  scValToNative,
  StrKey,
  TransactionBuilder,
  Contract,
  xdr,
} from "@stellar/stellar-sdk";
import { signTransaction } from "@stellar/freighter-api";
import type { BatchStreamEntry } from "./batch";
import { durationSecondsFromDays } from "../../scripts/lib/duration.ts";

const RPC_URL =
  process.env.NEXT_PUBLIC_SOROBAN_RPC_URL ||
  "https://soroban-testnet.stellar.org";
const NETWORK =
  process.env.NEXT_PUBLIC_NETWORK === "PUBLIC"
    ? Networks.PUBLIC
    : Networks.TESTNET;
const CONTRACT_ID = process.env.NEXT_PUBLIC_STREAMING_CONTRACT_ID || "";

function contract(): Contract {
  if (!CONTRACT_ID) {
    throw new Error(
      "NEXT_PUBLIC_STREAMING_CONTRACT_ID is not set — deploy the contract and put its id in frontend/.env.local"
    );
  }
  return new Contract(CONTRACT_ID);
}

export interface StreamArgs {
  /**
   * Token contract id (`C…`), or the `native` sentinel for native XLM —
   * resolved through [`tokenContractId`] before any contract call.
   */
  token: string;
  recipient: string;
  /** Deposit in smallest token units (stroops for classic assets), as a string. */
  deposit: string;
  /** Stream length in days (start = now). */
  days: string;
  cancelable: boolean;
}

import type { StreamView } from "./pagination";
export type { StreamView };

function scAddr(s: string) {
  return nativeToScVal(new Address(s), { type: "address" });
}

function u64(id: string | number | bigint) {
  return nativeToScVal(BigInt(id), { type: "u64" });
}

function i128(v: string) {
  return nativeToScVal(BigInt(v), { type: "i128" });
}

function u32(v: number) {
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

/**
 * Network label for localStorage keys — stream ids are per-network facts and
 * must never leak between testnet and publicnet caches.
 */
export const NETWORK_LABEL = NETWORK === Networks.PUBLIC ? "public" : "testnet";

/**
 * Sentinel the create form submits for its "Native XLM" asset choice.
 * [`tokenContractId`] resolves it to the active network's Stellar Asset
 * Contract: on Soroban, native XLM moves through its SAC exactly like any
 * other SEP-41 token (CAP-46-6), so XLM streams need no wrapped asset.
 */
export const NATIVE_XLM = "native";

/**
 * Well-known SAC contract ids for native XLM, per network — the output of
 * `stellar contract id asset --network <net> --asset native`. The address is
 * derived from the network passphrase + asset, so it is stable per network.
 */
const NATIVE_XLM_SAC: Record<string, string> = {
  testnet: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
  public: "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
};

/**
 * Resolves the create form's asset choice to a token contract id:
 *
 * - `"native"` → the active network's native XLM Stellar Asset Contract
 *   (overridable with `NEXT_PUBLIC_NATIVE_XLM_CONTRACT_ID` for other
 *   networks),
 * - anything else → validated as a `C…` contract id and returned unchanged.
 *
 * Throws for a malformed contract id so a typo fails before a transaction is
 * built instead of surfacing as an opaque simulation error.
 */
export function tokenContractId(asset: string): string {
  if (asset !== NATIVE_XLM) {
    if (!StrKey.isValidContract(asset)) {
      throw new Error(
        `"${asset}" is not a token contract id (expected a C… address).`
      );
    }
    return asset;
  }
  const override = process.env.NEXT_PUBLIC_NATIVE_XLM_CONTRACT_ID;
  if (override) return override;
  const sac = NATIVE_XLM_SAC[NETWORK_LABEL];
  if (!sac) {
    throw new Error(`Native XLM contract id unknown on "${NETWORK_LABEL}".`);
  }
  return sac;
}

/**
 * True when an on-chain stream's `token` field is native XLM on the active
 * network (used to label streams in the UI).
 */
export function isNativeXlm(token: string): boolean {
  const override = process.env.NEXT_PUBLIC_NATIVE_XLM_CONTRACT_ID;
  return (
    token === NATIVE_XLM_SAC[NETWORK_LABEL] ||
    (Boolean(override) && token === override)
  );
}

/** Map a StreamError code (contract errors surface as `Error(Contract, #N)`). */
const STREAM_ERRORS: Record<number, string> = {
  1: "StreamNotFound",
  2: "NotStreamSender",
  3: "NotStreamRecipient",
  4: "StreamNotCancellable",
  5: "StreamNotActive",
  6: "StartTimeInPast",
  7: "InvalidTimeRange",
  8: "DurationTooLong",
  9: "ZeroAmount",
  10: "AmountExceedsAvailable",
  11: "MathOverflow",
  12: "NotAuthorized",
  13: "NothingToWithdraw",
  14: "FeeRateTooHigh",
  15: "NotGovernor",
  16: "NoPendingGovernor",
  17: "InsufficientTreasuryBalance",
  18: "IntentNotFound",
  19: "IntentNotPending",
  20: "IntentNotYetExecutable",
  21: "IntentExpired",
  22: "IntentNotCancellable",
  23: "InvalidScheduleParameters",
  24: "IntentParamsMismatch",
  25: "IntentNotExpired",
  26: "DepositTransferFailed",
  27: "TopUpTransferFailed",
  28: "RecipientPayoutFailed",
  29: "SenderRefundFailed",
  30: "TreasuryPayoutFailed",
};

/** Rewrites raw simulation/tx error text into a readable StreamError name. */
export function describeError(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  const m = message.match(/Error\(Contract, #(\d+)\)/);
  if (m) {
    const name = STREAM_ERRORS[Number(m[1])];
    return name
      ? `Contract rejected the call: ${name} (#${m[1]})`
      : `Contract error #${m[1]}`;
  }
  return message;
}

/** The ed25519 all-zero key: a syntactically valid source for simulations. */
const DUMMY_SOURCE = new Account(
  "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
  "0"
);

/**
 * Builds, simulates, Freighter-signs, sends and polls a mutating contract
 * operation. Resolves with the contract method's decoded return value.
 */
async function send<T>(
  source: string,
  operation: xdr.Operation,
  decode: (v: xdr.ScVal) => T
): Promise<T> {
  const server = new rpc.Server(RPC_URL);
  const account = await server.getAccount(source);
  const tx = new TransactionBuilder(account, {
    fee: "100000",
    networkPassphrase: NETWORK,
  })
    .addOperation(operation)
    .setTimeout(60)
    .build();

  // 1. Simulate: catches contract errors early and assembles Soroban tx data.
  const simulated = await server.simulateTransaction(tx);
  if (rpc.Api.isSimulationError(simulated)) {
    throw new Error(`Simulation failed: ${simulated.error}`);
  }
  const ready = rpc.assembleTransaction(tx, simulated).build();

  // 2. Sign with Freighter.
  const { signedTxXdr } = await signTransaction(ready.toXDR(), {
    networkPassphrase: NETWORK,
  });
  const signed = TransactionBuilder.fromXDR(signedTxXdr, NETWORK);

  // 3. Send and poll for inclusion.
  const sent = await server.sendTransaction(signed);
  if (sent.status === "ERROR") {
    throw new Error(`Submit failed: ${JSON.stringify(sent.errorResult)}`);
  }

  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 1000));
    const res = await server.getTransaction(sent.hash);
    if (res.status === rpc.Api.GetTransactionStatus.FAILED) {
      throw new Error(`Transaction failed: ${JSON.stringify(res.resultXdr)}`);
    }
    if (res.status === rpc.Api.GetTransactionStatus.SUCCESS) {
      const successful = res as { returnValue: xdr.ScVal };
      return decode(successful.returnValue);
    }
    // PENDING / DUPLICATE / TRY_AGAIN_LATER — keep polling.
  }
  throw new Error("Timed out waiting for transaction inclusion");
}

/** Reads `allowance(from → spender)` from a SEP-41 token (read-only). */
async function readAllowance(
  token: string,
  from: string,
  spender: string
): Promise<bigint | null> {
  const retval = await simulateRead(token, "allowance", [
    scAddr(from),
    scAddr(spender),
  ]);
  if (!retval) return null;
  try {
    return BigInt(scValToNative(retval) as bigint);
  } catch {
    return null;
  }
}

/**
 * Makes sure the streaming contract may pull `amount` of `token` from
 * `sender` before a create/top-up is simulated. SAC allowances are exact and
 * decrease with every pull, so this signs a `token.approve` transaction with
 * Freighter **only when the current allowance is short**.
 *
 * A token that does not answer `allowance` (not SEP-41 shaped) or an RPC
 * hiccup returns without approving: the contract call that follows then
 * surfaces its own typed error (`DepositTransferFailed` / …).
 */
async function ensureAllowance(
  sender: string,
  token: string,
  amount: string
): Promise<void> {
  const current = await readAllowance(token, sender, CONTRACT_ID);
  if (current === null || current >= BigInt(amount)) return;

  const server = new rpc.Server(RPC_URL);
  const { sequence } = await server.getLatestLedger();
  // ~10 days of headroom at ~5 s/ledger; the pull happens in the very next
  // transaction.
  const liveUntilLedger = sequence + 172_800;
  await send(
    sender,
    new Contract(token).call(
      "approve",
      scAddr(sender),
      scAddr(CONTRACT_ID),
      i128(amount),
      u32(liveUntilLedger)
    ),
    () => undefined
  );
}

export async function createStream(
  sender: string,
  args: StreamArgs
): Promise<bigint> {
  const token = tokenContractId(args.token); // "native" → the network's XLM SAC
  const start = BigInt(Math.floor(Date.now() / 1000) + 1);
  // The contract pulls the deposit through a token allowance — make sure it
  // is in place before simulation (one extra Freighter signature on the
  // first stream with a given asset).
  await ensureAllowance(sender, token, args.deposit);
  const op = buildCreateStreamOperationForDays(
    CONTRACT_ID,
    sender,
    args.recipient,
    token,
    args.deposit,
    start,
    args.days,
    args.cancelable
  );
  // create_stream returns the new stream id (u64).
  return send(sender, op, (v) => BigInt(scValToNative(v) as bigint));
}

/** Creates a payroll batch with a single contract invocation. */
export async function createStreams(
  sender: string,
  token: string,
  entries: BatchStreamEntry[],
  cancelable: boolean
): Promise<bigint[]> {
  if (entries.length === 0) {
    throw new Error("Add at least one stream to the batch.");
  }

  const start = BigInt(Math.floor(Date.now() / 1000) + 1);
  const inputs = entries.map((entry) => {
    const duration = BigInt(entry.duration);
    const end = start + duration;
    if (end > (1n << 64n) - 1n) {
      throw new Error("A batch stream end time exceeds the u64 time limit.");
    }

    const fields = [
      ["cancelable", nativeToScVal(cancelable)],
      ["deposit_amount", i128(entry.deposit)],
      ["end_time", u64(end)],
      ["recipient", scAddr(entry.recipient)],
      ["start_time", u64(start)],
      ["token", scAddr(token)],
    ] as const;
    return xdr.ScVal.scvMap(
      fields.map(
        ([name, value]) =>
          new xdr.ScMapEntry({
            key: xdr.ScVal.scvSymbol(name),
            val: value,
          })
      )
    );
  });
  const op = contract().call(
    "create_streams",
    scAddr(sender),
    xdr.ScVal.scvVec(inputs)
  );
  return send(sender, op, (v) => {
    const decoded = scValToNative(v);
    if (!Array.isArray(decoded)) {
      throw new Error("Contract returned an invalid stream id list.");
    }
    return decoded.map((id) => BigInt(id));
  });
}

export async function withdrawFromStream(
  recipient: string,
  streamId: string,
  amount: string
): Promise<void> {
  await send(
    recipient,
    buildWithdrawOperation(CONTRACT_ID, streamId, amount),
    () => undefined
  );
}

/**
 * Claims the full accrued balance in one atomic call (`withdraw_max`),
 * returning the amount paid (smallest token units).
 */
export async function withdrawMaxFromStream(
  recipient: string,
  streamId: string
): Promise<bigint> {
  return send(
    recipient,
    contract().call("withdraw_max", u64(streamId)),
    (v) => BigInt(scValToNative(v) as bigint)
  );
}

export async function topUpStream(
  sender: string,
  streamId: string,
  amount: string
): Promise<void> {
  // Top-ups pull through the same token allowance as the deposit. If the
  // stream cannot be read, skip the pre-check and let `top_up` simulation
  // surface the contract's own error (e.g. StreamNotFound).
  const stream = await readStream(streamId).catch(() => null);
  if (stream) await ensureAllowance(sender, stream.token, amount);
  await send(
    sender,
    contract().call("top_up", u64(streamId), i128(amount)),
    () => undefined
  );
}

export async function cancelStream(
  caller: string,
  streamId: string
): Promise<void> {
  await send(
    caller,
    contract().call("cancel_stream", scAddr(caller), u64(streamId)),
    () => undefined
  );
}

/** Read-only contract call on any contract: simulated, never signed. */
async function simulateRead(
  contractId: string,
  method: string,
  args: xdr.ScVal[]
): Promise<xdr.ScVal | null> {
  // Reads run on whichever contract the caller names (the streaming contract
  // or, for the allowance pre-check, a token).
  if (!contractId) return null;
  try {
    const server = new rpc.Server(RPC_URL);
    const tx = new TransactionBuilder(DUMMY_SOURCE, {
      fee: "100",
      networkPassphrase: NETWORK,
    })
      .addOperation(new Contract(contractId).call(method, ...args))
      .setTimeout(30)
      .build();
    const sim = await server.simulateTransaction(tx);
    if (rpc.Api.isSimulationError(sim)) return null;
    return sim.result?.retval ?? null;
  } catch {
    return null;
  }
}

/** Read-only view of one stream. */
export async function readStream(streamId: string): Promise<StreamView> {
  const retval = await simulateRead(CONTRACT_ID, "get_stream", [u64(streamId)]);
  if (!retval) {
    throw new Error(`Stream #${streamId} not found or RPC unavailable`);
  }
  const raw = scValToNative(retval) as Record<string, unknown>;
  return {
    id: String(raw.id),
    sender: String(raw.sender),
    recipient: String(raw.recipient),
    token: String(raw.token),
    deposit: String(raw.deposit),
    withdrawn: String(raw.withdrawn),
    startTime: Number(raw.start_time),
    endTime: Number(raw.end_time),
    cancelable: Boolean(raw.cancelable),
    status: String(raw.status),
  };
}

/** Accrued-but-unwithdrawn balance of a stream at the current ledger time. */
export async function readAvailable(streamId: string): Promise<bigint> {
  const retval = await simulateRead(CONTRACT_ID, "available", [u64(streamId)]);
  if (!retval) {
    throw new Error(`Stream #${streamId} not found or RPC unavailable`);
  }
  return BigInt(scValToNative(retval) as bigint);
}

const MAX_PAGE_SIZE = 50; // mirrors the contract's MAX_PAGE_SIZE

/** One page of stream ids from an address index view (read-only). */
async function readStreamIdsPage(
  view: "recipient_streams" | "sender_streams",
  address: string,
  offset: number
): Promise<string[]> {
  const retval = await simulateRead(CONTRACT_ID, view, [
    scAddr(address),
    u32(offset),
    u32(MAX_PAGE_SIZE),
  ]);
  if (!retval) return [];
  const raw = scValToNative(retval) as bigint[];
  return (raw ?? []).map((id) => String(id));
}

/**
 * Discovers all streams for an address through the on-chain indexes,
 * following pagination until exhausted. `role` selects the index:
 * streams where the address receives funds ("recipient") or funds them
 * ("sender"). Streams that fail to read (archived, RPC hiccups) are skipped.
 */
export async function fetchStreamsFor(
  address: string,
  role: "recipient" | "sender"
): Promise<StreamView[]> {
  const view = role === "recipient" ? "recipient_streams" : "sender_streams";
  const ids: string[] = [];
  for (let offset = 0; ; offset += MAX_PAGE_SIZE) {
    const page = await readStreamIdsPage(view, address, offset);
    ids.push(...page);
    if (page.length < MAX_PAGE_SIZE) break;
  }
  const streams = await Promise.all(
    ids.map((id) => readStream(id).catch(() => null))
  );
  return streams.filter((s): s is StreamView => s !== null);
}

import {
  paginateStreamsByCursor,
  type CursorPaginationOptions,
  type PaginatedStreamsResponse,
} from "./pagination";

export {
  paginateStreamsByCursor,
  type StreamRoleFilter,
  type StreamStatusFilter,
  type PaginationDirection,
  type CursorPaginationOptions,
  type PaginatedStreamsResponse,
} from "./pagination";

/**
 * Discovers and paginates streams for any Stellar address across incoming
 * and outgoing indexes with stable cursor semantics.
 */
export async function fetchAddressStreamsPage(
  address: string,
  options: CursorPaginationOptions = {}
): Promise<PaginatedStreamsResponse> {
  const asOfTimestamp = Math.floor(Date.now() / 1000);
  const role = options.role || "all";

  let streams: StreamView[] = [];
  try {
    if (role === "incoming") {
      streams = await fetchStreamsFor(address, "recipient");
    } else if (role === "outgoing") {
      streams = await fetchStreamsFor(address, "sender");
    } else {
      const [asRecipient, asSender] = await Promise.all([
        fetchStreamsFor(address, "recipient").catch(() => []),
        fetchStreamsFor(address, "sender").catch(() => []),
      ]);
      const byId = new Map<string, StreamView>();
      for (const s of [...asRecipient, ...asSender]) {
        byId.set(s.id, s);
      }
      streams = [...byId.values()];
    }
  } catch {
    streams = [];
  }

  return paginateStreamsByCursor(streams, options, asOfTimestamp);
}

