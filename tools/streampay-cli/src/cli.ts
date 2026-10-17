#!/usr/bin/env node
import { chmodSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { pathToFileURL } from "node:url";
import {
  Account,
  Address,
  Contract,
  Keypair,
  Networks,
  nativeToScVal,
  rpc,
  scValToNative,
  Transaction,
  TransactionBuilder,
  xdr,
} from "@stellar/stellar-sdk";
import { decryptSecret, encryptSecret } from "./crypto.ts";
import { durationSecondsFromDays } from "../../../scripts/lib/duration.ts";

type OutputMode = "json" | "text";
type SignerMode = "env" | "file" | "ledger";

interface Options {
  command: string;
  subcommand?: string;
  values: Map<string, string>;
  flags: Set<string>;
  json: boolean;
  dryRun: boolean;
}

interface BalanceChange {
  token: string;
  address: string;
  delta: string;
  derivedFrom: string[];
}

interface SimulationReport {
  latestLedger: number;
  minResourceFee: string;
  returnValue: string | null;
  footprint: { readOnly: string[]; readWrite: string[] };
  eventXdr: string[];
  expectedBalances: {
    basis: "successful-invocation contract events";
    note: string;
    changes: BalanceChange[];
  };
}

interface PreparedBatch {
  transaction: Transaction;
  unsignedXdr: string;
  report: SimulationReport;
  streamIds: string[];
}

class CliError extends Error {
  readonly code: string;
  readonly exitCode: number;

  constructor(
    message: string,
    code: string,
    exitCode: number
  ) {
    super(message);
    this.code = code;
    this.exitCode = exitCode;
  }
}

const DEFAULT_TESTNET_RPC = "https://soroban-testnet.stellar.org";
const DEFAULT_MAINNET_RPC = "https://mainnet.sorobanrpc.com";
const DUMMY_SOURCE = new Account(
  "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
  "0"
);
const MAX_PAGE_SIZE = 50;
const MAX_CLAIM_OPERATIONS_PER_TX = 50;

function usage(): string {
  return `Usage:
  streampay [global options] <create|withdraw|cancel|top-up|claim-all> [options]
  streampay key encrypt --out <file> [--secret-env <name>] [--password-env <name>]

Global options:
  --contract <C...>       StreamPay contract id
  --network <testnet|mainnet> (default: testnet)
  --rpc-url <url>         Soroban RPC endpoint
  --signer <env|file|ledger>
  --source <G...>         Source address for ledger handoff or dry-run
  --key-file <path>       Encrypted signer file (file mode)
  --dry-run               Simulate, report footprint and do not submit
  --signed-xdr <path>     Submit a ledger-signed transaction XDR file
  --json                  Emit JSON schema v1

Commands:
  create --token <C...> --recipient <G...> --deposit <units> --days <n> [--cancelable]
  withdraw --stream-id <n> --amount <units>
  cancel --stream-id <n>
  top-up --stream-id <n> --amount <units>
  claim-all

The --signer ledger mode prints unsigned transaction XDR. Sign that exact XDR
with a Stellar hardware wallet, then pass the signed XDR using --signed-xdr.`;
}

function parseArgs(argv: string[]): Options {
  if (argv.length === 0) {
    return { command: "help", values: new Map(), flags: new Set(), json: false, dryRun: false };
  }

  const values = new Map<string, string>();
  const flags = new Set<string>();
  const positional: string[] = [];
  const booleanFlags = new Set(["--json", "--dry-run", "--cancelable", "--force", "--help"]);
  let wantsHelp = argv.includes("-h");
  for (let index = 0; index < argv.length; index += 1) {
    const part = argv[index];
    if (part === "-h") continue;
    if (!part.startsWith("--")) {
      positional.push(part);
      continue;
    }
    if (booleanFlags.has(part)) {
      flags.add(part);
      if (part === "--help") wantsHelp = true;
      continue;
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new CliError(`missing value for ${part}`, "USAGE", 2);
    }
    values.set(part.slice(2), value);
    index += 1;
  }
  const command = wantsHelp ? "help" : positional[0] ?? "help";
  const subcommand = command === "key" ? positional[1] : undefined;
  return {
    command,
    subcommand,
    values,
    flags,
    json: flags.has("--json"),
    dryRun: flags.has("--dry-run"),
  };
}

function getOption(options: Options, name: string): string | undefined {
  return options.values.get(name);
}

function required(options: Options, name: string): string {
  const value = getOption(options, name);
  if (!value) throw new CliError(`missing required --${name}`, "USAGE", 2);
  return value;
}

function parseAddress(value: string, label: string): string {
  try {
    return Address.fromString(value).toString();
  } catch {
    throw new CliError(`invalid ${label} address`, "USAGE", 2);
  }
}

function parsePositiveInteger(value: string, label: string): bigint {
  if (!/^[0-9]+$/.test(value) || BigInt(value) <= 0n) {
    throw new CliError(`${label} must be a positive integer`, "USAGE", 2);
  }
  return BigInt(value);
}

function networkConfig(options: Options): { passphrase: string; rpcUrl: string; label: string } {
  const network = getOption(options, "network") ?? "testnet";
  if (network !== "testnet" && network !== "mainnet") {
    throw new CliError("--network must be testnet or mainnet", "USAGE", 2);
  }
  const passphrase = getOption(options, "passphrase") ??
    (network === "testnet" ? Networks.TESTNET : Networks.PUBLIC);
  return {
    passphrase,
    rpcUrl: getOption(options, "rpc-url") ??
      (network === "testnet" ? DEFAULT_TESTNET_RPC : DEFAULT_MAINNET_RPC),
    label: network,
  };
}

function scAddress(value: string): xdr.ScVal {
  return nativeToScVal(Address.fromString(value), { type: "address" });
}

function scInteger(value: bigint, type: "u32" | "u64" | "i128"): xdr.ScVal {
  return nativeToScVal(value, { type });
}

export function buildCreateStreamOperation(
  contractId: string,
  source: string,
  recipient: string,
  token: string,
  deposit: bigint,
  startTime: bigint,
  endTime: bigint,
  cancelable: boolean
): xdr.Operation {
  return contractCall(
    contractId,
    "create_stream",
    scAddress(source),
    scAddress(recipient),
    scAddress(token),
    scInteger(deposit, "i128"),
    scInteger(startTime, "u64"),
    scInteger(endTime, "u64"),
    nativeToScVal(cancelable)
  );
}

export function buildWithdrawOperation(
  contractId: string,
  streamId: bigint,
  amount: bigint
): xdr.Operation {
  return contractCall(
    contractId,
    "withdraw",
    scInteger(streamId, "u64"),
    scInteger(amount, "i128")
  );
}

function contractCall(contractId: string, method: string, ...args: xdr.ScVal[]): xdr.Operation {
  return new Contract(contractId).call(method, ...args);
}

function decodeEvent(encoded: string): { name: string; data: Record<string, unknown>; topics: unknown[] } | null {
  try {
    const event = xdr.DiagnosticEvent.fromXDR(encoded, "base64").event();
    const body = event.body().v0();
    const topics = body.topics().map((topic) => scValToNative(topic));
    const data = scValToNative(body.data());
    if (!data || typeof data !== "object") return null;
    return { name: String(topics[0] ?? ""), data: data as Record<string, unknown>, topics };
  } catch {
    return null;
  }
}

function streamStatus(value: unknown): string {
  if (typeof value === "string") return value.toLowerCase();
  try {
    return JSON.stringify(value).toLowerCase();
  } catch {
    return String(value).toLowerCase();
  }
}

function eventDeltas(
  events: string[],
  contractId: string,
  streamTokens: Map<string, string>
): BalanceChange[] {
  const changes = new Map<string, BalanceChange>();
  const add = (token: string, address: unknown, delta: bigint, eventName: string) => {
    if (delta === 0n || address == null) return;
    const account = String(address);
    const key = `${token}:${account}`;
    const existing = changes.get(key);
    if (existing) {
      existing.delta = (BigInt(existing.delta) + delta).toString();
      existing.derivedFrom.push(eventName);
    } else {
      changes.set(key, { token, address: account, delta: delta.toString(), derivedFrom: [eventName] });
    }
  };

  for (const encoded of events) {
    const decoded = decodeEvent(encoded);
    if (!decoded) continue;
    const { data, name, topics } = decoded;
    const streamId = String(topics[1] ?? "");
    const token = String(data.token ?? streamTokens.get(streamId) ?? "");
    if (!token) continue;
    if (name === "StreamCreated") {
      const amount = BigInt(String(data.deposit));
      eventDeltasAdd(add, token, data.sender, contractId, amount, "StreamCreated");
    } else if (name === "FeeCharged") {
      const amount = BigInt(String(data.amount));
      eventDeltasAdd(add, token, data.payer, contractId, amount, "FeeCharged");
    } else if (name === "Withdrawn") {
      const amount = BigInt(String(data.amount));
      add(token, data.recipient, amount, "Withdrawn");
      add(token, contractId, -amount, "Withdrawn");
    } else if (name === "StreamCancelled") {
      const recipientAmount = BigInt(String(data.recipient_amount));
      const senderAmount = BigInt(String(data.sender_amount));
      const sender = streamTokens.get(`${streamId}:sender`);
      const recipient = streamTokens.get(`${streamId}:recipient`);
      add(token, recipient, recipientAmount, "StreamCancelled");
      add(token, sender, senderAmount, "StreamCancelled");
      add(token, contractId, -(recipientAmount + senderAmount), "StreamCancelled");
    } else if (name === "StreamToppedUp") {
      const amount = BigInt(String(data.amount));
      add(token, data.sender, -amount, "StreamToppedUp");
      add(token, contractId, amount, "StreamToppedUp");
    }
  }
  return [...changes.values()].filter((change) => change.delta !== "0");
}

function eventDeltasAdd(
  add: (token: string, address: unknown, delta: bigint, eventName: string) => void,
  token: string,
  payer: unknown,
  contractId: string,
  amount: bigint,
  eventName: string
) {
  add(token, payer, -amount, eventName);
  add(token, contractId, amount, eventName);
}

function readSecret(mode: SignerMode, options: Options): { keypair?: Keypair; source: string } {
  if (mode === "ledger") {
    const source = parseAddress(required(options, "source"), "source");
    return { source };
  }
  let secret: string;
  if (mode === "env") {
    const envName = getOption(options, "secret-env") ?? "STREAMPAY_SECRET_KEY";
    secret = process.env[envName] ?? "";
    if (!secret) throw new CliError(`signing secret is missing from ${envName}`, "SIGNER", 3);
  } else {
    const keyFile = required(options, "key-file");
    const passwordEnv = getOption(options, "password-env") ?? "STREAMPAY_KEY_PASSWORD";
    const password = process.env[passwordEnv];
    if (!password) throw new CliError(`password is missing from ${passwordEnv}`, "SIGNER", 3);
    try {
      secret = decryptSecret(JSON.parse(readFileSync(keyFile, "utf8")), password);
    } catch (error) {
      throw new CliError(`could not unlock key file: ${errorMessage(error)}`, "SIGNER", 3);
    }
  }
  try {
    const keypair = Keypair.fromSecret(secret.trim());
    return { keypair, source: keypair.publicKey() };
  } catch {
    throw new CliError("signing secret is not a valid Stellar secret key", "SIGNER", 3);
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function simulateRead(
  server: rpc.Server,
  contractId: string,
  passphrase: string,
  method: string,
  args: xdr.ScVal[]
): Promise<unknown> {
  const tx = new TransactionBuilder(DUMMY_SOURCE, { fee: "100", networkPassphrase: passphrase })
    .addOperation(contractCall(contractId, method, ...args))
    .setTimeout(30)
    .build();
  const result = await server.simulateTransaction(tx);
  if (rpc.Api.isSimulationError(result)) {
    throw new CliError(`simulation of ${method} failed: ${result.error}`, "SIMULATION", 4);
  }
  if (!result.result?.retval) {
    throw new CliError(`simulation of ${method} returned no value`, "SIMULATION", 4);
  }
  return scValToNative(result.result.retval);
}

async function readStream(
  server: rpc.Server,
  contractId: string,
  passphrase: string,
  streamId: string
): Promise<Record<string, unknown>> {
  const result = await simulateRead(server, contractId, passphrase, "get_stream", [
    scInteger(BigInt(streamId), "u64"),
  ]);
  return result as Record<string, unknown>;
}

async function prepareBatch(
  server: rpc.Server,
  passphrase: string,
  contractId: string,
  source: string,
  operations: xdr.Operation[],
  streamIds: string[],
  streamTokens: Map<string, string>
): Promise<PreparedBatch> {
  let account: Account;
  try {
    account = await server.getAccount(source);
  } catch (error) {
    throw new CliError(`could not load source account: ${errorMessage(error)}`, "RPC", 1);
  }
  const builder = new TransactionBuilder(account, { fee: "100000", networkPassphrase: passphrase });
  for (const operation of operations) builder.addOperation(operation);
  const tx = builder.setTimeout(60).build();
  let simulation: Awaited<ReturnType<typeof server.simulateTransaction>>;
  try {
    simulation = await server.simulateTransaction(tx);
  } catch (error) {
    throw new CliError(`Soroban RPC simulation failed: ${errorMessage(error)}`, "RPC", 1);
  }
  if (rpc.Api.isSimulationError(simulation)) {
    throw new CliError(`simulation rejected the operation: ${simulation.error}`, "SIMULATION", 4);
  }
  const transaction = rpc.assembleTransaction(tx, simulation).build();
  const transactionData = xdr.SorobanTransactionData.fromXDR(simulation.transactionData, "base64");
  const footprint = transactionData.resources().footprint();
  const events = simulation.events ?? [];
  return {
    transaction,
    unsignedXdr: transaction.toXDR(),
    streamIds,
    report: {
      latestLedger: simulation.latestLedger,
      minResourceFee: String(simulation.minResourceFee),
      returnValue: simulation.result?.retval
        ? String(scValToNative(simulation.result.retval))
        : null,
      footprint: {
        readOnly: footprint.readOnly().map((entry) => entry.toXDR("base64")),
        readWrite: footprint.readWrite().map((entry) => entry.toXDR("base64")),
      },
      eventXdr: events,
      expectedBalances: {
        basis: "successful-invocation contract events",
        note: "Token balance deltas decoded from this RPC simulation's contract events; these are not post-transaction account balances.",
        changes: eventDeltas(events, contractId, streamTokens),
      },
    },
  };
}

async function discoverClaimOperations(
  server: rpc.Server,
  contractId: string,
  passphrase: string,
  source: string
): Promise<{ ids: string[]; operations: xdr.Operation[]; tokens: Map<string, string> }> {
  const ids: string[] = [];
  for (let offset = 0; ; offset += MAX_PAGE_SIZE) {
    const page = await simulateRead(server, contractId, passphrase, "recipient_streams", [
      scAddress(source),
      scInteger(BigInt(offset), "u32"),
      scInteger(BigInt(MAX_PAGE_SIZE), "u32"),
    ]) as bigint[];
    const pageIds = (page ?? []).map(String);
    ids.push(...pageIds);
    if (pageIds.length < MAX_PAGE_SIZE) break;
    if (ids.length >= 50_000) {
      throw new CliError("claim-all stopped after 50,000 streams; narrow the wallet's stream set", "USAGE", 2);
    }
  }

  const operations: xdr.Operation[] = [];
  const eligibleIds: string[] = [];
  const tokens = new Map<string, string>();
  for (const id of ids) {
    const stream = await readStream(server, contractId, passphrase, id);
    const token = String(stream.token);
    const recipient = String(stream.recipient);
    tokens.set(id, token);
    tokens.set(`${id}:recipient`, recipient);
    tokens.set(`${id}:sender`, String(stream.sender));
    if (recipient !== source || !streamStatus(stream.status).includes("active")) continue;
    const available = await simulateRead(server, contractId, passphrase, "available", [
      scInteger(BigInt(id), "u64"),
    ]);
    if (BigInt(String(available)) <= 0n) continue;
    operations.push(contractCall(contractId, "withdraw_max", scInteger(BigInt(id), "u64")));
    eligibleIds.push(id);
  }
  return { ids: eligibleIds, operations, tokens };
}

async function buildOperations(
  options: Options,
  server: rpc.Server,
  passphrase: string,
  contractId: string,
  source: string
): Promise<{
  operations: xdr.Operation[];
  streamIds: string[];
  streamTokens: Map<string, string>;
}> {
  const streamTokens = new Map<string, string>();
  if (options.command === "create") {
    const token = parseAddress(required(options, "token"), "token");
    const recipient = parseAddress(required(options, "recipient"), "recipient");
    const deposit = parsePositiveInteger(required(options, "deposit"), "deposit");
    const daysText = required(options, "days");
    let duration: bigint;
    try {
      duration = durationSecondsFromDays(daysText);
    } catch (error) {
      throw new CliError(errorMessage(error), "USAGE", 2);
    }
    const start = BigInt(Math.floor(Date.now() / 1000) + 60);
    const end = start + duration;
    return {
      operations: [buildCreateStreamOperation(
        contractId,
        source,
        recipient,
        token,
        deposit,
        start,
        end,
        options.flags.has("--cancelable")
      )],
      streamIds: [],
      streamTokens,
    };
  }
  if (options.command === "withdraw" || options.command === "cancel" || options.command === "top-up") {
    const streamId = parsePositiveInteger(required(options, "stream-id"), "stream-id").toString();
    const stream = await readStream(server, contractId, passphrase, streamId);
    streamTokens.set(streamId, String(stream.token));
    streamTokens.set(`${streamId}:sender`, String(stream.sender));
    streamTokens.set(`${streamId}:recipient`, String(stream.recipient));
    const amount = options.command === "cancel" ? undefined :
      parsePositiveInteger(required(options, "amount"), "amount");
    const operation = options.command === "withdraw"
      ? buildWithdrawOperation(contractId, BigInt(streamId), amount!)
      : options.command === "top-up"
        ? contractCall(contractId, "top_up", scInteger(BigInt(streamId), "u64"), scInteger(amount!, "i128"))
        : contractCall(contractId, "cancel_stream", scAddress(source), scInteger(BigInt(streamId), "u64"));
    return { operations: [operation], streamIds: [streamId], streamTokens };
  }
  if (options.command === "claim-all") {
    throw new Error("claim-all operations require asynchronous stream discovery");
  }
  throw new CliError(`unknown command: ${options.command}`, "USAGE", 2);
}

async function resolveOptionsAndRun(options: Options): Promise<{ value: Record<string, unknown>; exitCode: number }> {
  if (options.command === "help") {
    return { value: { schemaVersion: 1, ok: true, help: usage() }, exitCode: 0 };
  }
  if (options.command === "key" && options.subcommand === "encrypt") {
    const output = required(options, "out");
    const secretEnv = getOption(options, "secret-env") ?? "STREAMPAY_SECRET_KEY";
    const passwordEnv = getOption(options, "password-env") ?? "STREAMPAY_KEY_PASSWORD";
    const secret = process.env[secretEnv];
    const password = process.env[passwordEnv];
    if (!secret || !password) {
      throw new CliError(`secret and password must be set in ${secretEnv} and ${passwordEnv}`, "SIGNER", 3);
    }
    if (existsSync(output) && !options.flags.has("--force")) {
      throw new CliError(`refusing to overwrite ${output}; pass --force to replace it`, "USAGE", 2);
    }
    try {
      Keypair.fromSecret(secret.trim());
      writeFileSync(output, `${JSON.stringify(encryptSecret(secret.trim(), password), null, 2)}\n`, { mode: 0o600 });
      if (process.platform !== "win32") chmodSync(output, 0o600);
    } catch (error) {
      throw new CliError(`could not create encrypted key file: ${errorMessage(error)}`, "SIGNER", 3);
    }
    return { value: { schemaVersion: 1, ok: true, command: "key encrypt", keyFile: output }, exitCode: 0 };
  }
  if (!["create", "withdraw", "cancel", "top-up", "claim-all"].includes(options.command)) {
    throw new CliError(`unknown command: ${options.command}`, "USAGE", 2);
  }

  const contractId = parseAddress(required(options, "contract"), "contract");
  const network = networkConfig(options);
  const signerMode = (getOption(options, "signer") ?? (options.dryRun ? "ledger" : "env")) as SignerMode;
  if (!["env", "file", "ledger"].includes(signerMode)) {
    throw new CliError("--signer must be env, file, or ledger", "USAGE", 2);
  }
  const signer = readSecret(signerMode, options);
  const source = signerMode === "ledger" ? signer.source :
    (getOption(options, "source") ? parseAddress(required(options, "source"), "source") : signer.source);
  if (signer.keypair && source !== signer.keypair.publicKey()) {
    throw new CliError("--source must match the selected signing key", "SIGNER", 3);
  }
  const server = new rpc.Server(network.rpcUrl);
  let operations: xdr.Operation[];
  let streamIds: string[];
  let streamTokens = new Map<string, string>();
  if (options.command === "claim-all") {
    const claim = await discoverClaimOperations(server, contractId, network.passphrase, source);
    operations = claim.operations;
    streamIds = claim.ids;
    streamTokens = claim.tokens;
  } else {
    const plan = await buildOperations(options, server, network.passphrase, contractId, source);
    operations = plan.operations;
    streamIds = plan.streamIds;
    streamTokens = plan.streamTokens;
  }

  if (operations.length === 0) {
    return {
      value: {
        schemaVersion: 1,
        ok: true,
        command: options.command,
        status: "nothing-to-claim",
        network: network.label,
        contract: contractId,
        streamIds,
        batches: [],
      },
      exitCode: 0,
    };
  }

  const signedXdrPath = getOption(options, "signed-xdr");
  const isLedgerHandoff = signerMode === "ledger";
  if (signedXdrPath && !isLedgerHandoff) {
    throw new CliError("--signed-xdr requires --signer ledger", "USAGE", 2);
  }
  const prepareOnly = options.dryRun || (isLedgerHandoff && !signedXdrPath);
  const operationLimit = isLedgerHandoff && !options.dryRun
    ? MAX_CLAIM_OPERATIONS_PER_TX
    : operations.length;
  const operationGroups: Array<{ operations: xdr.Operation[]; streamIds: string[] }> = [];
  for (let offset = 0; offset < Math.min(operations.length, operationLimit); offset += MAX_CLAIM_OPERATIONS_PER_TX) {
    operationGroups.push({
      operations: operations.slice(offset, offset + MAX_CLAIM_OPERATIONS_PER_TX),
      streamIds: streamIds.slice(offset, offset + MAX_CLAIM_OPERATIONS_PER_TX),
    });
  }
  const hasMore = operations.length > operationLimit;
  if (prepareOnly) {
    const batches: PreparedBatch[] = [];
    for (const group of operationGroups) {
      batches.push(await prepareBatch(server, network.passphrase, contractId, source, group.operations, group.streamIds, streamTokens));
    }
    return {
      value: {
        schemaVersion: 1,
        ok: true,
        command: options.command,
        status: options.dryRun ? "simulated" : "prepared",
        network: network.label,
        contract: contractId,
        streamIds: options.command === "create" && batches[0]?.report.returnValue
          ? [batches[0].report.returnValue]
          : operationGroups.flatMap((group) => group.streamIds),
        ...(hasMore ? { hasMore: true } : {}),
        batches: batches.map((batch) => ({
          unsignedXdr: batch.unsignedXdr,
          operationCount: batch.transaction.operations.length,
          simulation: batch.report,
        })),
      },
      exitCode: 0,
    };
  }

  if (isLedgerHandoff && operationGroups.length !== 1) {
    throw new CliError("submit one ledger-signed batch per invocation", "USAGE", 2);
  }
  const transactionResults: Array<{ hash: string; status: string }> = [];
  const batches: PreparedBatch[] = [];
  for (const group of operationGroups) {
    const batch = await prepareBatch(server, network.passphrase, contractId, source, group.operations, group.streamIds, streamTokens);
    batches.push(batch);
    let signed: Transaction | ReturnType<typeof TransactionBuilder.fromXDR>;
    if (isLedgerHandoff) {
      let signedContents: string;
      try {
        signedContents = readFileSync(signedXdrPath!, "utf8").trim();
        signed = TransactionBuilder.fromXDR(signedContents, network.passphrase);
      } catch (error) {
        throw new CliError(`could not read signed transaction XDR: ${errorMessage(error)}`, "SIGNER", 3);
      }
      if (!Buffer.from(signed.hash()).equals(Buffer.from(batch.transaction.hash()))) {
        throw new CliError("signed XDR does not match the simulated transaction", "SIGNER", 3);
      }
    } else {
      signed = batch.transaction;
      signed.sign(signer.keypair!);
    }
    let sent: Awaited<ReturnType<typeof server.sendTransaction>>;
    try {
      sent = await server.sendTransaction(signed);
    } catch (error) {
      throw new CliError(`transaction submission failed: ${errorMessage(error)}`, "RPC", 1);
    }
    if (sent.status === "ERROR") {
      throw new CliError(`transaction rejected: ${JSON.stringify(sent.errorResult)}`, "RPC", 1);
    }
    let finalStatus = sent.status;
    for (let attempt = 0; attempt < 30; attempt += 1) {
      const result = await server.getTransaction(sent.hash);
      if (result.status === rpc.Api.GetTransactionStatus.SUCCESS) {
        finalStatus = "SUCCESS";
        break;
      }
      if (result.status === rpc.Api.GetTransactionStatus.FAILED) {
        throw new CliError("transaction failed on-chain", "RPC", 1);
      }
      if (attempt === 29) throw new CliError("timed out waiting for transaction inclusion", "TIMEOUT", 5);
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
    transactionResults.push({ hash: sent.hash, status: finalStatus });
  }

  return {
    value: {
      schemaVersion: 1,
      ok: true,
      command: options.command,
      status: "submitted",
      network: network.label,
      contract: contractId,
      streamIds: options.command === "create" && batches[0]?.report.returnValue
        ? [batches[0].report.returnValue]
        : streamIds,
      ...(hasMore ? { hasMore: true } : {}),
      transactions: transactionResults,
      batches: batches.map((batch) => ({
        operationCount: batch.transaction.operations.length,
        simulation: batch.report,
      })),
    },
    exitCode: 0,
  };
}

function printResult(value: Record<string, unknown>, mode: OutputMode): void {
  if (mode === "json") {
    console.log(JSON.stringify(value));
    return;
  }
  if (value.help) {
    console.log(String(value.help));
    return;
  }
  if (value.command === "key encrypt") {
    console.log(`Encrypted signer key written to ${String(value.keyFile)}`);
    return;
  }
  console.log(`StreamPay ${String(value.command)}: ${String(value.status)}`);
  for (const batch of (value.batches as Array<Record<string, unknown>> | undefined) ?? []) {
    if (batch.unsignedXdr) console.log(`Unsigned transaction XDR: ${batch.unsignedXdr}`);
    const simulation = batch.simulation as SimulationReport | undefined;
    if (!simulation) continue;
    console.log(`Ledger: ${simulation.latestLedger}; minimum resource fee: ${simulation.minResourceFee}`);
    console.log("Footprint read-only:");
    for (const entry of simulation.footprint.readOnly) console.log(`  ${entry}`);
    console.log("Footprint read-write:");
    for (const entry of simulation.footprint.readWrite) console.log(`  ${entry}`);
    if (simulation.returnValue !== null) console.log(`Simulated return value: ${simulation.returnValue}`);
    for (const change of simulation.expectedBalances.changes) {
      console.log(`Expected token delta ${change.delta}: ${change.address} (${change.token}), from ${change.derivedFrom.join(", ")}`);
    }
  }
  if (value.transactions) console.log(JSON.stringify(value.transactions, null, 2));
}

async function main(): Promise<void> {
  let options: Options = { command: "help", values: new Map(), flags: new Set(), json: false, dryRun: false };
  try {
    options = parseArgs(process.argv.slice(2));
    const result = await resolveOptionsAndRun(options);
    printResult(result.value, options.json ? "json" : "text");
    process.exitCode = result.exitCode;
  } catch (error) {
    const failure = error instanceof CliError
      ? error
      : new CliError(errorMessage(error), "RPC", 1);
    if (options.json || process.argv.includes("--json")) {
      console.log(JSON.stringify({
        schemaVersion: 1,
        ok: false,
        error: { code: failure.code, message: failure.message },
      }));
    } else {
      console.error(`streampay: ${failure.message}`);
    }
    process.exitCode = failure.exitCode;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}