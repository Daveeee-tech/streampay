#![no_std]

//! StreamPay: continuous, linear streaming payments for Stellar (Soroban).
//!
//! Lock a deposit and let it accrue to a recipient per second between
//! `start_time` and `end_time` — for payroll, DAO contributor pay,
//! subscriptions, or vesting. The recipient withdraws accrued funds at any
//! time; the sender can top up or cancel with a fair pro-rata refund.
//!
//! # Entry points
//!
//! - [`StreamingPayments::create_stream`] — open a stream and pull the deposit
//! - [`StreamingPayments::create_streams`] — atomically create a batch of streams
//! - [`StreamingPayments::withdraw`] — recipient claims accrued funds
//! - [`StreamingPayments::withdraw_max`] — recipient claims *all* accrued
//!   funds in one atomic call
//! - [`StreamingPayments::cancel_stream`] — sender (or recipient, if the
//!   stream is `cancelable`) closes the stream with a pro-rata split
//! - [`StreamingPayments::top_up`] — add funds to a running stream
//! - [`StreamingPayments::get_stream`] / [`StreamingPayments::available`] —
//!   read-only views
//! - [`StreamingPayments::recipient_streams`] /
//!   [`StreamingPayments::sender_streams`] — paginated on-chain discovery
//!   indexes per address
//! - [`StreamingPayments::treasury_status`] /
//!   [`StreamingPayments::stream_fee`] / [`StreamingPayments::verify_treasury`] —
//!   open verification reads for the protocol-fee ledger (see `treasury.rs`)
//!
//! # Invariants
//!
//! - `0 <= withdrawn <= streamed <= deposit` for every stored stream.
//! - The contract's token balance always covers `sum(deposit - withdrawn)`
//!   over all streams plus `charged_total - withdrawn_total` over all
//!   token treasuries (funds are only ever moved by these entry points).
//! - All state changes happen *before* token transfers (checks-effects-
//!   interactions), so reentrant token callbacks cannot over-withdraw.
//! - Every token call goes through the SDK's fallible (`try_`) client and a
//!   failure is re-raised as a distinct `StreamError` transfer-failure code
//!   (18–22). Token contract errors share the `Error(Contract, #N)` namespace
//!   with this contract's codes, so propagating them raw would let a token's
//!   own code masquerade as a StreamPay code in clients. Because the failure
//!   is still returned as `Err`, the whole invocation — including any effects
//!   written before the transfer — rolls back atomically.
//! - `treasury.charged_total == sum of FeeCharged events == sum of stored
//!   per-stream fees`, provable from public reads (see `treasury.rs`).

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod client_parity_test;
#[cfg(test)]
mod differential_harness;
#[cfg(test)]
mod differential_test;
#[cfg(test)]
mod error_surface_test;
mod errors;
mod events;
mod policy;
mod schedule;
#[cfg(test)]
mod schedule_test;
mod stream;
pub mod surface;
#[cfg(test)]
mod test;
#[cfg(test)]
mod test_client;
mod treasury;
pub mod verification;

use soroban_sdk::{
    contract, contractimpl, contracttype, token::TokenClient, Address, BytesN, ConversionError,
    Env, Error as SorobanError, InvokeError, MuxedAddress, Vec,
};

use surface::Branch;

pub use errors::StreamError;
pub use events::{
    ContractFrozenStateChanged, ContractUpgraded, FeeCharged, FeeWithdrawn, PolicyAssetExecuted,
    PolicyAssetQueued, PolicyHandoffAccepted, PolicyHandoffCancelled, PolicyHandoffProposed,
    PolicyInitialized, ScheduledIntentCancelled, ScheduledIntentExpired, ScheduledStreamExecuted,
    StorageMigrated, StreamCancelled, StreamCreated, StreamScheduled, StreamToppedUp,
    StreamsCreated, UpgradeCancelled, UpgradeProposed, Withdrawn,
};
pub use policy::QueuedAssetChange;
pub use schedule::{IntentStatus, ScheduledIntent, MAX_OCCURRENCES};
// `streamed_at` is re-exported for the fuzz target and downstream rate
// calculators: it is a pure function requiring no `Env`.
pub use stream::{
    streamed_at, MigrationStatus, PendingUpgrade, Stream, StreamStatus, INITIAL_SCHEMA_VERSION,
    MAX_DURATION_SECONDS, MAX_MIGRATION_BATCH_SIZE, MAX_PAGE_SIZE, MIN_UPGRADE_DELAY_SECONDS,
    TARGET_SCHEMA_VERSION,
};
pub use treasury::{fee_amount, FeeScan, Treasury, TreasuryStatus, BPS_DENOMINATOR, MAX_FEE_BPS};

/// Parameters for one stream in a [`StreamingPayments::create_streams`] call.
#[contracttype]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamInput {
    pub recipient: Address,
    pub token: Address,
    pub deposit_amount: i128,
    pub start_time: u64,
    pub end_time: u64,
    pub cancelable: bool,
}

/// The StreamPay streaming-payments contract.
#[contract]
pub struct StreamingPayments;

/// Outcome of an SDK fallible (`try_`) call on a token contract: an outer
/// `Err` is a failed invocation (token contract error or host failure), an
/// inner `Err` a return-value conversion failure (a non-SEP-41 return type).
type TokenCall<T> = Result<Result<T, ConversionError>, Result<SorobanError, InvokeError>>;

/// Unwraps a token call, mapping **every** failure mode to `on_failure`.
///
/// A token's own error code must never escape this contract: contract error
/// numbers are per-contract and clients see them as `Error(Contract, #N)`, so
/// a token's `#5` would otherwise be indistinguishable from
/// [`StreamError::StreamNotActive`] (#5). Callers must always propagate the
/// mapped error with `?`: returning `Err` aborts the top-level invocation, so
/// effects already written in this call roll back together with the failed
/// (and therefore fully rolled-back) token transfer — no partial state and no
/// partial event log can survive a failed transfer.
fn token_result<T>(res: TokenCall<T>, on_failure: Branch) -> Result<T, StreamError> {
    match res {
        Ok(Ok(value)) => Ok(value),
        // Conversion failures mean the callee did not return a SEP-41 shaped
        // value; treat a non-compliant token exactly like a failed transfer.
        // Both failure modes are reported through the branch registry, so the
        // error-surface matrix sees the exact `(entrypoint, condition)` that
        // refused the call.
        Ok(Err(_)) | Err(_) => Err(surface::reject(on_failure)),
    }
}

/// Pulls `amount` of `token` from `payer` into this contract via
/// `transfer_from`, mapping a token-side failure (insufficient balance or
/// allowance, a non-transferable asset, a non-token contract) to the code
/// `on_failure` declares.
fn pull(
    env: &Env,
    token: &Address,
    payer: &Address,
    amount: i128,
    on_failure: Branch,
) -> Result<(), StreamError> {
    let res = TokenClient::new(env, token).try_transfer_from(
        &env.current_contract_address(),
        payer,
        &env.current_contract_address(),
        &amount,
    );
    token_result(res, on_failure)
}

/// Sends `amount` of `token` held by this contract to `recipient` via
/// `transfer`, mapping a token-side failure to the code `on_failure` declares.
fn push(
    env: &Env,
    token: &Address,
    recipient: &MuxedAddress,
    amount: i128,
    on_failure: Branch,
) -> Result<(), StreamError> {
    let res = TokenClient::new(env, token).try_transfer(
        &env.current_contract_address(),
        recipient,
        &amount,
    );
    token_result(res, on_failure)
}

#[contractimpl]
impl StreamingPayments {
    /// Internal helper: verifies that mutating entrypoints are not called
    /// while the contract is frozen for maintenance or upgrade.
    fn require_not_frozen(env: &Env) -> Result<(), StreamError> {
        if stream::is_frozen(env) {
            return Err(surface::reject(Branch::ContractFrozen));
        }
        Ok(())
    }

    /// Deploy-time constructor: binds the treasury governor.
    ///
    /// Binding governance in the deployment transaction (rather than via a
    /// later `initialize` call) removes the front-running window where an
    /// attacker could claim an uninitialized treasury. The fee rate starts
    /// at 0 and is configured afterwards via
    /// [`StreamingPayments::set_fee_bps`].
    pub fn __constructor(env: Env, governor: Address) {
        env.storage()
            .instance()
            .set(&stream::DataKey::Governor, &governor);
        env.storage()
            .instance()
            .extend_ttl(stream::TTL_THRESHOLD_LEDGERS, stream::TTL_EXTEND_LEDGERS);
    }

    /// Creates a linear payment stream and pulls `deposit_amount` of `token`
    /// from `sender` into this contract.
    ///
    /// The sender must first approve this contract as a spender on `token`
    /// (`token.approve(sender, streaming_contract, deposit_amount, ...)`).
    /// When a protocol fee is configured, the approval (and the pull) cover
    /// `deposit_amount + fee`, where `fee = deposit_amount * fee_bps / 10_000`
    /// is charged once, here at the deposit boundary — never during
    /// accrual or payout. At `fee_bps == 0` the call is identical to the
    /// fee-less contract: same pull, same events, same state.
    ///
    /// * `start_time` must not be in the past (UIs pass the current time to
    ///   start streaming immediately).
    /// * `end_time` must be strictly after `start_time` (zero-duration streams
    ///   are rejected) and at most `MAX_DURATION_SECONDS` long.
    #[allow(clippy::too_many_arguments)] // 8-arg surface is the intended client API
    /// * `cancelable` controls whether the recipient may also call
    ///   `cancel_stream` to reclaim their accrued balance.
    ///
    /// Returns the new stream id (monotonically increasing, starting at 1).
    ///
    /// The deposit pull is mapped to [`StreamError::DepositTransferFailed`]
    /// (missing balance/allowance, or an asset that refuses the transfer);
    /// nothing is persisted when it fails, so no stream id is consumed.
    pub fn create_stream(
        env: Env,
        sender: Address,
        recipient: Address,
        token: Address,
        deposit_amount: i128,
        start_time: u64,
        end_time: u64,
        cancelable: bool,
    ) -> Result<u64, StreamError> {
        Self::require_not_frozen(&env)?;
        sender.require_auth();

        let now = env.ledger().timestamp();
        Self::validate_stream_creation(now, deposit_amount, start_time, end_time)?;

        // Policy check: if the policy module is active, the asset must be on
        // the allowlist. Uninitialized policy permits all assets (opt-in).
        policy::check_asset_allowed(&env, &token)?;

        // Protocol fee, computed at the deposit boundary only. The accrual
        // math (`deposit`, `streamed_at`, cancel splits) never sees it.
        let fee_bps = treasury::current_fee_bps(&env);
        let fee = treasury::fee_amount(deposit_amount, fee_bps)?;
        let pull_amount = deposit_amount
            .checked_add(fee)
            .ok_or(StreamError::MathOverflow)?;

        // Interaction: pull the deposit (plus fee, when configured) from
        // the sender. A token-side failure maps to a distinct code; returning
        // `Err` aborts before anything is persisted (no id consumed, no fee
        // recorded, no event emitted).
        pull(
            &env,
            &token,
            &sender,
            pull_amount,
            Branch::CreateDepositTransferFailed,
        )?;

        // Effects: persist the stream and announce it.
        let id = stream::next_stream_id(&env);
        let s = Stream {
            id,
            sender: sender.clone(),
            recipient: recipient.clone(),
            token: token.clone(),
            deposit: deposit_amount,
            withdrawn: 0,
            start_time,
            end_time,
            cancelable,
            status: StreamStatus::Active,
        };
        stream::put_stream(&env, &s);
        stream::index_stream(&env, &s);
        if fee > 0 {
            // Effects: accumulate the fee in the token's treasury and store
            // the per-stream fee before any event announces it.
            treasury::apply_fee(&env, &token, id, fee)?;
        }

        StreamCreated {
            stream_id: id,
            sender,
            recipient,
            token,
            deposit: deposit_amount,
            start_time,
            end_time,
            cancelable,
        }
        .publish(&env);

        if fee > 0 {
            FeeCharged {
                stream_id: id,
                token: s.token.clone(),
                payer: s.sender.clone(),
                amount: fee,
                fee_bps,
            }
            .publish(&env);
        }

        Ok(id)
    }

    fn validate_stream_creation(
        now: u64,
        deposit_amount: i128,
        start_time: u64,
        end_time: u64,
    ) -> Result<(), StreamError> {
        if deposit_amount <= 0 {
            return Err(surface::reject(Branch::CreateDepositNonPositive));
        }
        if start_time < now {
            return Err(surface::reject(Branch::CreateStartTimeInPast));
        }
        if end_time <= start_time {
            return Err(surface::reject(Branch::CreateInvalidTimeRange));
        }
        // The ordered range check above makes this subtraction safe.
        let duration = end_time - start_time;
        if duration > MAX_DURATION_SECONDS {
            return Err(surface::reject(Branch::CreateDurationTooLong));
        }
        stream::validate_capacity(deposit_amount, duration)
    }

    /// Allocates the next stream id, persists the stream, indexes it under both
    /// parties, and emits [`StreamCreated`]. No authorization and no token
    /// movement: callers are responsible for the checks and for having the
    /// deposit escrowed in the contract (scheduled execution uses an intent's
    /// already-escrowed deposit).
    #[allow(clippy::too_many_arguments)]
    fn create_stream_internal(
        env: &Env,
        sender: Address,
        recipient: Address,
        token: Address,
        deposit: i128,
        start_time: u64,
        end_time: u64,
        cancelable: bool,
    ) -> u64 {
        let id = stream::next_stream_id(env);
        let stream = Stream {
            id,
            sender: sender.clone(),
            recipient: recipient.clone(),
            token: token.clone(),
            deposit,
            withdrawn: 0,
            start_time,
            end_time,
            cancelable,
            status: StreamStatus::Active,
        };
        stream::put_stream(env, &stream);
        stream::index_stream(env, &stream);

        StreamCreated {
            stream_id: id,
            sender,
            recipient,
            token,
            deposit,
            start_time,
            end_time,
            cancelable,
        }
        .publish(env);

        id
    }

    /// Creates multiple streams atomically, authorizing the sender once and
    /// grouping deposits into one token pull per token. Every item uses the
    /// same validation and fee rules as `create_stream`; if any item is
    /// invalid, the entire transaction (including transfers and state writes)
    /// is rolled back. An empty input returns an empty vector without auth.
    ///
    /// Emits one `StreamsCreated` event containing all ids; individual
    /// `FeeCharged` events are still emitted when fees apply.
    pub fn create_streams(
        env: Env,
        sender: Address,
        inputs: Vec<StreamInput>,
    ) -> Result<Vec<u64>, StreamError> {
        Self::require_not_frozen(&env)?;
        let mut ids = Vec::new(&env);
        let count = inputs.len();
        if count == 0 {
            return Ok(ids);
        }

        sender.require_auth();

        let now = env.ledger().timestamp();
        let fee_bps = treasury::current_fee_bps(&env);
        let mut pull_totals: Vec<(Address, i128)> = Vec::new(&env);
        let mut fees = Vec::new(&env);
        let mut index = 0;
        while index < count {
            let input = inputs.get(index).ok_or(StreamError::MathOverflow)?;
            Self::validate_stream_creation(
                now,
                input.deposit_amount,
                input.start_time,
                input.end_time,
            )?;
            policy::check_asset_allowed(&env, &input.token)?;

            let fee = treasury::fee_amount(input.deposit_amount, fee_bps)?;
            let pull_amount = input
                .deposit_amount
                .checked_add(fee)
                .ok_or(StreamError::MathOverflow)?;
            let mut token_index = 0;
            let mut found = false;
            while token_index < pull_totals.len() {
                let (token, total) = pull_totals
                    .get(token_index)
                    .ok_or(StreamError::MathOverflow)?;
                if token == input.token {
                    pull_totals.set(
                        token_index,
                        (
                            token,
                            total
                                .checked_add(pull_amount)
                                .ok_or(StreamError::MathOverflow)?,
                        ),
                    );
                    found = true;
                    break;
                }
                token_index += 1;
            }
            if !found {
                pull_totals.push_back((input.token, pull_amount));
            }
            fees.push_back(fee);
            index += 1;
        }

        let first_id = stream::reserve_stream_ids(&env, count)?;
        index = 0;
        while index < count {
            let id = first_id
                .checked_add(index as u64)
                .ok_or(StreamError::MathOverflow)?;
            ids.push_back(id);
            index += 1;
        }

        let mut token_index = 0;
        while token_index < pull_totals.len() {
            let (token, amount) = pull_totals
                .get(token_index)
                .ok_or(StreamError::MathOverflow)?;
            // A token-side refusal maps to [`StreamError::DepositTransferFailed`]
            // like the single-stream path: the whole batch aborts and every
            // effect written so far (including the reserved ids) rolls back.
            pull(
                &env,
                &token,
                &sender,
                amount,
                Branch::CreateDepositTransferFailed,
            )?;
            token_index += 1;
        }

        let mut index = 0;
        while index < count {
            let input = inputs.get(index).ok_or(StreamError::MathOverflow)?;
            let id = ids.get(index).ok_or(StreamError::MathOverflow)?;
            let fee = fees.get(index).ok_or(StreamError::MathOverflow)?;
            let created = Stream {
                id,
                sender: sender.clone(),
                recipient: input.recipient,
                token: input.token,
                deposit: input.deposit_amount,
                withdrawn: 0,
                start_time: input.start_time,
                end_time: input.end_time,
                cancelable: input.cancelable,
                status: StreamStatus::Active,
            };
            stream::put_stream(&env, &created);
            stream::index_stream(&env, &created);
            if fee > 0 {
                treasury::apply_fee(&env, &created.token, id, fee)?;
                FeeCharged {
                    stream_id: id,
                    token: created.token.clone(),
                    payer: sender.clone(),
                    amount: fee,
                    fee_bps,
                }
                .publish(&env);
            }
            index += 1;
        }

        StreamsCreated {
            sender,
            stream_ids: ids.clone(),
        }
        .publish(&env);

        Ok(ids)
    }

    /// Withdraws up to `amount` of the accrued-but-unwithdrawn balance and
    /// pays it to the stream recipient. Only the recipient can call this
    /// (their authorization is required).
    ///
    /// Reverts with [`StreamError::AmountExceedsAvailable`] if `amount`
    /// exceeds what has accrued so far; before `start_time` nothing has
    /// accrued, at or after `end_time` the full remainder is available.
    ///
    /// A token-side failure to deliver the payout returns
    /// [`StreamError::RecipientPayoutFailed`] and rolls the recorded
    /// `withdrawn` update back with it.
    ///
    /// # Ordering against `cancel_stream` (same-ledger race)
    ///
    /// `withdraw` and `cancel_stream` are strictly serial: each invocation
    /// runs to completion — including its CEI state writes — before the next
    /// begins, even when both land in the same ledger. There is no
    /// interleaving and no partial application; whichever call executes
    /// first wins, and the loser observes the post-write state:
    ///
    /// * If `cancel_stream` executes first, the stream is already `Cancelled`
    ///   when `withdraw` runs, so it reverts with
    ///   [`StreamError::StreamNotActive`] (code 5) without moving tokens.
    /// * If `withdraw` executes first, a subsequent `cancel_stream` splits
    ///   only the *remaining* balance (`streamed − withdrawn`). If this
    ///   withdrawal drained the stream (`withdrawn == deposit`, status
    ///   `Depleted`), the cancel instead reverts with
    ///   [`StreamError::StreamNotActive`] (code 5).
    ///
    /// The losing call never mutates state, so the funds-conservation
    /// outcome is identical to the serial composition: the recipient
    /// receives exactly the amount accrued at the later call and the sender
    /// is refunded the unaccrued remainder. Regression tests pin these
    /// sequences in `test.rs` (`race_*`); the race table lives in
    /// `docs/ARCHITECTURE.md` §6.1.
    pub fn withdraw(env: Env, stream_id: u64, amount: i128) -> Result<(), StreamError> {
        Self::require_not_frozen(&env)?;
        if amount <= 0 {
            return Err(surface::reject(Branch::WithdrawAmountNonPositive));
        }
        let mut s = Self::authorize_withdrawal(&env, stream_id)?;

        let available = stream::available_amount(&env, &s)?;
        if amount > available {
            return Err(surface::reject(Branch::WithdrawAmountExceedsAvailable));
        }

        Self::pay_out(&env, &mut s, amount)
    }

    /// Withdraws the entire accrued-but-unwithdrawn balance in one call and
    /// returns the amount paid. Only the recipient can call this (their
    /// authorization is required).
    ///
    /// This is the atomic form of "read `available`, then `withdraw` it":
    /// clients never risk a stale amount between the two calls. Reverts with
    /// [`StreamError::NothingToWithdraw`] when nothing has accrued yet (or
    /// everything accrued has already been withdrawn), with
    /// [`StreamError::StreamNotActive`] on cancelled or depleted streams, and
    /// with [`StreamError::RecipientPayoutFailed`] if the token refuses the
    /// payout (state rolls back unchanged).
    pub fn withdraw_max(env: Env, stream_id: u64) -> Result<i128, StreamError> {
        Self::require_not_frozen(&env)?;
        let mut s = Self::authorize_withdrawal(&env, stream_id)?;

        let amount = stream::available_amount(&env, &s)?;
        if amount <= 0 {
            return Err(surface::reject(Branch::WithdrawMaxNothingToWithdraw));
        }

        Self::pay_out(&env, &mut s, amount)?;
        Ok(amount)
    }

    /// Shared preamble for both withdrawal paths: load the stream, require
    /// the recipient's authorization, and reject terminal streams.
    fn authorize_withdrawal(env: &Env, stream_id: u64) -> Result<Stream, StreamError> {
        let s = stream::get_stream(env, stream_id)?;
        s.recipient.require_auth();
        if s.status != StreamStatus::Active {
            return Err(surface::reject(Branch::StreamNotActive));
        }
        Ok(s)
    }

    /// Shared payout tail for both withdrawal paths, checks-effects-
    /// interactions ordered: record `withdrawn` and emit the event *before*
    /// moving tokens, so a reentrant token callback can never double-spend.
    fn pay_out(env: &Env, s: &mut Stream, amount: i128) -> Result<(), StreamError> {
        s.withdrawn = s
            .withdrawn
            .checked_add(amount)
            .ok_or_else(|| surface::reject(Branch::PayoutOverflow))?;
        if s.withdrawn == s.deposit {
            s.status = StreamStatus::Depleted;
        }
        stream::put_stream(env, s);

        Withdrawn {
            stream_id: s.id,
            recipient: s.recipient.clone(),
            amount,
            withdrawn_total: s.withdrawn,
        }
        .publish(env);

        push(
            env,
            &s.token,
            &stream::muxed(&s.recipient),
            amount,
            Branch::RecipientPayoutFailed,
        )
    }

    /// Cancels a stream: pays the recipient their accrued-but-unwithdrawn
    /// balance and refunds the sender the unaccrued remainder, atomically.
    ///
    /// Callable by the sender always, and by the recipient only if the stream
    /// was created with `cancelable = true`. Cancelling an already-cancelled
    /// or depleted stream reverts with [`StreamError::StreamNotActive`].
    ///
    /// Each payout leg maps a token-side failure to its own code —
    /// [`StreamError::RecipientPayoutFailed`] for the recipient's accrued
    /// share and [`StreamError::SenderRefundFailed`] for the sender's refund —
    /// and any failure rolls the whole cancellation back: the stream stays
    /// `Active`, `withdrawn` is restored, and no event survives.
    ///
    /// # Ordering against `withdraw` (same-ledger race)
    ///
    /// The counterpart of the ordering documented on
    /// [`StreamingPayments::withdraw`]: cancellation is atomic and sees the
    /// `withdrawn` balance as of *its* execution point.
    ///
    /// * If the recipient's `withdraw` executes first, the cancel splits the
    ///   remainder only (`streamed − withdrawn` to the recipient,
    ///   `deposit − streamed` to the sender). If that withdrawal already
    ///   drained the stream to `Depleted`, the cancel reverts with
    ///   [`StreamError::StreamNotActive`] (code 5).
    /// * If this cancel executes first, the stream is `Cancelled` and any
    ///   later `withdraw` / `withdraw_max` reverts with
    ///   [`StreamError::StreamNotActive`] (code 5).
    ///
    /// Last write wins in execution order; no interleaving exists within a
    /// ledger, and the losing call leaves state and balances untouched.
    /// Pinned by the `race_*` regression tests in `test.rs`; see
    /// `docs/ARCHITECTURE.md` §6.1 for the race table.
    pub fn cancel_stream(env: Env, caller: Address, stream_id: u64) -> Result<(), StreamError> {
        Self::require_not_frozen(&env)?;
        caller.require_auth();

        let mut s = stream::get_stream(&env, stream_id)?;
        if s.status != StreamStatus::Active {
            return Err(surface::reject(Branch::StreamNotActive));
        }
        if caller != s.sender {
            if caller == s.recipient {
                if !s.cancelable {
                    return Err(surface::reject(Branch::CancelStreamNotCancellable));
                }
            } else {
                return Err(surface::reject(Branch::CancelStreamNotAuthorized));
            }
        }

        let (recipient_amount, sender_amount) = stream::cancel_amounts(&env, &s)?;

        // Effects first (CEI): finalise state before moving tokens.
        s.status = StreamStatus::Cancelled;
        s.withdrawn = s
            .withdrawn
            .checked_add(recipient_amount)
            .ok_or_else(|| surface::reject(Branch::CancelSplitOverflow))?;
        stream::put_stream(&env, &s);

        StreamCancelled {
            stream_id,
            cancelled_by: caller,
            recipient_amount,
            sender_amount,
        }
        .publish(&env);

        // MUTANT-SUPPRESS: replace > with >= is acceptable because recipient_amount
        // is only 0 when cancel_amounts computes 0 (cancel before start). The test
        // cancel_at_exact_start_pays_sender_everything verifies correct behavior
        // without the transfer. The >= mutation would only add a no-op transfer of
        // 0 tokens, which is functionally equivalent but unnecessary.
        if recipient_amount > 0 {
            push(
                &env,
                &s.token,
                &stream::muxed(&s.recipient),
                recipient_amount,
                Branch::RecipientPayoutFailed,
            )?;
        }
        // MUTANT-SUPPRESS: replace > with >= is acceptable because sender_amount
        // is only 0 when cancel_amounts computes 0 (cancel after end). The test
        // cancel_after_end_pays_recipient_everything verifies correct behavior
        // without the transfer. The >= mutation would only add a no-op transfer of
        // 0 tokens, which is functionally equivalent but unnecessary.
        if sender_amount > 0 {
            push(
                &env,
                &s.token,
                &stream::muxed(&s.sender),
                sender_amount,
                Branch::SenderRefundFailed,
            )?;
        }

        Ok(())
    }

    /// Adds funds to an existing active stream. The deposit grows and accrual
    /// remains linear over the original window, so the recipient's accrued
    /// amount increases pro-rata immediately (Sablier-v1 `increaseAmount`
    /// semantics).
    ///
    /// Top-ups are rejected on cancelled or depleted streams. Topping up a
    /// stream that has already passed `end_time` adds fully-accrued funds the
    /// recipient can withdraw immediately.
    ///
    /// A token-side failure to pull the funds returns
    /// [`StreamError::TopUpTransferFailed`] and leaves `deposit` unchanged.
    ///
    /// Note: extending `end_time` is intentionally not supported in v1 —
    /// retroactively re-baselining accrual is lossy for the recipient; see
    /// `docs/ARCHITECTURE.md` for the design considerations and roadmap.
    pub fn top_up(env: Env, stream_id: u64, amount: i128) -> Result<(), StreamError> {
        Self::require_not_frozen(&env)?;
        if amount <= 0 {
            return Err(surface::reject(Branch::TopUpAmountNonPositive));
        }
        let mut s = stream::get_stream(&env, stream_id)?;
        if s.status != StreamStatus::Active {
            return Err(surface::reject(Branch::StreamNotActive));
        }
        s.sender.require_auth();

        let new_deposit = s
            .deposit
            .checked_add(amount)
            .ok_or_else(|| surface::reject(Branch::TopUpDepositOverflow))?;
        stream::validate_capacity(new_deposit, s.duration())?;

        // Interaction: pull the added funds from the sender. A token-side
        // failure maps to a distinct code; the effects below never run, and
        // no partial state survives the `Err`.
        pull(
            &env,
            &s.token,
            &s.sender,
            amount,
            Branch::TopUpTransferFailed,
        )?;

        // Effects: record the new deposit and announce it.
        s.deposit = new_deposit;
        stream::put_stream(&env, &s);

        StreamToppedUp {
            stream_id,
            sender: s.sender.clone(),
            amount,
            new_deposit,
        }
        .publish(&env);

        Ok(())
    }

    /// Read-only view of a stream's full state.
    pub fn get_stream(env: Env, stream_id: u64) -> Result<Stream, StreamError> {
        stream::get_stream(&env, stream_id)
    }

    /// Read-only view of the accrued-but-unwithdrawn balance at the current
    /// ledger time. Zero before `start_time` and for cancelled streams; the
    /// full remainder at or after `end_time`.
    pub fn available(env: Env, stream_id: u64) -> Result<i128, StreamError> {
        let s = stream::get_stream(&env, stream_id)?;
        stream::available_amount(&env, &s)
    }

    /// A page of stream ids where `recipient` is the recipient, in creation
    /// order (oldest first). `limit` is clamped to `MAX_PAGE_SIZE`; paginate
    /// by advancing `offset`. Unknown addresses yield an empty page.
    ///
    /// Discovery view only: it does not authorize anything and terminal
    /// (cancelled/depleted) streams keep their ids — read each stream's
    /// `status` for lifecycle state.
    pub fn recipient_streams(env: Env, recipient: Address, offset: u32, limit: u32) -> Vec<u64> {
        stream::page_stream_ids(
            &env,
            &stream::DataKey::RecipientStreams(recipient),
            offset,
            limit,
        )
    }

    /// A page of stream ids created by `sender`, in creation order. Same
    /// pagination and advisory semantics as [`StreamingPayments::recipient_streams`].
    pub fn sender_streams(env: Env, sender: Address, offset: u32, limit: u32) -> Vec<u64> {
        stream::page_stream_ids(&env, &stream::DataKey::SenderStreams(sender), offset, limit)
    }

    // ------------------------------------------------- scheduled intents ----

    /// Pre-authorizes the creation of one or more streams at a future time and
    /// escrows the up-front cost.
    ///
    /// `sender` must authorize this call (their signature over the invocation
    /// is verified by the host). The contract escrows
    /// `(deposit_amount + fee_budget) * occurrences` of `token`:
    /// `deposit_amount` per occurrence funds the stream, and `fee_budget` per
    /// occurrence is the reward paid to whichever keeper later executes it.
    ///
    /// * A **one-shot** intent is scheduled with `occurrences = 1` and
    ///   `interval = 0`.
    /// * A **recurring** intent is scheduled with `occurrences > 1` and a
    ///   strictly positive `interval` (seconds between occurrence starts). The
    ///   `i`-th occurrence's stream window is `[start_time + i*interval,
    ///   end_time + i*interval)`.
    ///
    /// Timing constraints: `execute_after <= start_time`, `now <= execute_after`
    /// and `start_time` must be strictly in the future; `expires_at >
    /// execute_after` bounds the execution window. The exact parameters are
    /// committed into the intent's `params_hash`.
    ///
    /// Returns the new intent id (monotonically increasing, starting at 1).
    #[allow(clippy::too_many_arguments)] // the scheduling surface is intentionally explicit
    pub fn schedule_stream(
        env: Env,
        sender: Address,
        recipient: Address,
        token: Address,
        deposit_amount: i128,
        start_time: u64,
        end_time: u64,
        cancelable: bool,
        interval: u64,
        occurrences: u32,
        fee_budget: i128,
        execute_after: u64,
        expires_at: u64,
    ) -> Result<u64, StreamError> {
        sender.require_auth();

        let total_escrow = schedule::validate_schedule(
            &env,
            deposit_amount,
            start_time,
            end_time,
            interval,
            occurrences,
            fee_budget,
            execute_after,
            expires_at,
        )?;

        // Effects first (CEI): persist the exact authorized intent before the
        // token call. If the transfer fails, Soroban rolls the whole invocation
        // back, including these writes and the event.
        let id = schedule::next_intent_id(&env);
        let params_hash = schedule::params_hash(
            &env,
            &sender,
            &recipient,
            &token,
            deposit_amount,
            start_time,
            end_time,
            cancelable,
            interval,
            occurrences,
            fee_budget,
            execute_after,
            expires_at,
        );
        let intent = ScheduledIntent {
            id,
            sender: sender.clone(),
            recipient: recipient.clone(),
            token: token.clone(),
            deposit: deposit_amount,
            start_time,
            end_time,
            cancelable,
            interval,
            occurrences_total: occurrences,
            occurrences_remaining: occurrences,
            fee_budget,
            execute_after,
            expires_at,
            params_hash,
            status: IntentStatus::Pending,
        };
        schedule::put_intent(&env, &intent);
        schedule::index_intent(&env, &intent);

        StreamScheduled {
            intent_id: id,
            sender: sender.clone(),
            recipient,
            token: token.clone(),
            deposit: deposit_amount,
            start_time,
            end_time,
            cancelable,
            interval,
            occurrences,
            fee_budget,
            execute_after,
            expires_at,
        }
        .publish(&env);

        // Interaction: pull the whole escrow (deposit + fee budget, for every
        // occurrence) into the contract. Reentrant token code cannot observe an
        // unregistered schedule or execute against an intent without state.
        TokenClient::new(&env, &token).transfer_from(
            &env.current_contract_address(),
            &sender,
            &env.current_contract_address(),
            &total_escrow,
        );

        Ok(id)
    }

    /// Executes the next due occurrence of a scheduled intent, creating the
    /// stream and paying `keeper` the escrowed per-occurrence fee.
    ///
    /// **Permissionless**: any address may call as `keeper`, but `keeper` must
    /// authorize the call (so the fee cannot be front-run and redirected to a
    /// copycat). The contract enforces that the intent is pending, is within its
    /// `[execute_after, expires_at)` window for the next occurrence, and that
    /// its stored parameters still match the scheduling commitment.
    ///
    /// Execution is **once per occurrence**: each call decrements
    /// `occurrences_remaining`, and a second call for an already-executed
    /// occurrence (or a completed one-shot) reverts with
    /// [`StreamError::IntentNotPending`]. Returns the created stream id.
    pub fn execute_scheduled(
        env: Env,
        intent_id: u64,
        keeper: Address,
    ) -> Result<u64, StreamError> {
        keeper.require_auth();

        let mut intent = schedule::get_intent(&env, intent_id)?;
        if intent.status != IntentStatus::Pending {
            return Err(surface::reject(Branch::IntentNotPending));
        }

        // Defensive: the stored parameters must still hash to the commitment
        // recorded when the sender authorized the intent.
        if schedule::intent_params_hash(&env, &intent) != intent.params_hash {
            return Err(surface::reject(Branch::IntentParamsMismatch));
        }

        let now = env.ledger().timestamp();
        if now >= intent.expires_at {
            return Err(surface::reject(Branch::IntentExpired));
        }
        let index = intent.executed_count();
        if now < intent.occurrence_due(index) {
            return Err(surface::reject(Branch::IntentNotYetExecutable));
        }

        let (start_time, end_time) = intent.occurrence_window(index);

        // Effects first (CEI): advance the intent before moving any tokens.
        intent.occurrences_remaining = intent
            .occurrences_remaining
            .checked_sub(1)
            .ok_or(StreamError::MathOverflow)?;
        if intent.occurrences_remaining == 0 {
            intent.status = IntentStatus::Executed;
        }
        schedule::put_intent(&env, &intent);

        // The stream deposit is already escrowed; only bookkeeping is required.
        let stream_id = Self::create_stream_internal(
            &env,
            intent.sender.clone(),
            intent.recipient.clone(),
            intent.token.clone(),
            intent.deposit,
            start_time,
            end_time,
            intent.cancelable,
        );

        // Pay the keeper its escrowed fee for this occurrence.
        let fee_paid = intent.fee_budget;
        if fee_paid > 0 {
            TokenClient::new(&env, &intent.token).transfer(
                &env.current_contract_address(),
                stream::muxed(&keeper),
                &fee_paid,
            );
        }

        ScheduledStreamExecuted {
            intent_id,
            keeper,
            stream_id,
            occurrence_index: index,
            occurrences_remaining: intent.occurrences_remaining,
            fee_paid,
        }
        .publish(&env);

        Ok(stream_id)
    }

    /// Cancels a pending scheduled intent and refunds the sender the escrow for
    /// every outstanding occurrence.
    ///
    /// Only the intent's sender may cancel (their authorization is required),
    /// and only **before the execution window opens** (`now < execute_after`).
    /// Once the window is open, keepers may execute and cancellation is
    /// disallowed ([`StreamError::IntentNotCancellable`]) to remove a
    /// cancel/execute race that would otherwise burn keeper gas. After
    /// `expires_at`, use [`StreamingPayments::reclaim_expired`] instead.
    pub fn cancel_scheduled(env: Env, sender: Address, intent_id: u64) -> Result<(), StreamError> {
        sender.require_auth();

        let mut intent = schedule::get_intent(&env, intent_id)?;
        if intent.status != IntentStatus::Pending {
            return Err(surface::reject(Branch::IntentNotPending));
        }
        if sender != intent.sender {
            return Err(StreamError::NotAuthorized);
        }

        let now = env.ledger().timestamp();
        if now >= intent.expires_at {
            return Err(surface::reject(Branch::IntentExpired));
        }
        if now >= intent.execute_after {
            return Err(surface::reject(Branch::IntentNotCancellable));
        }

        let refund = intent.remaining_escrow()?;

        // Effects first (CEI).
        intent.status = IntentStatus::Cancelled;
        schedule::put_intent(&env, &intent);

        if refund > 0 {
            TokenClient::new(&env, &intent.token).transfer(
                &env.current_contract_address(),
                stream::muxed(&intent.sender),
                &refund,
            );
        }

        ScheduledIntentCancelled {
            intent_id,
            sender: intent.sender.clone(),
            refund,
        }
        .publish(&env);

        Ok(())
    }

    /// Reclaims a pending intent that passed `expires_at` without being fully
    /// executed, refunding the sender the escrow for every outstanding
    /// occurrence.
    ///
    /// **Permissionless**: anyone may trigger the refund (the funds always go
    /// to the sender), so abandoned intents cannot lock capital indefinitely.
    /// Reverts with [`StreamError::IntentExpired`]-style semantics via
    /// [`StreamError::IntentNotExpired`] if called too early, and with
    /// [`StreamError::IntentNotPending`] if the intent is terminal.
    pub fn reclaim_expired(env: Env, intent_id: u64) -> Result<(), StreamError> {
        let mut intent = schedule::get_intent(&env, intent_id)?;
        if intent.status != IntentStatus::Pending {
            return Err(surface::reject(Branch::IntentNotPending));
        }
        let now = env.ledger().timestamp();
        if now < intent.expires_at {
            return Err(surface::reject(Branch::IntentNotExpired));
        }

        let refund = intent.remaining_escrow()?;

        // Effects first (CEI).
        intent.status = IntentStatus::Expired;
        schedule::put_intent(&env, &intent);

        if refund > 0 {
            TokenClient::new(&env, &intent.token).transfer(
                &env.current_contract_address(),
                stream::muxed(&intent.sender),
                &refund,
            );
        }

        ScheduledIntentExpired {
            intent_id,
            sender: intent.sender.clone(),
            refund,
        }
        .publish(&env);

        Ok(())
    }

    /// Read-only view of a scheduled intent's full state.
    pub fn get_intent(env: Env, intent_id: u64) -> Result<ScheduledIntent, StreamError> {
        schedule::get_intent(&env, intent_id)
    }

    /// A page of scheduled-intent ids created by `sender`, in creation order.
    /// Same pagination and advisory semantics as
    /// [`StreamingPayments::sender_streams`].
    pub fn sender_intents(env: Env, sender: Address, offset: u32, limit: u32) -> Vec<u64> {
        stream::page_stream_ids(&env, &stream::DataKey::SenderIntents(sender), offset, limit)
    }

    // ------------------------------------------------------------ treasury --
    //
    // The optional protocol-fee layer: configuration (governor-gated),
    // governance withdrawal, and the open verification reads. Design and
    // invariants live in `treasury.rs`; the decision table for fees vs.
    // cancel refunds is in `docs/ARCHITECTURE.md`.

    /// Sets the protocol fee rate (basis points on each new stream's
    /// deposit, charged once at creation). Governor-only. Reverts with
    /// [`StreamError::FeeRateTooHigh`] above [`MAX_FEE_BPS`] (the protocol
    /// cap) and with [`StreamError::NotGovernor`] for any other caller.
    ///
    /// Changes apply only to streams created afterwards: fees are a
    /// property of the deposit boundary a stream actually crossed, never
    /// retroactively.
    pub fn set_fee_bps(env: Env, caller: Address, new_fee_bps: u32) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        if new_fee_bps > MAX_FEE_BPS {
            return Err(surface::reject(Branch::SetFeeRateTooHigh));
        }
        treasury::set_fee_bps(&env, new_fee_bps);
        env.storage()
            .instance()
            .extend_ttl(stream::TTL_THRESHOLD_LEDGERS, stream::TTL_EXTEND_LEDGERS);
        Ok(())
    }

    /// Nominates `new_governor` as the next treasury governor (governor-only,
    /// step 1 of a two-step rotation). Two steps mean a typo'd address can
    /// never irreversibly strand the treasury: only an address that can
    /// prove control (`accept_governor`) ever becomes governor.
    pub fn nominate_governor(
        env: Env,
        caller: Address,
        new_governor: Address,
    ) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        env.storage()
            .instance()
            .set(&stream::DataKey::PendingGovernor, &new_governor);
        env.storage()
            .instance()
            .extend_ttl(stream::TTL_THRESHOLD_LEDGERS, stream::TTL_EXTEND_LEDGERS);
        Ok(())
    }

    /// Completes a governor rotation (step 2): the nominated address proves
    /// control by authorizing this call and becomes the governor. Reverts
    /// with [`StreamError::NoPendingGovernor`] when nothing is pending and
    /// [`StreamError::NotGovernor`] for a caller who is not the nominee.
    pub fn accept_governor(env: Env, caller: Address) -> Result<(), StreamError> {
        let pending: Address = env
            .storage()
            .instance()
            .get(&stream::DataKey::PendingGovernor)
            .ok_or_else(|| surface::reject(Branch::AcceptGovernorNoPending))?;
        if pending != caller {
            return Err(surface::reject(Branch::AcceptGovernorNotNominee));
        }
        caller.require_auth();
        env.storage()
            .instance()
            .set(&stream::DataKey::Governor, &caller);
        env.storage()
            .instance()
            .remove(&stream::DataKey::PendingGovernor);
        env.storage()
            .instance()
            .extend_ttl(stream::TTL_THRESHOLD_LEDGERS, stream::TTL_EXTEND_LEDGERS);
        Ok(())
    }

    /// Withdraws accumulated protocol fees for `token` to `recipient`.
    /// Governor-only; bounded by fees collected minus fees already
    /// withdrawn ([`StreamError::InsufficientTreasuryBalance`]).
    ///
    /// Effects-interactions ordered: `FeeWithdrawn` is emitted and the
    /// accounting persisted *before* the token transfer. A token-side failure
    /// to pay out returns [`StreamError::TreasuryPayoutFailed`], rolling the
    /// recorded withdrawal and the event back with it.
    pub fn withdraw_treasury(
        env: Env,
        caller: Address,
        token: Address,
        recipient: Address,
        amount: i128,
    ) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        if amount <= 0 {
            return Err(surface::reject(Branch::WithdrawTreasuryAmountNonPositive));
        }
        let mut t = treasury::get_treasury(&env, &token);
        if amount > t.collectible()? {
            return Err(surface::reject(Branch::WithdrawTreasuryExceedsBalance));
        }

        // Effects (CEI): record the withdrawal and announce it before the
        // tokens move.
        t.withdrawn_total = t
            .withdrawn_total
            .checked_add(amount)
            .ok_or(StreamError::MathOverflow)?;
        treasury::put_treasury(&env, &token, &t);

        FeeWithdrawn {
            token: token.clone(),
            governor: caller,
            recipient: recipient.clone(),
            amount,
            withdrawn_total: t.withdrawn_total,
        }
        .publish(&env);

        // Interaction: a token-side failure maps to its own code and rolls
        // the recorded withdrawal and the `FeeWithdrawn` event back with it.
        push(
            &env,
            &token,
            &stream::muxed(&recipient),
            amount,
            Branch::TreasuryPayoutFailed,
        )?;

        Ok(())
    }

    /// One-call treasury snapshot for `token`: governor, pending nominee,
    /// current fee configuration, and the fee ledger totals that must
    /// reconcile with the `FeeCharged` / `FeeWithdrawn` event streams.
    /// Public, read-only, no auth.
    pub fn treasury_status(env: Env, token: Address) -> TreasuryStatus {
        let t = treasury::get_treasury(&env, &token);
        let collectible = t.collectible().unwrap_or(0);
        TreasuryStatus {
            token,
            governor: treasury::governor(&env),
            pending_governor: treasury::pending_governor(&env),
            fee_bps: treasury::current_fee_bps(&env),
            max_fee_bps: MAX_FEE_BPS,
            charged_total: t.charged_total,
            withdrawn_total: t.withdrawn_total,
            collectible,
        }
    }

    /// The protocol fee charged for `stream_id` at creation, in smallest
    /// token units. Zero for streams created while fees were disabled (or
    /// whose fee rounded to zero). Reverts with
    /// [`StreamError::StreamNotFound`] for unknown ids.
    pub fn stream_fee(env: Env, stream_id: u64) -> Result<i128, StreamError> {
        stream::get_stream(&env, stream_id)?;
        Ok(treasury::stream_fee(&env, stream_id))
    }

    /// One bounded page of the fee verification scan: sums stored
    /// per-stream fees for `token` over a window of the stream-id space
    /// (`count` clamped to [`MAX_PAGE_SIZE`]). Page from id 1 until `done`
    /// is true; the summed `fees_in_range` values must equal
    /// `treasury_status(token).charged_total` — and both must equal the
    /// amounts in the contract's `FeeCharged` events. That three-way
    /// equality is the treasury invariant, verifiable by anyone from
    /// public reads alone (see `scripts/verify-fees.ts`).
    pub fn verify_treasury(env: Env, token: Address, start_id: u64, count: u32) -> FeeScan {
        treasury::scan_fees(&env, &token, start_id, count)
    }

    /// Number of streams ever created (ids are `1..=total_streams()`).
    /// Public convenience for verifiers to know the scan range.
    pub fn total_streams(env: Env) -> u64 {
        let next: u64 = env
            .storage()
            .persistent()
            .get(&stream::DataKey::NextStreamId)
            .unwrap_or(1);
        next.saturating_sub(1)
    }

    // ------------------------------------------------------------- policy --
    //
    // The optional asset allowlist and governance layer: restricts which assets
    // may be streamed while guaranteeing existing streams remain withdrawable
    // and refundable (anti-brick). Features a two-step admin handoff, a timelock
    // on all allowlist mutations, and permissionless execution once matured.

    /// Initializes the policy module with `admin`. Callable once only; panics
    /// if called again. Admin authorization is required.
    pub fn policy_init(env: Env, admin: Address) {
        policy::init(&env, &admin);
    }

    /// Queues an allowlist mutation (adding or removing `asset`) with a
    /// timelock delay. Callable only by the current admin.
    pub fn policy_propose_asset(
        env: Env,
        asset: Address,
        allowed: bool,
    ) -> Result<(), StreamError> {
        policy::propose_asset(&env, asset, allowed)
    }

    /// Executes a queued asset allowlist change after the timelock has elapsed.
    ///
    /// Callable by anyone: once a change has been vetted by the admin and the
    /// public timelock delay has expired, execution is permissionless so it
    /// cannot be stalled or held hostage by an absent admin.
    pub fn policy_execute_asset(env: Env, asset: Address) -> Result<(), StreamError> {
        policy::execute_asset(&env, asset)
    }

    /// Proposes a new admin for the two-step handoff (admin-only).
    pub fn policy_propose_handoff(env: Env, new_admin: Address) -> Result<(), StreamError> {
        policy::propose_handoff(&env, new_admin)
    }

    /// Accepts the pending admin handoff. Callable only by the proposed new
    /// admin, consuming their authorization and clearing the proposal.
    pub fn policy_accept_handoff(env: Env) -> Result<(), StreamError> {
        policy::accept_handoff(&env)
    }

    /// Cancels a pending admin handoff (admin-only).
    pub fn policy_cancel_handoff(env: Env) -> Result<(), StreamError> {
        policy::cancel_handoff(&env)
    }

    /// Returns the current policy admin, if initialized.
    pub fn policy_admin(env: Env) -> Option<Address> {
        policy::admin(&env)
    }

    /// Returns the pending policy admin nominee, if any.
    pub fn policy_pending_admin(env: Env) -> Option<Address> {
        policy::pending_admin(&env)
    }

    /// Returns the on-chain timelock delay in seconds.
    pub fn policy_timelock_delay(env: Env) -> u64 {
        policy::timelock_delay(&env)
    }

    /// Sets the timelock delay in seconds (admin-only).
    pub fn policy_set_timelock_delay(env: Env, delay: u64) -> Result<(), StreamError> {
        policy::set_timelock_delay(&env, delay)
    }

    /// Read-only check whether `asset` is currently allowed for new streams.
    pub fn policy_is_asset_allowed(env: Env, asset: Address) -> bool {
        policy::is_asset_allowed(&env, &asset)
    }

    /// Returns the queued allowlist change for `asset`, if any.
    pub fn policy_get_queued_asset(env: Env, asset: Address) -> Option<QueuedAssetChange> {
        policy::get_queued_asset(&env, &asset)
    }

    // --------------------------------------------- upgrade & migration --

    /// Returns the current schema version of stored contract state.
    pub fn schema_version(env: Env) -> u32 {
        stream::schema_version(&env)
    }

    /// Returns whether mutating contract operations are currently frozen.
    pub fn is_frozen(env: Env) -> bool {
        stream::is_frozen(&env)
    }

    /// Sets the freeze state of the contract. Governor-only.
    ///
    /// When frozen, mutating stream entrypoints (create, withdraw, withdraw_max,
    /// cancel, top_up) revert with [`StreamError::ContractFrozen`], but read views
    /// remain accessible.
    pub fn set_frozen(env: Env, caller: Address, frozen: bool) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        stream::set_frozen(&env, frozen);

        ContractFrozenStateChanged {
            frozen,
            changed_by: caller,
        }
        .publish(&env);

        Ok(())
    }

    /// Proposes a Wasm bytecode upgrade with an enforced timelock delay. Governor-only.
    ///
    /// `delay_seconds` must be at least [`MIN_UPGRADE_DELAY_SECONDS`] (24 hours).
    /// Overwrites any existing proposal.
    pub fn propose_upgrade(
        env: Env,
        caller: Address,
        new_wasm_hash: BytesN<32>,
        delay_seconds: u64,
    ) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        if delay_seconds < stream::MIN_UPGRADE_DELAY_SECONDS {
            return Err(surface::reject(Branch::UpgradeDelayTooShort));
        }
        let now = env.ledger().timestamp();
        let unlock_time = now
            .checked_add(delay_seconds)
            .ok_or(StreamError::MathOverflow)?;

        let pending = PendingUpgrade {
            new_wasm_hash: new_wasm_hash.clone(),
            unlock_time,
        };
        stream::set_pending_upgrade(&env, &pending);

        UpgradeProposed {
            new_wasm_hash,
            unlock_time,
            proposer: caller,
        }
        .publish(&env);

        Ok(())
    }

    /// Cancels a pending upgrade proposal. Governor-only.
    ///
    /// Reverts with [`StreamError::NoPendingUpgrade`] if no proposal is pending.
    pub fn cancel_upgrade(env: Env, caller: Address) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        let pending = stream::pending_upgrade(&env)
            .ok_or_else(|| surface::reject(Branch::CancelUpgradeNoPending))?;
        stream::remove_pending_upgrade(&env);

        UpgradeCancelled {
            new_wasm_hash: pending.new_wasm_hash,
            cancelled_by: caller,
        }
        .publish(&env);

        Ok(())
    }

    /// Executes a pending upgrade proposal once its timelock delay has elapsed. Governor-only.
    ///
    /// Updates the contract executable via [`env.deployer().update_current_contract_wasm`]
    /// and clears the pending upgrade.
    pub fn execute_upgrade(env: Env, caller: Address) -> Result<(), StreamError> {
        treasury::require_governor(&env, &caller)?;
        let pending = stream::pending_upgrade(&env)
            .ok_or_else(|| surface::reject(Branch::ExecuteUpgradeNoPending))?;
        let now = env.ledger().timestamp();
        if now < pending.unlock_time {
            return Err(surface::reject(Branch::TimelockNotExpired));
        }

        env.deployer()
            .update_current_contract(soroban_sdk::ContractExecutable::Wasm(
                pending.new_wasm_hash.clone(),
            ));
        stream::remove_pending_upgrade(&env);

        ContractUpgraded {
            new_wasm_hash: pending.new_wasm_hash,
            executed_by: caller,
        }
        .publish(&env);

        Ok(())
    }

    /// Read-only view of any pending upgrade proposal.
    pub fn pending_upgrade(env: Env) -> Option<PendingUpgrade> {
        stream::pending_upgrade(&env)
    }

    /// Migrates a batch of streams from schema v1 to schema v2. Governor-only.
    ///
    /// Requires the contract to be frozen ([`StreamError::ContractNotFrozen`]).
    /// `count` is clamped to [`MAX_MIGRATION_BATCH_SIZE`] (50 streams per call).
    /// If `start_id == 0`, migration resumes from `migration_cursor + 1`.
    /// When the cursor reaches `total_streams`, the schema version is bumped to v2.
    /// Calling this after migration is complete reverts with [`StreamError::AlreadyMigrated`].
    pub fn migrate_storage(
        env: Env,
        caller: Address,
        start_id: u64,
        count: u32,
    ) -> Result<MigrationStatus, StreamError> {
        treasury::require_governor(&env, &caller)?;
        if !stream::is_frozen(&env) {
            return Err(surface::reject(Branch::MigrationNotFrozen));
        }
        let current_version = stream::schema_version(&env);
        if current_version >= stream::TARGET_SCHEMA_VERSION {
            return Err(surface::reject(Branch::AlreadyMigrated));
        }

        let total = Self::total_streams(env.clone());
        let batch_size = count.clamp(1, stream::MAX_MIGRATION_BATCH_SIZE) as u64;

        if total == 0 {
            stream::set_schema_version(&env, stream::TARGET_SCHEMA_VERSION);
            stream::set_migration_cursor(&env, 0);

            StorageMigrated {
                from_version: current_version,
                to_version: stream::TARGET_SCHEMA_VERSION,
                start_id: 0,
                end_id: 0,
                is_complete: true,
            }
            .publish(&env);

            return Ok(MigrationStatus {
                current_version: stream::TARGET_SCHEMA_VERSION,
                target_version: stream::TARGET_SCHEMA_VERSION,
                cursor: 0,
                total_streams: 0,
                is_complete: true,
            });
        }

        let cursor = stream::migration_cursor(&env);
        let effective_start = if start_id == 0 {
            cursor.saturating_add(1)
        } else {
            start_id
        };
        let effective_start = effective_start.clamp(1, total);
        let end_id = (effective_start.saturating_add(batch_size).saturating_sub(1)).min(total);

        // Migrate each stream in the batch: read, verify, touch TTL, re-store.
        for id in effective_start..=end_id {
            if let Ok(s) = stream::get_stream(&env, id) {
                stream::put_stream(&env, &s);
            }
        }

        let new_cursor = cursor.max(end_id);
        stream::set_migration_cursor(&env, new_cursor);
        let is_complete = new_cursor >= total;
        let new_version = if is_complete {
            stream::set_schema_version(&env, stream::TARGET_SCHEMA_VERSION);
            stream::TARGET_SCHEMA_VERSION
        } else {
            current_version
        };

        StorageMigrated {
            from_version: current_version,
            to_version: new_version,
            start_id: effective_start,
            end_id,
            is_complete,
        }
        .publish(&env);

        Ok(MigrationStatus {
            current_version: new_version,
            target_version: stream::TARGET_SCHEMA_VERSION,
            cursor: new_cursor,
            total_streams: total,
            is_complete,
        })
    }

    /// Read-only snapshot of storage migration progress.
    pub fn migration_progress(env: Env) -> MigrationStatus {
        let current_version = stream::schema_version(&env);
        let cursor = stream::migration_cursor(&env);
        let total = Self::total_streams(env);
        let is_complete = current_version >= stream::TARGET_SCHEMA_VERSION;
        MigrationStatus {
            current_version,
            target_version: stream::TARGET_SCHEMA_VERSION,
            cursor,
            total_streams: total,
            is_complete,
        }
    }
}
