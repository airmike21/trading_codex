# IBKR Paper Lane

This is the narrow Stage 2 IBKR PaperTrader path for `primary_live_candidate_v1`.
It is intentionally limited to one strategy, long-only ETFs, whole shares, and manual `status` / `apply` operation.

## Preconditions

- Run the IBKR Client Portal Gateway or Web API bridge locally and authenticate a PaperTrader session.
- Use the PaperTrader username, not the live username.
- Do not store credentials, secrets, or account identifiers in repo files.

## Environment

Set these outside the repo:

```bash
export IBKR_PAPER_ACCOUNT_ID="DUXXXXXXX"
export IBKR_WEB_API_BASE_URL="https://127.0.0.1:5000/v1/api"
export IBKR_WEB_API_VERIFY_SSL="false"
export IBKR_WEB_API_TIMEOUT_SECONDS="15"
```

`IBKR_PAPER_ACCOUNT_ID` is required.
`IBKR_WEB_API_BASE_URL` defaults to `https://127.0.0.1:5000/v1/api`.
`IBKR_WEB_API_VERIFY_SSL` defaults to `false` because local gateway installs often use a self-signed certificate.

## Commands

Status / reconcile from the primary preset:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json status --preset dual_mom_vol10_cash_core
```

Apply from the primary preset:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json apply --preset dual_mom_vol10_cash_core
```

Inspect one pending claim explicitly by `event_id`:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json claim-status \
  --event-id "2026-03-30:dual_mom_vol10_cash:ENTER:EFA:100::2026-04-20"
```

Inspect the latest pending claim:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit text claim-status --latest
```

Resolve a pending claim as manually verified applied:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json claim-resolve \
  --event-id "2026-03-30:dual_mom_vol10_cash:ENTER:EFA:100::2026-04-20" \
  --mark-applied
```

Clear a pending claim for safe retry when no submit acknowledgement may have reached IBKR:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit text claim-resolve \
  --event-id "2026-03-30:dual_mom_vol10_cash:ENTER:EFA:100::2026-04-20" \
  --clear-for-retry
```

Operate from a saved signal payload:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json status \
  --signal-json-file /path/to/next_action.json \
  --allowed-symbols SPY,QQQ,IWM,EFA,BIL
```

Shadow execution v1 against the confirmed WSL Paper TWS socket. This path uses the IBKR TWS / IB Gateway socket API via `ibapi`, not the Client Portal Gateway. It is read-only and no-submit only:

```bash
.venv/bin/python scripts/ibkr_shadow_paper.py --emit json \
  --ibkr-account-id DUXXXXXXX \
  --preset dual_mom_vol10_cash_core
```

Manual shadow-loop change detection wrapper. Each invocation runs the same no-submit shadow path once, persists the last full `shadow_action_fingerprint` locally, and reports `first_seen`, `unchanged`, or `changed`:

```bash
.venv/bin/python scripts/ibkr_shadow_loop.py --emit json \
  --ibkr-account-id DUXXXXXXX \
  --preset dual_mom_vol10_cash_core
```

Wrapper notes:

- manual invocation only; no scheduler, daemon, or polling loop is introduced
- text output is a single concise line with `state`, `change`, and short fingerprint fields
- JSON output includes the full `shadow_action_fingerprint`, additive `shadow_action_fingerprint_short`, and `change_status`
- local change-detection state defaults to the same durable path policy used elsewhere: `~/.trading_codex`, then `~/.cache/trading_codex`, then `/tmp/trading_codex`
- override the persisted change-detection state location with `--state-dir` or `--state-file`; override the logical key with `--state-key`

Defaults for the shadow path:

- host `172.26.192.1`
- port `7497`
- market orders are translated only as a proposed IBKR order shape
- no orders are placed and no broker/account state is mutated

Shadow command prerequisites:

- TWS or IB Gateway must be running in paper mode
- API access for socket clients must be enabled in TWS / IB Gateway
- the socket endpoint must match the configured host / port; the default is `172.26.192.1:7497`
- `ibapi` must be installed in the repo environment used to run `.venv/bin/python scripts/ibkr_shadow_paper.py`
- the Client Portal Gateway environment variables above are not used by `scripts/ibkr_shadow_paper.py`

The shadow payload includes:

- endpoint used
- timestamp
- `simulation_only` / `no_submit`
- signal target metadata plus broker current position and delta-to-target reconciliation fields
- explicit shadow reconciliation summary fields such as `action_state`, `has_drift`, `is_noop`, `proposed_order_count`, `managed_symbol_count`, and `broker_position_symbol_count`
- a deterministic `shadow_action_fingerprint` for repeated identical shadow action sets
- intended IBKR order shape for each proposed order

The archived shadow run now also carries additive manifest metadata for the endpoint, account id, no-submit status, proposed-order count, drift/no-op summary, and a concise text summary artifact.

## Durable Local State

By default the lane stores local state under the run archive root in:

`ibkr_paper_lane/<state_key>/`

Important files:

- `ibkr_paper_state.json`: last status / attempt / applied summary
- `ibkr_paper_ledger.jsonl`: durable local operator ledger
- `event_receipts/`: applied event receipts keyed by `event_id`
- `pending_claims/`: restart-safe submit claims for interrupted or unresolved apply attempts

If an apply leaves a pending claim, the lane will refuse duplicate submit for that same `event_id` until the claim is manually reviewed.

## Pending Claim Workflow

Use `claim-status` first. It is read-only and reports the stored pending claim, whether `acknowledged_submit_may_have_reached_ibkr` is true, whether a reply is still pending, and whether `clear-for-retry` is allowed.

Use `claim-resolve` only after operator review of the broker-side outcome for that exact `event_id`.

### Claim Created By Reply-Required Warning

This happens when IBKR returns a reply-required warning and `apply` is run without `--confirm-replies`.

Safe workflow:

1. Run `claim-status --event-id ...`.
2. If the warning was not confirmed anywhere and no acknowledged submit reached IBKR, use `claim-resolve --event-id ... --clear-for-retry`.
3. Re-run `apply` normally once the claim is cleared.

This path removes the pending claim, does not write an event receipt, updates local state, and writes a ledger entry showing the manual clear-for-retry outcome.

### Claim Created After Acknowledged Submit Plus Later Error

This happens when the lane received a submit acknowledgement or broker order id, but later status fetch or follow-up handling failed.

Safe workflow:

1. Run `claim-status --event-id ...`.
2. Verify in IBKR PaperTrader whether the submit actually reached the broker and whether the event should be treated as applied.
3. Use `claim-resolve --event-id ... --mark-applied` once that verification is complete.

This path writes an event receipt, removes the pending claim, updates local state, and writes a ledger entry showing the manual mark-applied outcome.

### Retry Allowed Versus Refused

- `clear-for-retry` is allowed only when `acknowledged_submit_may_have_reached_ibkr` is false and no event receipt already exists.
- `clear-for-retry` is refused when `acknowledged_submit_may_have_reached_ibkr` is true. In that case the lane fails closed and requires the safer `--mark-applied` path after operator verification.
- `mark-applied` remains the safe manual resolution when broker-side evidence shows the event should be treated as already applied.
