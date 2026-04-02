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

Operate from a saved signal payload:

```bash
.venv/bin/python scripts/ibkr_paper_lane.py --emit json status \
  --signal-json-file /path/to/next_action.json \
  --allowed-symbols SPY,QQQ,IWM,EFA,BIL
```

Shadow execution v1 against the confirmed WSL Paper TWS socket. This is read-only and no-submit only:

```bash
.venv/bin/python scripts/ibkr_shadow_paper.py --emit json \
  --ibkr-account-id DUXXXXXXX \
  --preset dual_mom_vol10_cash_core
```

Defaults for the shadow path:

- host `172.26.192.1`
- port `7497`
- market orders are translated only as a proposed IBKR order shape
- no orders are placed and no broker/account state is mutated

The shadow payload includes:

- endpoint used
- timestamp
- `simulation_only` / `no_submit`
- symbol, action, target shares, current position, and delta-to-target reconciliation fields
- intended IBKR order shape for each proposed order

## Durable Local State

By default the lane stores local state under the run archive root in:

`ibkr_paper_lane/<state_key>/`

Important files:

- `ibkr_paper_state.json`: last status / attempt / applied summary
- `ibkr_paper_ledger.jsonl`: durable local operator ledger
- `event_receipts/`: applied event receipts keyed by `event_id`
- `pending_claims/`: restart-safe submit claims for interrupted or unresolved apply attempts

If an apply leaves a pending claim, the lane will refuse duplicate submit for that same `event_id` until the claim is manually reviewed.
