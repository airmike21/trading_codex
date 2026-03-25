# Tastytrade Sandbox Capability

Last updated: 2026-03-25

This document scopes the bounded Stage 1 tastytrade sandbox lane for the first-live program.
It is intentionally sandbox-only and exists to produce durable evidence about what the repo can and cannot do against tastytrade sandbox today.

## Command

Default command for the current primary live candidate universe:

```bash
.venv/bin/python scripts/tastytrade_sandbox_capability.py \
  --preset dual_mom_vol10_cash_core \
  --secrets-file ~/.config/trading_codex/tastytrade_sandbox.env \
  --emit json
```

Explicit-symbol variant:

```bash
.venv/bin/python scripts/tastytrade_sandbox_capability.py \
  --symbols EFA BIL SPY QQQ IWM \
  --probe-order-symbol EFA \
  --secrets-file ~/.config/trading_codex/tastytrade_sandbox.env \
  --emit json
```

Opt-in sandbox submit:

```bash
.venv/bin/python scripts/tastytrade_sandbox_capability.py \
  --preset dual_mom_vol10_cash_core \
  --secrets-file ~/.config/trading_codex/tastytrade_sandbox.env \
  --enable-sandbox-submit \
  --sandbox-submit-account 5WT00001 \
  --emit json
```

## Evidence Recorded

Each run archives a machine-readable JSON report and a text summary under the standard Trading Codex archive root:

- `~/.trading_codex`
- `~/.cache/trading_codex`
- `/tmp/trading_codex`

The report records pass/fail/blocked status for:

- auth
- account discovery/selection
- balances
- positions
- instrument lookup
- quote lookup
- order construction
- order preview
- sandbox submit
- sandbox cancel

The report also records:

- exact sandbox base URL used
- whether the host passed the sandbox-host guard
- selected account id
- per-symbol instrument/quote probe attempts
- synthetic preview order details
- submit/cancel request details when opt-in mutation flags are used
- manifest/report artifact paths for future review

## Sandbox Config

Use sandbox-prefixed env keys only.
Do not reuse live-prefixed tastytrade settings for this command.

Supported keys:

- `TASTYTRADE_SANDBOX_API_BASE_URL`
- `TASTYTRADE_SANDBOX_ACCOUNT`
- `TASTYTRADE_SANDBOX_USERNAME`
- `TASTYTRADE_SANDBOX_PASSWORD`
- `TASTYTRADE_SANDBOX_SESSION_TOKEN`
- `TASTYTRADE_SANDBOX_ACCESS_TOKEN`
- `TASTYTRADE_SANDBOX_CHALLENGE_CODE`
- `TASTYTRADE_SANDBOX_CHALLENGE_TOKEN`
- `TASTYTRADE_SANDBOX_TIMEOUT_SECONDS`

See `docs/examples/tastytrade_sandbox.env.example`.

## Boundaries

- Sandbox only
- No scheduler or automation
- No persistent paper-trading lane yet
- No new strategy work
- No broker abstraction expansion
- No live-account submit changes

## Remaining Blockers After This Slice

- Instrument and quote checks are probe-based. The repo now records exact sandbox endpoint outcomes, but future chats should use the archived evidence from a real sandbox run before assuming those paths are settled.
- Sandbox submit is still off by default and requires both the explicit enable flag and an account confirmation that matches the selected sandbox account.
- Sandbox cancel is best-effort probe logic only. If the archived report shows it blocked or failed, treat that as unresolved sandbox evidence rather than paper over it with broader broker work.
- This slice does not start the persistent paper-trading lane.
