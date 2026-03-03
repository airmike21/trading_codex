# Assistant Brief (Trading Codex)

## Purpose
This repo implements systematic, rules-based trading strategies and “next action” alerts for manual execution (tastytrade). Work should be repeatable, testable, and alert-friendly (no second-by-second timing).

## Repo + Environment
- Repo root (WSL Ubuntu): `~/trading_codex`
- Remote: `git@github.com:airmike21/trading_codex.git`
- Default branch: `master` (NOT `main`)
- I use Codex CLI in WSL and want large “Codex-ready” prompts that Codex can execute end-to-end.

## Non-Negotiable Invariants
### Next Action Output Contract
- `--next-action-json` prints **exactly one line** (minified JSON).
- The WSL alert wrapper prints **ONE line only on change** and prints **nothing** if unchanged.
- `event_id` composition MUST NOT change:
  `"{date}:{strategy}:{action}:{symbol}:{target_shares}:{resize_new_shares}:{next_rebalance}"`

### Backward Compatibility
- `scripts/next_action_alert.py` supports:
  - `--emit json|text`
  - `--state-file` (explicit path: MUST remain backward compatible)
  - `--state-key` (keyed state isolation so multiple monitors don’t clobber each other)
  - `--dry-run`, `--verbose`
- Do NOT break existing schemas or CLI flags.

## Current Alerting System
### WSL
- `scripts/next_action_alert.py`:
  - Runs the backtest command that emits `--next-action-json`.
  - Compares `event_id` vs state and prints one line only on change.

### Windows
- Runner script (live path):
  - `%USERPROFILE%\Scripts\trading_codex_next_action_alert.ps1`
- Uses BurntToast:
  - RESIZE: shows “RESIZE prev→new / target_shares” and includes leverage/vol_target if present.
  - HOLD: “HOLD (no trade)”
  - Small text includes symbol, next_rebalance, event_id.
- Config:
  - `%USERPROFILE%\trading_codex_alerts.json`
- Generator (prints schtasks commands):
  - `scripts/windows/generate_schtasks.ps1`

## Codex Workflow Contract (How Changes Must Be Made)
When implementing a task, Codex should:
1) Create a branch (never commit directly to `master` unless explicitly told).
2) Make changes.
3) Add/adjust tests.
4) Run tests: `.venv/bin/python -m pytest -q`
5) Commit with a clear message.
6) Push the branch to origin.
7) Print a final report containing:
   - `git status --short --branch`
   - `git log -1 --oneline`
   - pytest summary line
   - `git diff --stat origin/master..HEAD`
   - one example command showing the new feature works

## Useful Commands
### Run tests
- `.venv/bin/python -m pytest -q`

### Clear keyed alert state (WSL)
- `rm -rf ~/.cache/trading_codex/next_action_alert/*`

### Windows tasks
- The generator prints `/Create` + `/Run` commands:
  - `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%USERPROFILE%\Scripts\generate_schtasks.ps1" -ConfigPath "%USERPROFILE%\trading_codex_alerts.json"`

## Current Priorities (Update This As We Go)
### Next Task: Volatility Target Overlay v1
Implement a reusable “volatility targeting overlay” that can apply to any strategy’s raw target weights.

Requirements summary:
- Realized vol: annualized stdev of daily portfolio returns over lookback (default 63).
- Leverage scalar: `clamp(target_vol / realized_vol, min_leverage, max_leverage)`
  - Defaults: `target_vol=0.10`, `lookback=63`, `min_leverage=0.0`, `max_leverage=1.0`
  - Handle `realized_vol ~ 0` deterministically (no crash; unit test).
- Apply overlay AFTER strategy computes raw weights, BEFORE translating to target shares.
- CLI flags:
  - `--vol-target` (enables overlay when provided)
  - `--vol-lookback` (default 63)
  - `--min-leverage` (default 0.0)
  - `--max-leverage` (default 1.0)
- next_action JSON should include `leverage` and `vol_target` when enabled (and optionally `realized_vol`, `lookback`) WITHOUT changing `event_id`.
- Add unit tests + integration smoke test.
- Keep one-line/no-output-on-unchanged contracts.

## How To Start a New Chat
Paste `docs/BOOTSTRAP_PROMPT.txt` into a new chat. It points the assistant here and asks for a single Codex-ready prompt for the current priority task.
