# Analyst Opinion Agent - Execution Checklist

Updated: 2026-03-10 (KST)

## A. Stability Loop (Run-Recheck-Fix)
- [x] Add Python runtime guard (3.10+)
- [x] Fix cross-version typing crash (`list[str] | None` on python3.9)
- [x] Keep AST parse check in every edit cycle
- [x] Run full online diagnostics with production runtime (`python3.10`) repeatedly until no mismatch
- [x] Add automated healthcheck command (`--healthcheck`) for one-shot validation

## B. Person DB Quality (Accumulate, not wipe)
- [x] Confidence score thresholds introduced
- [x] Uncertain person gating + review workflow retained
- [x] Accumulation mode defaults enabled (skip purge/rebuild by default)
- [x] Multi-name contamination guard (`A, B`) added
- [x] Auto-reconciliation for Notion-only / Sheet-only keys
- [x] Add confidence columns to Sheet/Notion summary columns (score, status)
- [ ] Add periodic anomaly report message to admin/review chat

## C. Daily Briefing Product Design
- [x] Traffic-light structured briefing prompt
- [x] Feedback buttons (`좋아요`, `아쉬워요`, `설정`)
- [x] Feedback persistence to briefing log
- [x] Daily highlights tab logging
- [x] Information architecture draft documented (`MASTER_DESIGN_PROPOSAL.md`)
- [ ] Refine “expert outlook” into deterministic percentage block (post-generation validator)
- [ ] Add quality KPIs (open/click/reaction proxy) and weekly trend review

## D. Autonomous Content Discovery
- [x] Define source pool strategy draft (`MASTER_DESIGN_PROPOSAL.md`)
- [ ] Build low-token discovery pipeline (metadata first, transcript on demand)
- [ ] Add dedupe gate (URL, title, semantic near-duplicate)
- [ ] Add priority scoring (macro relevance, urgency, novelty)

## E. Backup / Data Governance
- [x] Snapshot/GitHub backup strategy draft (`MASTER_DESIGN_PROPOSAL.md`)
- [x] GitHub backup strategy (private repo, secrets excluded, signed tags) draft
- [ ] Retention policy (raw logs vs curated datasets)
- [ ] Disaster recovery runbook

## F. FE/BE Architecture Review
- [x] Document current architecture and bottlenecks (draft)
- [x] Propose service split (ingest / analyze / entity / briefing / bot) draft
- [x] Entrypoint split applied (`cli`, `bot_runtime`, `background_loops`)
- [x] Service split applied for reporting and person-db core flows
- [x] SQLite-backed retry queue baseline applied
- [x] Queue + worker model baseline applied (SQLite lease/claim worker)
- [x] Observability baseline (structured ops event log + healthcheck event)
- [x] Person lookup/cache module split applied (`services/person_lookup.py`)
- [x] Person dedup module split applied (`services/dedup.py`)
- [x] Person review memory moved from JSON to SQLite (`person_review_log`)

## G. Claude Collaboration Package
- [x] Create handoff brief skeleton (`MASTER_DESIGN_PROPOSAL.md` + checklist)
- [x] Create final algorithm spec (`FINAL_ALGORITHM_SPEC.md`)
- [x] Prepare Claude review prompt (`CLAUDE_REVIEW_PROMPT.md`)
- [x] Apply Claude review round 1 fixes (confidence gate, dedupe guard, scheduler tolerance, orphan-page prevention, ops timestamp normalization)
- [x] Apply Claude review round 2 fixes (atomic queue rows, narrower person lock scope, archived-cache cleanup, uncertainty noise reduction)
- [x] Apply Claude review round 3 fixes (retry counter increment, uncertain relation continuity, queue init caching, Gemini candidate prefilter, existing-person profile backfill)
- [x] Apply Claude review round 4 fixes (uncertain path parity, real-time manual-input warnings, retry worker graceful shutdown, report scheduler persistence, sheet verify throttling)
- [x] Apply Claude review round 5 fixes (atomic scheduler state write, scheduler state pruning, main-thread signal handling, conditional profile backfill, ProcessPersonDeps backward compatibility)
- [x] Apply Claude review round 6 fixes (surname cache removal, conditional sync backfill, briefing dispatch lifecycle, dedup second-pass fingerprint scoring)
- [x] Apply Claude review round 7 fixes (dedup Google confirm fault isolation, empty-affiliation homonym guard, SQLite DB re-init after file deletion, accurate briefing sent_at)
- [x] Apply Claude review round 8 fixes (recent-match lookup before exact query, real-time new-person backfill parity, block-fetch cap note, ops autocommit cleanup)
- [x] Apply Claude review round 9 fixes (sheet delete log cleanup, sync status regex dedupe, enrich_all_people schema-safe Notion updates)
- [x] Replace report scheduler state file with SQLite briefing dispatch log
- [ ] Request Claude review on algorithm and schema choices
- [ ] Merge agreed changes and produce final algorithm spec

## H. UI for General Users
- [x] MVP screens draft defined (`MASTER_DESIGN_PROPOSAL.md`)
- [ ] RBAC and safe edit workflow
- [ ] Human-in-the-loop review UX for uncertain entities

## I. Deployment Plan (Mac mini/NAS or External Server)
- [x] Deployment matrix criteria draft (`MASTER_DESIGN_PROPOSAL.md`)
- [ ] Containerized runtime (Docker/Compose)
- [ ] Secret management and rotation
- [ ] Blue/green or canary rollout checklist
