# economy-content-agent

AI 경제 콘텐츠 분석 자동화 프로젝트입니다.

현재 이 저장소는 `/Users/barq/developer/Analyst_Opinion_Archive`의 정본 코드를
`/Users/barq/developer/projects/economy-content-agent` Git 저장소로 흡수한 상태입니다.

## 실행

Python 3.10 이상과 의존성 설치가 필요합니다.

```bash
cd /Users/barq/developer/projects/economy-content-agent
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 agent.py --help
./run_agent.sh
```

기본 `python3`가 3.10 미만이면 아래처럼 명시할 수 있습니다.

```bash
PYTHON_BIN=/opt/homebrew/bin/python3.11 ./run_agent.sh
```

## 로컬 전용 파일

아래 파일은 실행에 필요할 수 있지만 Git에는 포함하지 않습니다.

- `.env`
- `.env.example`만 Git에 유지
- `credentials.json`
- `failed_url_queue.sqlite3`
- `trust_data.sqlite3`
- `ops_events.jsonl`
- `person_review_memory.json`
- `backups/`

## 환경변수 / 템플릿

- 기본 템플릿: `/Users/barq/developer/projects/economy-content-agent/.env.example`
- 백업 설정 템플릿: `/Users/barq/developer/projects/economy-content-agent/deploy/backup.env.example`
- launchd 템플릿: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent.plist.example`
- launchd 백업 템플릿: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent-backup.plist.example`

`credentials.json` 경로를 기본 위치가 아닌 곳에 두고 싶다면 `.env`에
`GOOGLE_SERVICE_ACCOUNT_FILE=/absolute/path/to/credentials.json` 형태로 지정할 수 있습니다.

Notion integration 정본은 `노션-개발`로 고정합니다.

- `.env`의 `NOTION_API_KEY`는 개발용 integration 키만 사용
- `.env`의 `EXPECTED_NOTION_BOT_NAME` 기본값은 `노션-개발`
- 다른 키가 남아 있는 archive/legacy `.env`는 정리하거나 동일 키로 맞춰 혼선을 줄입니다.

LLM 문맥용 시트 탭 이름도 환경변수로 바꿀 수 있습니다.

- `EXPERT_SNAPSHOT_TAB` 기본값: `Expert_Snapshot`
- `REVIEW_QUEUE_TAB` 기본값: `Review_Queue`
- `TRUST_DATA_DB_PATH` 기본값: `/Users/barq/developer/projects/economy-content-agent/trust_data.sqlite3`
- `CLAIM_SAMPLE_DIR` 기본값: `/Users/barq/developer/projects/economy-content-agent/analysis/pilot_samples`
- `TRUST_SYMBOL_MAP_PATH` 기본값: `/Users/barq/developer/projects/economy-content-agent/data/trust_symbol_mappings.json`
- `TRUST_CLAIM_OVERRIDE_PATH` 기본값: `/Users/barq/developer/projects/economy-content-agent/data/trust_claim_overrides.local.json`

## 운영 파일

- 실제 launchd 파일: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent.plist`
- 실제 백업 launchd 파일: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent-backup.plist`
- launchd 설치 스크립트: `/Users/barq/developer/projects/economy-content-agent/deploy/install_launchd.sh`
- archive 삭제 체크리스트: `/Users/barq/developer/projects/economy-content-agent/deploy/ARCHIVE_CLEANUP_CHECKLIST.md`

## 보조 도구

- 전문가 데이터 정제 도구: `/Users/barq/developer/projects/economy-content-agent/tools/expert-cleaning`

## 설계 문서

- 신뢰 전문가 데이터 파이프라인 설계도: `/Users/barq/developer/projects/economy-content-agent/analysis/TRUSTED_EXPERT_PIPELINE_BLUEPRINT.md`
- TrustScore v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/TRUSTSCORE_V1.md`
- Claim Schema v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CLAIM_SCHEMA_V1.md`
- Outcome Evaluation v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/OUTCOME_EVALUATION_SPEC_V1.md`
- Continuous Validation Loop v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CONTINUOUS_VALIDATION_LOOP_V1.md`
- Person Taxonomy v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/PERSON_TAXONOMY_V1.md`
- Content Person Role Schema v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CONTENT_PERSON_ROLE_SCHEMA_V1.md`
- Storage Schema Draft v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/STORAGE_SCHEMA_DRAFT_V1.md`
- Google Sheets for LLM Context v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/GOOGLE_SHEETS_FOR_LLM_CONTEXT_V1.md`
- Sheets Tab Schema Expert_Snapshot v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/SHEETS_TAB_SCHEMA_EXPERT_SNAPSHOT_V1.md`
- Sheets Tab Schema Review_Queue v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/SHEETS_TAB_SCHEMA_REVIEW_QUEUE_V1.md`
- Claim Extraction Prompt v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CLAIM_EXTRACTION_PROMPT_V1.md`
- Claim Pilot Plan v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CLAIM_PILOT_PLAN_V1.md`
- Pilot Video Selection Template v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/PILOT_VIDEO_SELECTION_TEMPLATE_V1.md`
- Pilot Video Selection Recommended v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/PILOT_VIDEO_SELECTION_RECOMMENDED_V1.md`
- Claim Goldset Template v1 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CLAIM_GOLDSET_TEMPLATE_V1.json`
- Claude Code Review Prompt 문서: `/Users/barq/developer/projects/economy-content-agent/analysis/CLAUDE_CODE_REVIEW_PROMPT_TRUSTSCORE_V1.md`
- Pilot sample claims 폴더: `/Users/barq/developer/projects/economy-content-agent/analysis/pilot_samples`

## 인물 클렌징 운영 메모

- 인물 경량 동기화는 `PEOPLE_LIGHT_INTERVAL_MIN` 주기로 실행됩니다.
- 인물 full cleanse는 `PEOPLE_DAWN_ENABLED=1`일 때 `PEOPLE_DAWN_HOUR`, `PEOPLE_DAWN_MINUTE`, `PEOPLE_FULL_CLEAN_WEEKDAY` 기준으로 주 1회 실행됩니다.
- 활동명은 `닉네임`, 실명은 `이름`으로 정규화합니다.

## LLM 문맥 시트

- `python agent.py --sync-expert-snapshot`: `Expert_Snapshot` 탭 재구성
- `python agent.py --init-review-queue`: `Review_Queue` 탭 헤더 초기화
- `python agent.py --sync-review-queue`: `needs_review` outcome을 `Review_Queue` 탭에 동기화
- `python agent.py --apply-review-queue-resolutions`: `Review_Queue`에서 `resolved` 처리한 심볼 검수 결과를 로컬 override로 반영
- `python agent.py --sync-llm-context-sheets`: `Expert_Snapshot`과 `Review_Queue`를 함께 갱신
- `python agent.py --init-trust-store`: TrustScore/Claim/Outcome SQLite 저장소 초기화
- `python agent.py --bootstrap-trust-store`: Person DB 기준으로 TrustScore 저장소 기본 row 생성/갱신
- `python agent.py --ingest-claim-samples`: 파일럿 claim JSON을 SQLite claims 원장에 적재
- `python agent.py --recompute-trust-scores`: 현재 claims 기준 최소 TrustScore snapshot 재계산
- `python agent.py --refresh-claim-outcomes`: checkpoint 도래 claim outcome 갱신
- `python agent.py --force-refresh-claim-outcomes`: 파일럿 검증용 강제 outcome 갱신
- `python agent.py --check-content-sync`: Notion/Sheets 경제 콘텐츠 공통 필드 diff 점검 + 로컬 리포트 생성
- `python agent.py --reconcile-content-sync`: `only_notion`/`only_sheet`만 반영하고 `field_conflict`는 수동 검토 대상으로 유지

심볼/벤치마크 수동 매핑은 `/Users/barq/developer/projects/economy-content-agent/data/trust_symbol_mappings.json`에서 관리합니다.

`Review_Queue`에서 `symbol_review`를 해결할 때는 `notes` 컬럼에 예를 들어 `stooq_symbol=xauusd; benchmark_symbol=^spx; note=manual_gold_etf_proxy`처럼 적고 `status=resolved`로 바꾸면 됩니다. 이후 `python agent.py --apply-review-queue-resolutions`를 실행하면 로컬 override 파일에 반영됩니다.

## 콘텐츠 정렬 운영

- 범위: `Notion 경제 콘텐츠 DB` ↔ `Google Sheets 경제 콘텐츠 시트`
- 공통 키: `URL`
- 공통 필드: `콘텐츠 제목`, `채널`, `해시태그`, `한 줄 요약`, `출연자`, `인물의견`, `언급 상품`, `핵심 섹터`, `경제 전망`, `처리일시`
- 정책: 같은 `URL`에서 값이 다르면 자동 덮어쓰지 않고 `field_conflict`로 분류합니다.
- 로컬 diff 리포트: `/Users/barq/developer/projects/economy-content-agent/handoffs/content-sync-report-YYYY-MM-DD.md`
