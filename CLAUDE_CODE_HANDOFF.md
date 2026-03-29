# Claude Code Handoff

Updated: 2026-03-11 (KST)

## 목적
- 현재 경제 콘텐츠 분석 에이전트의 구조/알고리즘/운영 안정성을 검수받기 위한 전달 문서
- 이번 라운드의 초점은:
  1. 인물 동일성 판단과 불확실 인물 처리
  2. SQLite retry queue + lease/claim worker 안전성
  3. 실시간/배치 경로 일관성

## 현재 상태 요약
- `entrypoints`, `adapters`, `services` 분리 완료
- `services/person_identity.py` 분리 완료
- `services/person_lookup.py` 분리 완료
- `services/dedup.py` 분리 완료
- 실패 URL 큐를 JSON에서 SQLite로 전환 완료
- report scheduler 발송 상태를 SQLite briefing dispatch log로 관리
- person review memory를 SQLite person_review_log로 관리
- retry queue는 `lease_owner`, `lease_until` 기반 claim/worker baseline 적용 완료
- 전용 retry worker CLI 추가 완료
- 봇/launchd는 현재 중지 상태

## 이번 라운드 핵심 변경

### 1. 인물 동일성 / 신뢰도 / 불확실 판정 분리
- 파일:
  - `services/person_identity.py`
  - `agent.py`
- 변경 내용:
  - Gemini 동일인 판정에 confidence gate 적용
  - 신뢰도 점수 계산과 불확실 판정을 서비스 계층으로 이동
  - 비경제/허용 카테고리 외 인물 후보 산출을 서비스 계층으로 이동

### 2. 인물 upsert 공통화
- 파일:
  - `services/people.py`
- 변경 내용:
  - `process_person_db`와 `sync_person_and_link`의 중복 로직을 `_upsert_person_core()`로 통합
  - 신규 인물 생성 후 불확실 판정 시 orphan page가 남지 않도록 임시 보관 처리

### 3. 배치/실시간 정합성 강화
- 파일:
  - `agent.py`
- 변경 내용:
  - `_PERSON_DB_LOCK`를 `RLock`으로 교체
  - `sync_people_from_notion`, `rebuild_people_db`, `reconcile_people_sync` 락 보호
  - `reconcile_people_sync`가 Notion 기준으로 sheet stale row를 제거하도록 수정

### 4. retry queue 저장소/worker 개선
- 파일:
  - `services/ops.py`
  - `entrypoints/retry_worker.py`
  - `entrypoints/cli.py`
  - `agent.py`
- 변경 내용:
  - 저장소를 `queue.sqlite3`로 전환
  - 레거시 `failed_url_queue.json` 자동 마이그레이션
  - `lease_owner`, `lease_until` 기반 claim
  - due item만 선점해서 처리하는 worker baseline 구현
  - 전용 CLI: `--run-retry-worker`

### 5. Claude Round 2 피드백 반영
- 파일:
  - `services/ops.py`
  - `services/people.py`
  - `services/person_identity.py`
  - `agent.py`
- 변경 내용:
  - `enqueue_failed_url`, `dequeue_failed_url`를 전체 load-modify-save 대신 row 단위 원자 작업으로 보정
  - `_upsert_person_core()`에서 Google/Gemini 검색을 person lock 밖으로 이동
  - archived 처리 직후 최근 인물 캐시 제거
  - source_url 없는 기존 인물의 불확실 판정 노이즈 완화
  - 기존 인물이 uncertain이어도 콘텐츠 relation은 유지 가능하도록 조정
  - 축약 이름 매칭을 더 보수적으로 강화

### 6. Claude Round 3 피드백 반영
- 파일:
  - `services/ops.py`
  - `services/people.py`
  - `agent.py`
  - `entrypoints/background_loops.py`
- 변경 내용:
  - retry worker 실패 시 `retries`가 실제로 증가하도록 수정
  - retry row 재삽입 전에 현재 큐 존재 여부를 확인하도록 보강
  - SQLite queue 초기화(`WAL`, schema check)를 DB path당 1회만 수행하도록 캐시 추가
  - 기존 uncertain 인물도 relation 연결/시트 저장이 계속 진행되도록 수정
  - 기존 인물의 search enrichment 결과(`career`, `expertise`, `source_url`)를 Notion person page에 backfill
  - Gemini 동일인 fallback 전에 후보를 점수 기반 상위 20개로 축소
  - 내장 failed retry scheduler를 환경변수로 비활성화할 수 있도록 추가 (`FAILED_RETRY_SCHEDULER_ENABLED`)

### 7. Claude Round 4 피드백 반영
- 파일:
  - `services/people.py`
  - `entrypoints/retry_worker.py`
  - `entrypoints/background_loops.py`
  - `services/ops.py`
  - `agent.py`
- 변경 내용:
  - `process_person_db`도 기존 uncertain 인물에 대해 body/sheet 기록을 유지하도록 배치 경로와 동작을 맞춤
  - 실시간 경로에도 `needs_manual_person_input`, `warn_manual_person_input_needed` 연결
  - retry worker에 `SIGTERM`/`SIGINT` graceful shutdown 추가
  - report scheduler 발송 상태를 영속화해 재시작 중복 발송 위험 완화
  - Economic_Expert 시트 저장의 verify re-read를 기본 비활성화 (`EXPERT_SHEET_VERIFY_WRITE=0`)
  - `save_failed_url_queue`에 레거시 전체 동기화 경고 추가

### 8. Claude Round 5 피드백 반영
- 파일:
  - `services/ops.py`
  - `entrypoints/background_loops.py`
  - `entrypoints/retry_worker.py`
  - `services/people.py`
- 변경 내용:
  - report scheduler 상태 파일을 제거하고 `queue.sqlite3`의 `briefing_dispatch_log`로 전환
  - `pending/sent/failed` dispatch lifecycle을 도입해 발송 실패 후 재시도 가능하도록 보강
  - retry worker signal 등록은 main thread에서만 시도하고, 비주 스레드면 명시적으로 로그만 남기도록 변경
  - 기존 인물 profile backfill은 실제 결손 필드가 있을 때만 호출하도록 최적화
  - `ProcessPersonDeps`의 optional manual-input warning 필드에 기본값을 추가해 하위호환성 확보

### 9. Claude Round 6 피드백 반영
- 파일:
  - `services/person_lookup.py`
  - `services/people.py`
  - `services/dedup.py`
  - `services/ops.py`
  - `entrypoints/background_loops.py`
  - `agent.py`
- 변경 내용:
  - `surname:` 기반 최근 인물 캐시 키를 제거해 동성(同姓) 인물 오매칭 위험 제거
  - `sync_person_and_link`의 불필요한 기존 인물 profile backfill 재호출 제거
  - dedup 2차 강제 병합 패스에서 raw Notion page가 아니라 fingerprint로 점수 계산하도록 수정
  - `briefing_dispatch_log`를 `claim -> sent/failed update` 흐름으로 변경
  - `ops.py`의 bare filename 경로에서도 동작하도록 `os.makedirs()` guard 추가

### 10. Claude Round 7 피드백 반영
- 파일:
  - `services/dedup.py`
  - `services/person_lookup.py`
  - `services/ops.py`
  - `entrypoints/background_loops.py`
- 변경 내용:
  - dedup 배치에서 `google_confirm_duplicate()` 예외가 전체 루프를 중단시키지 않도록 fault isolation 추가
  - 소속이 비어 있는 동명이인 입력은 즉시 첫 후보로 반환하지 않도록 보호
  - `find_conflicting_candidates()`의 Notion block 조회를 상위 일부 후보로 제한해 O(N) 호출 완화
  - SQLite DB 파일이 런타임 중 삭제되어도 `_ensure_failed_queue_db()`가 재초기화되도록 수정
  - `briefing_dispatch_log.sent_at`이 실제 발송 완료/실패 시각을 기록하도록 보정

### 11. Claude Round 8 피드백 반영
- 파일:
  - `services/person_lookup.py`
  - `services/people.py`
  - `services/ops.py`
- 변경 내용:
  - 최근 인물 캐시 확인을 exact-name Notion 쿼리보다 먼저 수행해 불필요한 API 호출 감소
  - 실시간 경로(`process_person_db`)도 신규 인물 생성 직후 profile backfill을 수행해 배치 경로와 정합성 확보
  - `find_conflicting_candidates()`의 block fetch 상한이 의도적 최적화임을 코드 주석으로 명시
  - `_ensure_failed_queue_db()`의 autocommit 환경에서 불필요한 `conn.commit()` 제거

### 12. Claude Round 9 피드백 반영
- 파일:
  - `agent.py`
- 변경 내용:
  - `_delete_sheet_rows()` 내부 stray `for...else` 로그 출력 제거
  - `check_people_sync_status()` 내 중복 regex 컴파일 제거
  - `enrich_all_people()`의 Notion 속성 업데이트를 스키마 해석 기반(`_get_person_db_schema`, `_resolve_person_fields`, `_build_person_prop_value`)으로 통일

### 13. Confidence summary 컬럼 추가
- 파일:
  - `agent.py`
  - `services/people.py`
- 변경 내용:
  - `Economic_Expert` 시트에 `신뢰도 점수`, `신뢰도 상태` 컬럼 추가
  - 시트 범위를 헤더 길이 기반으로 계산하도록 변경해 컬럼 추가 시 하드코딩 범위가 깨지지 않게 보강
  - Notion 인물 DB 파생 컬럼에도 `신뢰도 점수`, `신뢰도 상태` 자동 생성/동기화 추가
  - 실시간/배치/rebuild/reconcile 경로 모두에서 confidence 값이 시트와 Notion 요약 컬럼에 유지되도록 정합성 확보

### 14. Person lookup 모듈 분리
- 파일:
  - `services/person_lookup.py`
  - `agent.py`
- 변경 내용:
  - 인물 조회/캐시/후보축소 로직을 `agent.py`에서 `services/person_lookup.py`로 이동
  - 이동 대상:
    - `person_name_key`
    - `person_aff_key`
    - `person_lookup_keys`
    - `remember_person_match`
    - `forget_person_match`
    - `find_recent_person_match`
    - `candidate_hint_score`
    - `find_conflicting_candidates`
    - `find_person_in_notion_db`
  - `agent.py`는 thin wrapper만 유지

### 15. Report scheduler 상태를 SQLite로 전환
- 파일:
  - `services/ops.py`
  - `entrypoints/background_loops.py`
  - `agent.py`
- 변경 내용:
  - `report_scheduler_state.json` 경로와 상태 파일 로직 제거
  - `queue.sqlite3` 내부 `briefing_dispatch_log` 테이블 추가
  - daily/weekly 발송 여부를 `was_briefing_sent()` / `claim_briefing_dispatch()` / `update_briefing_dispatch_status()`로 조회/기록
  - `pending/sent/failed` lifecycle 기반으로 재시작/중복 실행 시 idempotent 보장

### 16. Person dedup 모듈 분리
- 파일:
  - `services/dedup.py`
  - `agent.py`
- 변경 내용:
  - 인물 중복 탐지/병합 로직을 `agent.py`에서 `services/dedup.py`로 이동
  - 이동 대상:
    - `person_fingerprint`
    - `duplicate_score`
    - `google_confirm_duplicate`
    - `cluster_groups_by_edges`
    - `find_duplicate_person_groups_hybrid`
    - `auto_dedup_people_db`
    - `merge_person_group`
  - `agent.py`는 thin wrapper만 유지

### 17. Person review memory를 SQLite로 전환
- 파일:
  - `services/ops.py`
  - `agent.py`
- 변경 내용:
  - `person_review_log` 테이블 추가
  - 기존 `_mark_person_review`, `_is_person_review_approved` 인터페이스는 유지
  - 저장/조회 원본을 JSON에서 SQLite로 전환
  - 기존 `person_review_memory.json`이 있으면 최초 로드 시 SQLite로 1회 마이그레이션

### 18. Notion DB ↔ Google Sheets 스키마 정렬 (2026-03-29)
- 파일:
  - `adapters/notion_content_repo.py`
  - `agent.py`
  - Notion 경제 콘텐츠 DB (collection://314883f1-56f5-80ce-9afb-000b0f132728)
- 변경 내용:
  - Notion DB `주제` 컬럼 → `해시태그`로 rename (Sheets 컬럼명과 통일)
  - Notion DB에 누락 컬럼 4개 추가: `채널`, `언급 상품`, `핵심 섹터`, `경제 전망`
  - `notion_content_repo.write_result`에 `channel` 파라미터 추가 및 4개 신규 필드 기록 로직 추가
  - `get_unprocessed_pages` 미처리 sentinel 필터를 `주제` → `해시태그`로 변경
  - `agent.write_notion_result` 래퍼에 `channel` 파라미터 추가
  - `_finalize_page_with_analysis`에서 `write_notion_result` 호출 시 `channel=metadata.get("channel", "")` 전달
- 정렬 후 Notion/Sheets 공통 필드:
  - URL, 콘텐츠 제목, 채널, 해시태그, 한 줄 요약, 출연자(인물), 인물의견, 언급 상품, 핵심 섹터, 경제 전망, 처리일시

### 19. 콘텐츠 양방향 정렬 마무리 baseline (2026-03-29)
- 파일:
  - `services/content_sync.py`
  - `entrypoints/cli.py`
  - `agent.py`
  - `test_agent.py`
  - `README.md`
  - `handoffs/content-sync-report-2026-03-29.md`
- 변경 내용:
  - 콘텐츠 공통 필드 비교/정규화 로직을 `services/content_sync.py`로 분리
  - 새 CLI 추가:
    - `--check-content-sync`
    - `--reconcile-content-sync`
  - `sync_sheets_from_notion()`을 구 스키마(`주제`, 빈 `채널`) 의존에서 제거하고 새 필드 기준 백필로 정리
  - Notion 속성값 우선, 본문 블록 파싱은 fallback으로만 사용
  - `only_sheet` 역반영 시 URL 기준 기존 Notion 페이지 재사용 후 속성만 반영
  - `field_conflict`는 자동 반영하지 않고 수동 검토 대상으로 유지
  - 행 수 차이만으로 시트를 지우는 destructive validate 로직을 제거하고 diff 점검 리포트 기반으로 전환
  - 콘텐츠 동기화 테스트 0단계 추가
- 현재 운영 blocker:
  - 런타임 `NOTION_API_KEY`는 유효하지만 `NOTION_DATABASE_ID=314883f1-56f5-809e-97ba-fa187bea7e2e`에 대해 `404 object_not_found`
  - `경제 전문가 DB`는 접근 가능하므로 토큰 자체 문제가 아니라 `경제 콘텐츠 DB` 공유/권한 문제일 가능성이 큼
  - 따라서 `--check-content-sync`는 현재 blocked 리포트를 남기고 정상 종료하도록 처리
  - Notion integration 정본은 `노션-개발`로 고정하고, legacy/archive 키는 운영 기준에서 제외

## 검증 결과
- AST parse: 통과
- `python3.10 agent.py --help`: 통과
- `python3.10 test_agent.py`: 통과
- 로컬 retry worker 시뮬레이션: 통과
  - 결과: `WORKER_OK {'queued': 0, 'requeued': 1, 'claimed': 1}`
 - 로컬 atomic queue 시뮬레이션: 통과
   - 결과: `OPS_OK {'queued': 0, 'requeued': 1, 'claimed': 1}`
- 로컬 due retry increment 시뮬레이션: 통과
  - 결과: `OPS_DUE_OK {'queued': 1, 'requeued': 0, 'claimed': 1} 2 forced retry failure`
- `python3.10 test_agent.py`: 통과
- `python3.10 agent.py --help`: 통과
- Round 5 패치 후 재검증:
  - `AST_OK_R5_FIX2`
  - `python3.10 agent.py --help`: 통과
  - `python3.10 test_agent.py`: 통과
- person lookup split 후 재검증:
  - `AST_OK_LOOKUP`
  - `python3.10 agent.py --help`: 통과
  - `python3.10 test_agent.py`: 통과
- briefing dispatch log 전환 후 재검증:
  - `AST_OK_BRIEFING_DB`
  - idempotent check: `BEFORE False / MARK1 True / AFTER1 True / MARK2 False / AFTER2 True`
  - `python3.10 agent.py --help`: 통과
  - `python3.10 test_agent.py`: 통과
- dedup split 후 재검증:
  - `AST_OK_DEDUP`
  - `python3.10 agent.py --help`: 통과
  - `python3.10 test_agent.py`: 통과
- person review SQLite 전환 후 재검증:
  - `AST_OK_REVIEW_SQLITE`
  - row readback:
    - `ONE {'name': '홍춘욱', 'affiliation': '프리즘투자자문', 'role': '대표', 'status': 'approved', 'note': 'ok', 'updated_at': '2026-03-10T10:00:00+09:00'}`
    - `ALL {'k1': {...}}`
  - `python3.10 agent.py --help`: 통과

## 현재 알려진 운영 이슈

### 1. Notion DNS/네트워크 간헐 실패
- 증상:
  - `httpx.ConnectError: [Errno 8] nodename nor servname provided, or not known`
- 영향:
  - `--check-non-econ-people`
  - `--queue-non-econ-review`
  가 간헐적으로 실데이터 조회 실패
- 메모:
  - 현재는 스택트레이스 대신 사용자용 실패 출력으로 완화
  - 코드 문제라기보다 환경/네트워크 이슈로 판단 중

### 2. 검토 대상 인물
- 최근 `healthcheck` 기준:
  - 비경제/카테고리 검토 필요 샘플: `오동석`, `성일관`
- 아직 실제 검토 큐 적재는 네트워크 안정화 후 재실행 필요

## Claude에게 특히 보고 싶은 포인트
1. `services/person_identity.py`의 분리가 충분한지
2. 인물 동일성 판단 기준이 과도하게 합치거나 놓칠 위험이 남아 있는지
3. `0.62 / 0.48` 신뢰도 기준이 실제 운영에서 타당한지
4. `services/people.py`의 `_upsert_person_core()` 구조가 실시간/배치 공통 코어로 충분한지
5. SQLite retry queue의 lease/claim 모델에서 남아 있는 race condition이 무엇인지
6. 전용 retry worker를 완전 분리 프로세스로 발전시킬 때 어떤 경계부터 나눠야 하는지
7. 지금 상태에서 다음으로 떼어내야 할 모듈이 무엇인지
8. `services/person_lookup.py`로 조회/캐시/후보축소 로직을 분리하는 것이 적절한지
9. report scheduler 상태 파일 대신 briefing log/SQLite를 단일 진실원천으로 삼는 편이 나은지
10. `report_scheduler_state.json`을 유지할지, `queue.sqlite3` 또는 briefing log 테이블로 흡수할지 최종 권고안

## Claude에게 같이 넘길 파일
- `CLAUDE_REVIEW_PROMPT.md`
- `FINAL_ALGORITHM_SPEC.md`
- `MASTER_DESIGN_PROPOSAL.md`
- `ROADMAP_CHECKLIST.md`
- `CLAUDE_CODE_HANDOFF.md`
- `agent.py`
- `services/people.py`
- `services/person_identity.py`
- `services/person_lookup.py`
- `services/dedup.py`
- `services/reports.py`
- `services/ops.py`
- `entrypoints/cli.py`
- `entrypoints/background_loops.py`
- `entrypoints/retry_worker.py`

## 권장 실행 확인 명령
- `python3.10 agent.py --help`
- `python3.10 test_agent.py`
- `python3.10 agent.py --healthcheck`
- `python3.10 agent.py --reconcile-people-sync`
- `python3.10 agent.py --run-retry-worker`

## 요청하는 리뷰 형식
- findings first
- 심각도 순
- 파일/라인 기준
- 위험 이유 설명
- 수정 방향 제안
