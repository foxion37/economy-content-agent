# Master Design Proposal (for multi-user productization)

Updated: 2026-03-10 (KST)

## 1) 데일리 브리핑 채널 정보 설계
- 핵심 원칙: "행동 가능성" 우선 (읽고 바로 판단 가능한 정보)
- 고정 섹션:
  1. 시장 신호등(미국/한국)
  2. 지표 요약(신호 + 전일 대비 + 환율 1일/1년 비교)
  3. 전문가 전망 비율(상승/하락/중립)
  4. 오늘 하이라이트(반복 빈도 기반 TOP)
  5. 전문가 한마디(최대 3명)
- 품질 지표:
  - 버튼 반응률(좋아요/아쉬워요)
  - 아쉬워요 비율이 높은 날의 공통 패턴
  - 다음날 유지된 시그널 적중률

## 2) 봇 주도 유튜브 탐색 설계
- 인입 경로 분리:
  - A. 사용자 링크 인입(기존)
  - B. 자동 발견 인입(신규)
- 자동 발견 파이프라인:
  1. 채널 seed 목록 + 키워드 검색
  2. 메타데이터만 먼저 수집(저토큰)
  3. 우선순위 점수 계산(거시/정책/시장 민감도, 신규성)
  4. 상위 N개만 자막+분석
  5. 기존 DB와 URL/제목/의미 중복 제거

## 3) GitHub/백업 데이터 관리
- 레벨1: 운영 로그(일일 rotate)
- 레벨2: 정제 데이터 스냅샷(JSON/CSV)
- 레벨3: 코드/설정 Git 관리
- 권장 운영:
  - private repo + secrets 제외(.env, credentials.json)
  - 주 1회 스냅샷 커밋 + 월 1회 태그
  - 복구 절차 문서(백업에서 복원 명령 포함)

## 4) FE/BE 구조 점검/수정 제안
- 현재 단일 스크립트 구조를 단계적으로 분리:
  - ingest (telegram/notion poller)
  - analysis (gemini + parsing)
  - entity (person db + reconcile)
  - briefing (daily/weekly generator)
  - bot interface (commands/review workflows)
- 2026-03-10 현재 반영 상태:
  - `entrypoints/cli.py`
  - `entrypoints/bot_runtime.py`
  - `entrypoints/background_loops.py`
  - `adapters/{youtube_client,telegram_gateway,gemini_client,notion_content_repo,sheets_repo}.py`
  - `services/reports.py`
  - `services/people.py`
- 다음 분리 우선순위:
  - person dedupe/enrichment helper 세분화
  - queue/worker 추상화
  - observability/alert 모듈
- 2026-03-10 운영 기본선 반영:
  - 실패 URL 큐 로직 `services/ops.py` 분리
  - 운영 이벤트 로그 `ops_events.jsonl` 추가
  - `healthcheck` 결과 이벤트 적재
  - 실패 URL 큐 저장소를 SQLite로 전환 (`queue.sqlite3`)
  - retry worker에 lease/claim 기반 배치 처리 추가
- 데이터 경계:
  - source-of-truth: Notion person page + sheet log
  - derived cache: runtime memory/json backups
- 장애 대응:
  - retry queue + dead-letter queue
  - healthcheck loop + alerting

### Retry Worker Baseline
- 저장소: SQLite (`queue.sqlite3`)
- 안전장치:
  - `BEGIN IMMEDIATE` 트랜잭션
  - `lease_owner`, `lease_until` 기반 claim
  - due item만 worker가 선점 후 처리
  - 성공 시 삭제, 실패 시 backoff 재예약
- 현재 범위:
  - 단일 프로세스 + 다중 재시도 호출 안전성 확보
  - 향후 확장:
    - dedicated worker process
    - dead-letter queue
    - retry reason analytics

## 5) Claude 코드 검수 요청 패키지
- 전달 파일:
  - ROADMAP_CHECKLIST.md
  - MASTER_DESIGN_PROPOSAL.md
  - agent.py diff summary
- 요청 포인트:
  - confidence scoring 기준 타당성
  - 사람-엔터티 dedupe 알고리즘
  - briefing post-validator rule

## 6) 상호 토론 기반 최종 알고리즘 확정
- 방식:
  1. 내가 baseline 제안
  2. Claude 반론/대안 수집
  3. 충돌 항목을 A/B 룰로 병행 실험
  4. KPI 기준으로 1안 고정
- 확정 문서:
  - FINAL_ALGORITHM_SPEC.md (입력/처리/출력/예외/운영지표)

## 7) 누구나 쓰는 UI 설계
- MVP 화면:
  - 오늘 브리핑
  - 인물 DB 탐색/수정
  - 불확실 인물 inbox
  - 피드백 대시보드
- UX 원칙:
  - 수동 수정은 버튼 기반으로만 상태 전이
  - 실수 방지(확정 전 미리보기 + 취소)
  - 역할별 권한(Admin/Reviewer/Viewer)

## 8) 배포 준비 (Mac mini/NAS/외부 서버)
- 1차: Mac mini launchd + healthcheck loop + 백업 cron
- 2차: Docker Compose (app + scheduler + reverse proxy)
- 3차: 외부 서버 이전 시
  - secrets manager
  - TLS + firewall
  - 모니터링(로그/메트릭/알림)
- 의사결정 기준:
  - 가용성, 운영 난이도, 월 비용, 복구 시간(RTO)
