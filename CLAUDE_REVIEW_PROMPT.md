# Claude Review Prompt

아래 프로젝트를 코드 리뷰해줘. 목적은 "기능 추가"가 아니라 "현재 구조와 알고리즘이 실제 운영에 견딜 수 있는지" 검증하는 것이다.

## 프로젝트 성격
- 경제 콘텐츠 분석 자동화 에이전트
- 입력: Telegram 유튜브 링크, Notion 콘텐츠 DB, 실패 URL 재시도 큐
- 출력: Notion 콘텐츠 DB, Notion 인물 DB, Google Sheet, Telegram 데일리/위클리 브리핑

## 이번 리뷰의 우선순위
1. 인물 동일성 판단 규칙이 과도하게 공격적이거나 보수적이지 않은지
2. 신뢰도 점수 기준 `0.62 / 0.48`이 합리적인지
3. 실시간 처리(Telegram)와 배치 처리(sync/rebuild/maintenance) 결과가 달라질 위험이 남아 있는지
4. dedupe / reconcile / uncertain review workflow 사이 충돌 가능성이 있는지
5. SQLite retry queue의 lease/claim worker 모델이 실제로 안전한지
6. `services/person_identity.py` 분리가 충분한지, 아직 `agent.py`에 남아야 할 이유가 있는지
7. 현재 리팩터링 구조(`entrypoints`, `adapters`, `services`)가 유지보수 가능한지

## 꼭 봐야 하는 파일
- `agent.py`
- `services/people.py`
- `services/person_identity.py`
- `services/reports.py`
- `services/ops.py`
- `entrypoints/cli.py`
- `entrypoints/bot_runtime.py`
- `entrypoints/background_loops.py`
- `adapters/gemini_client.py`
- `adapters/notion_content_repo.py`
- `adapters/sheets_repo.py`
- `adapters/youtube_client.py`
- `ROADMAP_CHECKLIST.md`
- `MASTER_DESIGN_PROPOSAL.md`
- `FINAL_ALGORITHM_SPEC.md`

## 원하는 리뷰 형식
- Findings first
- 심각도 순 정렬
- 파일/라인 기준으로 지적
- “이건 왜 위험한지”를 구체적으로 설명
- 가능하면 수정 방향까지 제안

## 특히 답해줬으면 하는 질문
1. 인물 엔터티 merge 기준이 잘못 합칠 가능성이 큰가?
2. 불확실 인물 보류 기준이 충분히 설명 가능하고 운영 가능한가?
3. 콘텐츠 DB와 인물 DB의 relation 연결 로직이 배치/실시간 모두에서 일관적인가?
4. 현재 구조에서 다음으로 반드시 분리해야 할 모듈은 무엇인가?
5. 지금 상태의 SQLite lease/claim queue에서 가장 위험한 race condition은 무엇인가?
6. 비경제/허용 카테고리 검토 대상을 자동 보류/수동 검토로 보내는 현재 방향이 적절한가?
