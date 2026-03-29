# Project Handoff

## 현재 목표

- Notion 경제 콘텐츠 DB와 Google Sheets 경제 콘텐츠 시트의 양방향 정렬을 운영 가능한 수준으로 마무리한다.
- 로컬 문서와 Notion 프로젝트 허브에 현재 상태, 리포트, 열린 이슈를 같은 기준으로 남긴다.

## 최근 완료 작업

- `services/content_sync.py` 추가
  - 콘텐츠 공통 필드 비교/정규화
  - `in_sync`, `only_notion`, `only_sheet`, `field_conflict` 분류
- 새 CLI 추가
  - `python agent.py --check-content-sync`
  - `python agent.py --reconcile-content-sync`
- `sync_sheets_from_notion()`을 새 필드 기준 백필 로직으로 정리
  - `주제` 의존 제거
  - `채널` 공백 하드코딩 제거
  - Notion 속성 우선 / 본문 블록 fallback 적용
- destructive validate 제거
  - 행 수 차이만으로 시트를 지우지 않고 diff 점검 리포트 기준으로 전환
- 로컬 검증 통과
  - `./.venv/bin/python -m py_compile agent.py entrypoints/cli.py services/content_sync.py test_agent.py`
  - `./.venv/bin/python test_agent.py`
  - `./.venv/bin/python agent.py --help`
- 콘텐츠 sync 점검 리포트 생성
  - `/Users/barq/developer/projects/economy-content-agent/handoffs/content-sync-report-2026-03-29.md`

## 다음 작업

- Notion `경제 콘텐츠` DB를 런타임 integration(`노션-개발`)에 다시 share한다.
- share 복구 후 `python agent.py --check-content-sync`를 재실행해 실제 diff를 생성한다.
- 실제 diff 결과를 기준으로 `field_conflict` 검토 큐 운영과 `--reconcile-content-sync` 반영 범위를 확인한다.

## 관련 파일

- `/Users/barq/developer/projects/economy-content-agent/services/content_sync.py`
- `/Users/barq/developer/projects/economy-content-agent/agent.py`
- `/Users/barq/developer/projects/economy-content-agent/entrypoints/cli.py`
- `/Users/barq/developer/projects/economy-content-agent/test_agent.py`
- `/Users/barq/developer/projects/economy-content-agent/handoffs/content-sync-report-2026-03-29.md`
- `/Users/barq/developer/projects/economy-content-agent/AGENTS.md`
- `/Users/barq/developer/projects/economy-content-agent/handoffs/ACTIVE.md`
- `/Users/barq/developer/projects/economy-content-agent/CLAUDE_CODE_HANDOFF.md`

## 주의사항

- handoff에는 비밀정보를 적지 않는다.
- 요약은 결정사항과 다음 액션 중심으로 유지한다.
- 프로젝트별 실제 문맥은 항상 프로젝트 내부 `handoffs/`에 둔다.
- 여러 에이전트가 동시에 같은 파일을 수정할 때는 충돌을 피하도록 먼저 수정 대상 파일을 명시한다.
- 현재 `NOTION_API_KEY`는 유효하지만 `NOTION_DATABASE_ID=314883f1-56f5-809e-97ba-fa187bea7e2e`는 `404 object_not_found` 상태다.
- `경제 전문가 DB`는 접근 가능하므로 토큰 자체 문제보다 `경제 콘텐츠 DB` 공유/권한 문제로 본다.
- Notion integration 정본은 `노션-개발`로 통일한다. archive/legacy `.env`에 남은 다른 integration 키는 사용하지 않는다.
- `field_conflict`는 자동 덮어쓰지 않는다.

## 검수 요청

- Claude 또는 다른 에이전트는 작업 시작 전에 `ACTIVE.md`를 먼저 확인한다.
- `경제 콘텐츠` DB 권한 복구 후 실제 diff 기준으로 충돌 정책이 충분한지 검토한다.
- `only_sheet` 역반영 시 relation/body 비자동화 범위가 운영상 적절한지 검토한다.

## 마지막 업데이트

- 2026-03-29 by Codex
