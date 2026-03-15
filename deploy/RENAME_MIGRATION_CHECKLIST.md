# Rename 마이그레이션 체크리스트

## 변경 범위
- `/Users/barq/developer/Analyst_Opinion_Archive` → `/Users/barq/developer/projects/economy-content-agent`
- 정본 코드를 Git 저장소 기준 경로로 흡수

## 사전 백업
- [ ] `.env`
- [ ] `credentials.json`
- [ ] `failed_url_queue.sqlite3`
- [ ] `ops_events.jsonl`
- [ ] `backups/`

## 순서
- [ ] `launchctl bootout`으로 에이전트 중지
- [ ] `/Users/barq/developer/projects/economy-content-agent` 상태 확인
- [ ] 정본 프로젝트를 Git 저장소 경로로 복사 또는 동기화
- [ ] `~/Library/LaunchAgents/com.barq.economy-agent.plist` 경로 수정
- [ ] `deploy/backup.env` 경로 수정
- [ ] NAS 백업 경로 수정
- [ ] `--help` 실행
- [ ] `--healthcheck` 실행
- [ ] 텔레그램 링크 1건 테스트
- [ ] 데일리 브리핑 1건 테스트

## 롤백
- [ ] 검증 완료 전 기존 `Analyst_Opinion_Archive`는 삭제하지 않음
