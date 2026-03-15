# Archive Cleanup Checklist

원본 정본 폴더였던 `Analyst_Opinion_Archive`를 보관 후 삭제할 때 사용하는 체크리스트입니다.

## 1. 보관 위치

- [x] 원본 폴더를 `/Users/barq/developer/archive/Analyst_Opinion_Archive`로 이동
- [ ] 이동 직후 원본 폴더와 새 저장소의 파일 개수/핵심 파일 존재 여부 확인
- [x] 삭제 전까지는 archive 사본을 최종 롤백 포인트로 유지

## 2. 새 경로 실행 검증

- [x] `/Users/barq/developer/projects/economy-content-agent/.venv/bin/python agent.py --help` 성공
- [x] `/Users/barq/developer/projects/economy-content-agent/.venv/bin/python agent.py --healthcheck` 실행
- [x] `/Users/barq/developer/projects/economy-content-agent/run_agent.sh` 실행 확인
- [x] `/tmp/economy-agent.log`에 새 경로 기준 시작 로그 확인
- [x] `.env`, `credentials.json`, `failed_url_queue.sqlite3`, `ops_events.jsonl`가 새 경로에 존재
- [x] `person_review_memory.json`이 새 경로에 존재

## 3. launchd 등록 검증

- [x] `~/Library/LaunchAgents/com.barq.economy-agent.plist` 등록
- [x] `~/Library/LaunchAgents/com.barq.economy-agent-backup.plist` 등록
- [x] `launchctl print gui/$(id -u)/com.barq.economy-agent` 확인
- [x] `launchctl print gui/$(id -u)/com.barq.economy-agent-backup` 확인

## 4. 운영 검증

- [x] 텔레그램 봇 연결 확인
- [x] 최소 1회 healthcheck 결과 확인
- [ ] 필요하면 테스트 링크 1건 처리 확인
- [ ] NAS 백업 스크립트 수동 1회 실행 확인
- [ ] `person_review_memory.json`이 NAS 백업 산출물에 포함되는지 확인

## 4A. NAS 연결 복구 후 해야 할 일

- [ ] NAS 마운트 경로 `/Volumes/NAS/projects-backups/economy-content-agent` 접근 가능 여부 확인
- [ ] `deploy/backup.env`의 `NAS_BACKUP_DIR` 값 재확인
- [ ] `zsh /Users/barq/developer/projects/economy-content-agent/deploy/backup_to_nas.sh` 수동 1회 실행
- [ ] `latest/`와 `snapshots/<timestamp>/` 생성 여부 확인
- [ ] `failed_url_queue.sqlite3` 백업 파일 존재 여부 확인
- [ ] `person_review_memory.json` 백업 파일 존재 여부 확인
- [ ] `ops_events.jsonl`, `.env`, `credentials.json`, `backups/` 포함 여부 확인
- [ ] 필요 시 `zsh /Users/barq/developer/projects/economy-content-agent/deploy/restore_from_nas.sh <snapshot_path>`로 복구 리허설
- [ ] 백업 성공 후 archive 삭제 여부 최종 판단

## 5. 삭제 조건

아래가 모두 완료되기 전에는 archive 사본을 삭제하지 않습니다.

- [x] 새 경로 실행이 최소 1회 이상 정상 동작
- [x] launchd 자동 실행이 정상 동작
- [ ] NAS 백업이 정상 동작
- [x] 복구에 필요한 핵심 파일이 새 경로에 모두 존재
- [ ] archive 삭제 전 최종 백업 완료

## 6. 최종 삭제

- [ ] `/Users/barq/developer/archive/Analyst_Opinion_Archive` 삭제
- [ ] 삭제 후 이 체크리스트에 삭제 날짜 기록

삭제 날짜:

- [ ] YYYY-MM-DD
