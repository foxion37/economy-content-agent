# Claude Migration Review Prompt

아래 프로젝트의 마이그레이션 결과를 검수해줘.

## 검수 대상

- 운영 경로: `/Users/barq/developer/projects/economy-content-agent`
- archive 원본: `/Users/barq/developer/archive/Analyst_Opinion_Archive`
- 현재 커밋: `cfb7927`

## 이번 검수의 목적

- `Analyst_Opinion_Archive` 정본 코드가 Git 저장소 경로로 안전하게 흡수되었는지 확인
- Mac Mini 운영 기준으로 `launchd`, 백업, 복구, archive 삭제 타이밍이 안전한지 확인
- archive 삭제 전에 남아 있는 실무 리스크를 찾기

## 특히 봐줬으면 하는 항목

1. `run_agent.sh`가 launchd 환경에서도 `.venv`와 Python 3.10+를 안정적으로 찾는지
2. `.gitignore`가 비밀정보, SQLite, 런타임 상태 파일을 적절히 제외하는지
3. `deploy/backup_to_nas.sh`와 `deploy/restore_from_nas.sh`가 실제 복구에 충분한 파일을 다루는지
4. SQLite 백업 방식이 운영 중에도 충분히 안전한지
5. `deploy/com.barq.economy-agent.plist`와 `deploy/com.barq.economy-agent-backup.plist`가 실제 운영용으로 타당한지
6. `deploy/ARCHIVE_CLEANUP_CHECKLIST.md` 기준으로 archive 삭제 시점이 적절한지
7. `deploy/legacy/`로 옮긴 레거시 파일 외에 추가로 정리해야 할 문서/예시 파일이 있는지

## 꼭 봐야 하는 파일

- `run_agent.sh`
- `.gitignore`
- `README.md`
- `requirements.txt`
- `deploy/backup_to_nas.sh`
- `deploy/restore_from_nas.sh`
- `deploy/backup.env.example`
- `deploy/com.barq.economy-agent.plist`
- `deploy/com.barq.economy-agent-backup.plist`
- `deploy/install_launchd.sh`
- `deploy/uninstall_launchd.sh`
- `deploy/ARCHIVE_CLEANUP_CHECKLIST.md`
- `deploy/MACMINI_NAS_RUNBOOK.md`

## 현재 확인된 상태

- `com.barq.economy-agent` launchd job은 등록되어 있고 실행 중
- `com.barq.economy-agent-backup` launchd job은 등록되어 있고 스케줄 대기 상태
- 코드와 운영 파일은 Git 커밋 `cfb7927`에 반영됨
- 원본 `Analyst_Opinion_Archive`는 archive로 이동됨
- NAS 마운트는 현재 세션에서 확인되지 않아 수동 백업 성공 여부는 아직 미검증

## 원하는 답변 형식

- Blocking issues
- Runtime risks
- Backup and restore risks
- Cleanup risks
- Recommended final fixes
- Safe to delete archive? (Yes/No with reason)
