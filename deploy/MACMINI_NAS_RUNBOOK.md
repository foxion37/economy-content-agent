# Mac mini + NAS 운영 런북

## 목표
- `Mac mini`에서 경제 콘텐츠 에이전트를 24/7 실행
- `NAS`에 영속 데이터와 설정을 주기적으로 백업
- 장애 시 `Mac mini` 또는 다른 Mac에서 빠르게 복구

## 운영 원칙
- 실행 주체는 `Mac mini` 1대만 유지
- NAS는 `백업/복구 저장소`로 사용하고, 실시간 DB는 Mac mini 로컬에서 운영
- `launchd`로 프로세스를 자동 실행
- 백업은 `rsync` + `manifest` 방식으로 단순하게 유지

## 백업 대상
- `.env`
- `credentials.json`
- `failed_url_queue.sqlite3`
- `ops_events.jsonl`
- `backups/`
- `CLAUDE_CODE_HANDOFF.md`
- `FINAL_ALGORITHM_SPEC.md`
- `ROADMAP_CHECKLIST.md`

## 백업 제외 대상
- `/tmp/economy-agent.log`
- `/tmp/economy-agent.err.log`
- `__pycache__/`
- `.DS_Store`
- `.agent.py.swp`

## 권장 디렉토리 구조
Mac mini 로컬:

```text
/Users/barq/developer/projects/
  economy-content-agent
  linkbot
  quant-research
  home-dev-infra
```

NAS 마운트:

```text
/Volumes/NAS/projects-backups/
  economy-content-agent/
```

백업 결과:

```text
/Volumes/NAS/projects-backups/economy-content-agent/
  latest/
  snapshots/
    20260311-221500/
    20260312-030000/
```

## Mac mini 준비
1. 프로젝트를 Mac mini에 아래 경로로 복사
   - `/Users/barq/developer/projects/economy-content-agent`
2. `python3.10` 이상 설치 확인
3. `.env`와 `credentials.json` 배치
4. NAS를 고정 경로로 마운트
5. `launchd` plist 설치
6. 백업 스크립트를 별도 `launchd`로 1일 1회 이상 실행

## 권장 launchd 구성
- 앱 실행: `com.barq.economy-agent`
- 백업 실행: `com.barq.economy-agent-backup`

## 일일 운영 체크
```zsh
tail -n 80 /tmp/economy-agent.log
tail -n 80 /tmp/economy-agent.err.log
cd /Users/barq/developer/projects/economy-content-agent && ./run_agent.sh
```

## 복구 순서
1. `launchctl bootout`으로 실행 중인 에이전트 중지
2. NAS 최신 스냅샷을 로컬 프로젝트에 복원
3. `.env`, `credentials.json`, `failed_url_queue.sqlite3`, `ops_events.jsonl` 우선 확인
4. `--healthcheck` 실행
5. `launchctl bootstrap`으로 재기동

## 권장 백업 주기
- `03:10 KST` 전체 백업 1회
- 필요 시 `12:10 KST` 추가 백업 1회

## 다음 단계
- NAS 경로 확정
- 백업용 `launchd` 등록
- Mac mini에서 `projects` 기준 동일 경로/동일 Python 3.10+ 버전 보장
