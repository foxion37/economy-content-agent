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
- `credentials.json`
- `failed_url_queue.sqlite3`
- `ops_events.jsonl`
- `person_review_memory.json`
- `backups/`

## 운영 파일

- 실제 launchd 파일: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent.plist`
- 실제 백업 launchd 파일: `/Users/barq/developer/projects/economy-content-agent/deploy/com.barq.economy-agent-backup.plist`
- launchd 설치 스크립트: `/Users/barq/developer/projects/economy-content-agent/deploy/install_launchd.sh`
- archive 삭제 체크리스트: `/Users/barq/developer/projects/economy-content-agent/deploy/ARCHIVE_CLEANUP_CHECKLIST.md`
