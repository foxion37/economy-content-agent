# economy-content-agent Agent Rules

이 프로젝트에서 작업하는 에이전트는 먼저 상위 공통 규칙을 확인한다.

- 공통 규칙: `/Users/barq/developer/AGENTS.md`

프로젝트 규칙:

- 이 프로젝트는 운영 중인 기준 프로젝트로 취급한다.
- 실행 경로, 배포 스크립트, `launchd` 연계 동작을 깨뜨릴 수 있는 루트 구조 변경은 신중하게 다룬다.
- 프로젝트 전용 비밀값은 이 디렉터리의 `.env` 또는 Git 제외 로컬 파일에 둔다.
- 백업, NAS, 런북 관련 문서는 `deploy/`와 `home-dev-infra/` 문서를 함께 확인한다.

작업 전 확인:

- 상위 `/Users/barq/developer/AGENTS.md`
- 프로젝트 `README.md`
- `/Users/barq/developer/projects/economy-content-agent/deploy/MACMINI_NAS_RUNBOOK.md`
- `/Users/barq/developer/home-dev-infra/plans/secrets-storage-manual.md`
