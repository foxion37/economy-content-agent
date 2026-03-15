# Projects 레이아웃 제안

## 최종 권장 구조
```text
/Users/barq/developer/projects/
  economy-content-agent
  linkbot
  quant-research
  home-dev-infra
```

## 이름 매핑
| 현재 | 변경 후 |
|------|---------|
| `/Users/barq/developer/Analyst_Opinion_Archive` | `/Users/barq/developer/projects/economy-content-agent` |
| `Analyst_Opinion_Archive` | `economy-content-agent` |
| `Linkbot` | `linkbot` |
| `Quant` | `quant-research` |
| `home-dev-infra` | `home-dev-infra` |

## NAS 권장 구조
```text
/Volumes/NAS/projects-backups/
  economy-content-agent/
  linkbot/
  quant-research/
  home-dev-infra/
```

## 운영 기준
- 실시간 실행: `economy-content-agent`
- 실시간 DB는 Mac mini 로컬 유지
- NAS는 백업/복구 저장소
