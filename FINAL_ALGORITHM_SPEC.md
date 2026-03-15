# Final Algorithm Spec

Updated: 2026-03-10 (KST)

## 1. 목적
- 경제 콘텐츠 입력을 안정적으로 수집한다.
- 콘텐츠 요약과 인물 엔터티를 누적형으로 관리한다.
- 중복/오탐/불확실 인물을 자동 감지하고 사람 검토로 연결한다.
- 데일리/위클리 브리핑을 운영 가능한 수준으로 자동 생성한다.

## 2. 입력
- 사용자 Telegram 유튜브 링크
- Notion 콘텐츠 DB 미처리 페이지
- 실패 URL 재시도 큐
- Google Sheets 콘텐츠 로그
- 경제 점검지표 시트

## 3. 콘텐츠 처리 파이프라인
1. URL 검증
2. YouTube 메타데이터 수집
3. 자막 수집
4. 댓글/채널 소개/최근 영상 제목 수집
5. 화자 힌트/반복 인물 힌트 추출
6. Gemini 분석
7. 인물 검증
8. Notion 콘텐츠 DB 반영
9. 인물 DB 반영
10. Google Sheet 반영
11. 실패 시 retry queue 적재

## 4. 인물 검증 우선순위
1. YouTube 메타데이터/설명/댓글/채널 정보/최근 영상 제목
2. 기존 Economic_Expert 시트와 Notion 인물 DB
3. Google 검색/SERP 기반 보강
4. 최종 불확실 시 Telegram 수동 검토

## 5. 인물 동일성 판단 기준
- 이름 일치
- 소속 유사도
- 직책 유사도
- 최근 채널/대표 채널 일치
- 발언 내용 유사도
- 기존 인물 페이지 본문 힌트
- Google/YouTube 외부 검색 신호

## 6. 신뢰도 규칙
- `PERSON_CONFIDENCE_MIN = 0.62`
  - 이 이상: 확정
- `PERSON_CONFIDENCE_STRICT_MIN = 0.48`
  - 0.48 이상 0.62 미만: 검토 필요
- 0.48 미만: 보류/삭제 후보

## 7. 불확실 인물 처리 규칙
- 조건 예시:
  - 이름/소속/직책 다수 미상
  - 동명이인 신호 존재
  - 경제/투자/증권 관련성 부족
  - 외부 근거 링크 부재 + 프로필 정보 빈약
- 처리:
  - `PERSON_UNCERTAIN_ACTION=notify` 기본
  - 승인 이력 있으면 재질문 생략
  - 미승인 시 Telegram 수동 검토 발송

## 8. 인물 DB 운영 원칙
- 적립형 운영 기본값 사용
  - `PEOPLE_ACCUMULATE_MODE=1`
- 자동 purge/rebuild 기본 비활성
- 새벽 배치에서:
  - 동기화
  - 누락 프로필 보강
  - 중복 병합
  - 본문 발언 dedupe

## 9. Sheet/Notion 동기화 원칙
- source of truth:
  - 콘텐츠: Notion + Sheet 로그 병행
  - 인물: Notion person page + Economic_Expert sheet
- reconcile 목적:
  - 누락 보강
  - 컬럼 정규화
  - 링크 누락 탐지
  - 이름/직책 붙음 오류 탐지

## 10. 브리핑 생성 원칙
- Daily:
  - 집계 구간: 전날 08:00 ~ 당일 07:50 KST
  - 발송 시각: 08:00 KST
- Weekly:
  - 이번 주 월~금 데이터
- 출력 구조:
  - 시장 신호등
  - 주요 지표
  - 전문가 전망 비율
  - 오늘 주목할 곳
  - 전문가 한마디

## 11. 실패 URL 큐
- 저장 위치: `queue.sqlite3` (레거시 `failed_url_queue.json` 자동 마이그레이션)
- 이벤트 로그: `ops_events.jsonl`
- 규칙:
  - 실패 시 큐 적재
  - `FAILED_URL_RETRY_INTERVAL_MIN` 후 재시도
  - `FAILED_URL_RETRY_MAX` 초과 시 하루 뒤 재시도
  - 완료/이미 처리됨이면 큐 제거
  - worker는 `lease_owner` / `lease_until` 기반으로 due item을 claim 후 처리

## 12. 운영 점검
- `--healthcheck`
  - Gemini 키 확인
  - Telegram 키 확인
  - Notion/Sheet 인물 정합성 점검
- 결과는 `ops_events.jsonl`에 기록

## 13. 남은 개선 과제
- dedicated retry worker 프로세스/서비스 분리
- person dedupe/enrichment helper 세분화
- 브리핑 post-validator 도입
- KPI 집계(반응률/신호 적중률) 자동화
