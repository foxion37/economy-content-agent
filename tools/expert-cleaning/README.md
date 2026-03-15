# Economy Expert 클렌징 파이프라인

## 1) 파일
- `clean_experts.py`: 데이터 클렌징 실행 스크립트

## 2) 입력 CSV 컬럼
최소 권장 컬럼:
- `id`
- `name`
- `affiliation`
- `title`
- `source_type` (`official`, `government`, `university`, `association`, `news`, `portal`, `blog`, `unknown`)
- `source_date` (`YYYY-MM-DD` 권장)
- `source_url`

※ 컬럼이 더 있어도 그대로 보존됩니다.

## 3) 실행 방법

샘플 데이터 생성:

```bash
python3 clean_experts.py --make-sample --input experts_raw.csv
```

클렌징 실행:

```bash
python3 clean_experts.py --input experts_raw.csv --outdir output
```

검수 민감도 조정(Top1-Top2 점수차 임계값):

```bash
python3 clean_experts.py --input experts_raw.csv --outdir output --margin-threshold 0.1
```

## 4) 출력 파일
- `output/experts_current.csv`
  - 사람별 최종 1건(현재값 후보)
- `output/experts_history.csv`
  - 동일 인물 후보 전체 + 점수/순위
- `output/experts_review_queue.csv`
  - 저신뢰/충돌(점수차 낮음) 케이스

## 5) 적용 알고리즘
- 이름 정규화: 이름 문자열에서 직책/호칭 제거
- 소속 표준화: 사전 매핑 + 유사도 보정
- 현재성 판단: 과거 신호(`전`, `former`, `前` 등) 감점
- 신뢰도/최신성 스코어링: 출처 유형 + 날짜 기반 점수
- 충돌 해결: 인물별 최고 점수 선택, 임계 이하 검수 큐 이동

## 6) 커스터마이징 포인트 (`clean_experts.py`)
- `ROLE_TOKENS`: 이름에서 제거할 직책 사전
- `PAST_SIGNALS`: 과거 여부 키워드
- `SOURCE_RELIABILITY`: 출처 신뢰도 가중치
- `AFFILIATION_CANONICAL`: 기관명 표준화 사전

