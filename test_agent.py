"""
agent.py 테스트 스크립트
1단계: _build_notion_body_blocks 로직 검증 (API 불필요)
2단계: 실제 YouTube URL → Claude 분석 → 노션 저장 (API 필요)
"""

import json
import os
import sys

# ── 0단계: 콘텐츠 동기화 로직 테스트 ─────────────────────
def test_content_sync_logic():
    print("=" * 60)
    print("0단계: 콘텐츠 동기화 로직 테스트")
    print("=" * 60)

    try:
        from agent import _content_record_from_sheet_row, _content_record_to_sheet_row
        from services.content_sync import ContentSyncRecord, classify_sync
    except ModuleNotFoundError as e:
        print(f"  ⚠️ 의존성 모듈 없음으로 0단계 스킵: {e}")
        return None

    row = {
        "유튜브 URL": "https://youtu.be/example123",
        "영상 제목": "테스트 영상",
        "채널명": "테스트채널",
        "주제(해시태그)": "#금리 #달러",
        "한줄 요약": "요약",
        "출연자(소속/직책/이름)": "테스트증권 / 애널리스트 / 홍길동",
        "인물 의견": "의견",
        "언급 상품": "QQQ, TLT",
        "주요 섹터": "반도체, 금융",
        "경기 전망": "중립: 변동성 대응",
        "처리일시": "2026-03-29 15:00",
    }

    record = _content_record_from_sheet_row(row)
    all_ok = True
    if not record:
        print("  ❌ 시트 행 매핑 실패")
        return False

    checks = [
        ("채널명", record.channel == "테스트채널"),
        ("해시태그", record.hashtags == "#금리 #달러"),
        ("언급 상품", record.mentioned_products == "QQQ, TLT"),
        ("핵심 섹터", record.key_sectors == "반도체, 금융"),
        ("경제 전망", record.economic_outlook == "중립: 변동성 대응"),
    ]
    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")
        if not ok:
            all_ok = False

    row_values = _content_record_to_sheet_row(record)
    layout_ok = row_values[2] == "테스트채널" and row_values[7] == "QQQ, TLT" and row_values[9] == "중립: 변동성 대응"
    print(f"  {'✅' if layout_ok else '❌'} 시트 행 순서")
    if not layout_ok:
        all_ok = False

    notion_same = ContentSyncRecord(
        url=record.url,
        title=record.title,
        channel="테스트채널",
        hashtags="#달러 #금리",
        summary=record.summary,
        person_str=record.person_str,
        opinion=record.opinion,
        mentioned_products="TLT, QQQ",
        key_sectors="금융, 반도체",
        economic_outlook=record.economic_outlook,
        timestamp=record.timestamp,
        source="notion",
        source_ref="page-1",
    )
    notion_conflict = ContentSyncRecord(
        url="https://youtu.be/conflict1",
        title="충돌 영상",
        channel="채널A",
        hashtags="#주식",
        summary="A요약",
        person_str="A",
        opinion="A의견",
        mentioned_products="SPY",
        key_sectors="기술",
        economic_outlook="상승",
        timestamp="2026-03-29 15:01",
        source="notion",
        source_ref="page-2",
    )
    sheet_conflict = ContentSyncRecord(
        url="https://youtu.be/conflict1",
        title="충돌 영상",
        channel="채널B",
        hashtags="#주식",
        summary="B요약",
        person_str="A",
        opinion="A의견",
        mentioned_products="SPY",
        key_sectors="기술",
        economic_outlook="상승",
        timestamp="2026-03-29 15:01",
        source="sheet",
        source_ref="https://youtu.be/conflict1",
    )
    sync_result = classify_sync(
        [notion_same, notion_conflict],
        [record, sheet_conflict],
    )

    class_ok = len(sync_result["in_sync"]) == 1 and len(sync_result["field_conflict"]) == 1
    print(f"  {'✅' if class_ok else '❌'} diff 분류")
    if not class_ok:
        all_ok = False

    return all_ok

# ── 1단계: 블록 생성 로직 테스트 ─────────────────────────
def test_block_builder():
    print("=" * 60)
    print("1단계: _build_notion_body_blocks 로직 테스트")
    print("=" * 60)

    # agent.py에서 함수만 임포트 (텔레그램 봇 실행 없이)
    try:
        from agent import _build_notion_body_blocks
    except ModuleNotFoundError as e:
        print(f"  ⚠️ 의존성 모듈 없음으로 1단계 스킵: {e}")
        return None

    mock_analysis = {
        "hashtags": ["#미국주식", "#금리전망", "#나스닥"],
        "summary": "연준의 금리 동결 가능성과 미국 기술주 투자 전략을 분석한다.",
        "person": {
            "name": "홍길동",
            "role": "수석 애널리스트",
            "affiliation": "삼성증권",
            "background": "삼성증권 리서치센터 출신. 미국 주식 전문가로 10년 경력을 보유하고 있으며 CNBC 등 주요 매체에 자주 출연한다.",
        },
        "key_summary": [
            "연준의 정책 변화가 성장주 밸류에이션에 직접 영향을 미친다.",
            "AI 관련 빅테크의 실적 모멘텀이 단기 조정 리스크를 상쇄한다.",
            "달러 약세 전환 시 위험자산 선호가 회복될 수 있다.",
        ],
        "economic_indicators": [
            {
                "indicator": "미국 기준금리",
                "value": "5.25%",
                "context": "동결 기조 유지",
                "timestamp": "3:45",
            }
        ],
        "market_momentum": {
            "positive": ["AI 수요 확대", "인플레이션 둔화"],
            "negative": ["밸류에이션 부담", "경기 둔화 우려"],
        },
        "market_outlook": "단기 변동성은 높지만 중기적으로는 상승 우위 흐름이 유효하다.",
        "recommended_assets": [
            {
                "name": "QQQ",
                "reason": "AI 관련 대형 기술주 비중이 높아 실적 모멘텀 수혜 기대",
                "target_or_risk": "밸류에이션 고점 리스크",
                "timestamp": "12:10",
            }
        ],
        "key_sectors": ["기술", "AI/반도체", "헬스케어"],
        "opinion": "기술주 중심의 포트폴리오를 유지하되, 금리 민감 섹터 비중을 점진적으로 높일 것을 권고한다.",
    }

    blocks = _build_notion_body_blocks(
        analysis=mock_analysis,
        verified_name="홍길동",
        verified_affiliation="삼성증권",
        role="수석 애널리스트",
    )

    section_headings = [
        b["heading_2"]["rich_text"][0]["text"]["content"]
        for b in blocks
        if b["type"] == "heading_2"
    ]

    expected = [
        "👤 출연자/강연자",
        "📋 핵심 요약",
        "📊 주요 경제 지표 및 수치",
        "📈 시장 모멘텀 분석",
        "🔮 전문가의 시장 전망",
        "🏭 주요 섹터",
        "💼 추천 및 긍정 언급 자산",
    ]

    all_ok = True
    for heading in expected:
        ok = heading in section_headings
        status = "✅" if ok else "❌"
        print(f"  {status} {heading}")
        if not ok:
            all_ok = False

    print(f"\n  총 블록 수: {len(blocks)}개")

    # 빈 데이터 처리 테스트
    print("\n  [빈 데이터 처리 테스트]")
    empty_analysis = {
        "summary": "",
        "person": {},
        "key_summary": [],
        "economic_indicators": [],
        "market_momentum": {},
        "market_outlook": "",
        "recommended_assets": [],
        "key_sectors": [],
        "opinion": "",
    }
    try:
        blocks_empty = _build_notion_body_blocks(
            analysis=empty_analysis,
            verified_name="미상",
            verified_affiliation="미상",
            role="미상",
        )
        print(f"  ✅ 빈 데이터 처리 성공 ({len(blocks_empty)}개 블록)")
    except Exception as e:
        print(f"  ❌ 빈 데이터 처리 실패: {e}")
        all_ok = False

    return all_ok


# ── 2단계: 실제 파이프라인 테스트 ────────────────────────
def test_full_pipeline(youtube_url: str):
    print("\n" + "=" * 60)
    print("2단계: 실제 파이프라인 테스트")
    print("=" * 60)

    # 환경변수 확인
    required_keys = ["NOTION_API_KEY", "YOUTUBE_API_KEY", "GEMINI_API_KEY"]
    missing = [k for k in required_keys if not os.environ.get(k)]
    if missing:
        print(f"  ⚠️  환경변수 없음, 건너뜀: {', '.join(missing)}")
        return None

    from agent import (
        get_notion_client,
        extract_video_id,
        fetch_youtube_metadata,
        fetch_transcript,
        analyze_with_gemini,
        verify_person,
        write_notion_result,
        NOTION_DATABASE_ID,
    )

    notion = get_notion_client()

    print(f"  URL: {youtube_url}")
    video_id = extract_video_id(youtube_url)
    if not video_id:
        print("  ❌ 유효하지 않은 유튜브 URL")
        return False

    print("  → YouTube 메타데이터 추출 중...")
    metadata = fetch_youtube_metadata(video_id)
    if not metadata:
        print("  ❌ 메타데이터 없음")
        return False
    print(f"     제목: {metadata['title']}")
    print(f"     채널: {metadata['channel']}")

    print("  → 자막 추출 중...")
    transcript = fetch_transcript(video_id)

    print("  → Gemini 분석 중...")
    analysis = analyze_with_gemini(metadata, transcript)
    print("  ✅ Gemini 분석 완료")
    print(f"     요약: {analysis.get('summary', '')}")
    print(f"     출연자: {analysis.get('person', {}).get('name', '미상')}")
    print(f"     핵심 요약 항목 수: {len(analysis.get('key_summary', []))}")
    print(f"     언급 상품: {analysis.get('mentioned_products', [])}")
    print(f"     섹터: {analysis.get('key_sectors', [])}")
    print(f"     시장 전망: {analysis.get('market_outlook', '')}")

    person = analysis.get("person", {})
    name = person.get("name", "미상")
    role = person.get("role", "미상")
    affiliation = person.get("affiliation", "미상")

    print("  → 노션 테스트 페이지 생성 중...")
    resp = notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties={
            "URL": {"url": youtube_url},
            "콘텐츠 제목": {"title": [{"text": {"content": "[TEST] " + metadata["title"]}}]},
        },
    )
    page_id = resp["id"]
    print(f"     페이지 ID: {page_id}")

    print("  → 노션 페이지에 결과 기록 중...")
    write_notion_result(
        notion=notion,
        page_id=page_id,
        video_title="[TEST] " + metadata["title"],
        analysis=analysis,
        verified_name=name,
        verified_affiliation=affiliation,
        role=role,
    )
    print("  ✅ 노션 저장 완료 (본문 블록 포함)")
    print(f"\n  노션 페이지: https://notion.so/{page_id.replace('-', '')}")
    return True


# ── 실행 ─────────────────────────────────────────────────
if __name__ == "__main__":
    step0_ok = test_content_sync_logic()
    step1_ok = test_block_builder()

    # 커맨드라인 인수로 YouTube URL 전달 시 2단계 실행
    # 예: python test_agent.py https://youtu.be/XXXXXXXXXXX
    if len(sys.argv) > 1:
        test_full_pipeline(sys.argv[1])
    else:
        print("\n  💡 2단계 실행: python test_agent.py <YouTube_URL>")

    print("\n" + "=" * 60)
    if step0_ok is None:
        print("0단계 결과: ⚠️ 스킵 (로컬 의존성 모듈 필요)")
    else:
        print("0단계 결과:", "✅ 통과" if step0_ok else "❌ 실패")
    if step1_ok is None:
        print("1단계 결과: ⚠️ 스킵 (로컬 의존성 모듈 필요)")
    else:
        print("1단계 결과:", "✅ 통과" if step1_ok else "❌ 실패")
    print("=" * 60)
