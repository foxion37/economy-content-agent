import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional


WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]


@dataclass(slots=True)
class ReportGenerationDeps:
    kst: object
    api_key: str
    genai_module: object
    logger: object
    rows_to_text: Callable[[list[dict]], str]
    indicators_to_text: Callable[[dict], str]


def generate_daily_briefing(
    rows: list[dict],
    indicator_data: dict,
    highlights_text: str,
    deps: ReportGenerationDeps,
) -> str:
    if not rows:
        return "📭 집계 구간(전날 08:00~당일 07:50)에 수집된 영상이 없습니다."

    now_kst = datetime.now(deps.kst)
    today_str = now_kst.strftime("%Y년 %m월 %d일")
    weekday_ko = WEEKDAY_KO[now_kst.weekday()]
    rows_text = deps.rows_to_text(rows)
    indicator_text = deps.indicators_to_text(indicator_data)

    prompt = f"""당신은 10년 경력의 증권사 리서치 애널리스트인데, 오늘 아침 친한 친구에게 시장 상황을 카톡으로 알려주는 상황입니다.
전문 지식은 있지만 딱딱하지 않고, 어려운 용어는 쉽게 풀어서 설명하는 친근한 말투로 작성하세요.
(예: "오늘 시장은 좀 긴장된 분위기예요", "이 분야 전문가들이 눈독 들이고 있는 곳이에요")

아래는 데일리 집계 구간(전날 08:00~당일 07:50, 기준일 {today_str})의 한국 경제·금융 유튜브 콘텐츠 분석 데이터와 실시간 경제 점검지표입니다.

━━━ 유튜브 콘텐츠 분석 ━━━
{rows_text}

━━━ 경제 점검지표 (실시간) ━━━
{indicator_text}

━━━ 자동 하이라이트(빈도 기반) ━━━
{highlights_text or "없음"}

위 두 가지 데이터를 종합하여 텔레그램 채널 발송용 오전 데일리 브리핑을 작성하세요.
이모지로 섹션을 구분하고, 섹션 제목에 <b>태그</b>를 사용하며, 단락 사이에 빈 줄을 넣어 가독성을 높여주세요.
반드시 아래 5개 섹션을 순서대로 포함하고, 각 섹션은 빈 줄로 구분하세요.
첫 줄은 반드시 섹션 1 제목으로 시작하세요. 인사말/오프닝 문장은 금지입니다.

⚠️ 마크다운(*, **, ##, ###) 문법은 절대 사용하지 마세요. HTML 태그(<b>, <i>)만 사용하세요.
⚠️ 불렛 기호는 반드시 '-'만 사용하세요. '*'는 절대 사용 금지입니다.
⚠️ &amp;, &lt;, &gt; 같은 HTML 특수문자 이스케이프를 올바르게 처리하세요.
⚠️ 신호등 표시는 반드시 이 3가지만 사용: 초록🟢 / 노랑🟡 / 빨강🔴

1. 🚦 <b>시장 분위기 신호등</b>
   - 미국: 초록🟢/노랑🟡/빨강🔴 중 하나와 한 줄 이유
   - 한국: 초록🟢/노랑🟡/빨강🔴 중 하나와 한 줄 이유
   - 2~4문장으로 짧게

2. 📈 <b>주요 지표</b>
   - 지표별로 신호등(🟢/🟡/🔴)을 먼저 표시한 뒤 수치 작성
   - 미국/한국 주요 지수 포함 (예: S&P500, NASDAQ, KOSPI, KOSDAQ)
   - 환율(USD/KRW)은 반드시 2개 비교 포함:
     1) 전일 대비
     2) 1년 기준(기준값/평균값이 있으면 그것과 비교)
   - 형식 예시:
     - 환율(USD/KRW) 🟡: 1,350원 (전일 1,342원 대비 +8원 ▲, 1년 기준 1,280원 대비 +70원)

3. 🔭 <b>전문가 시장 전망</b>
   - 문장형 설명 금지. 반드시 비율 중심 줄바꿈 형식으로 작성:
     - 📗 장기 상승: XX%
     - 📕 단기 하락/위험관리: XX%
     - 📘 중립/변동성 대응: XX%
   - 필요하면 마지막에 근거 1~2개만 짧은 불렛으로 추가

4. 🔥 <b>오늘 주목할 곳</b>
   - 반복 언급된 종목/섹터를 우선순위로 정리
   - 왜 주목되는지 한 줄 코멘트

5. 👥 <b>전문가 한마디</b>
   - 이 섹션은 반드시 마지막에 배치
   - 각 전문가별 2줄 형식:
     👤 이름 (소속/직책)
     💬 핵심 의견 한 줄

작성 언어: 한국어"""

    try:
        client = deps.genai_module.Client(api_key=deps.api_key)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        header = (
            "📊 <b>오늘의 경제 데일리 브리핑</b>\n"
            f"{today_str} ({weekday_ko})\n"
            "*지난 24시간 동안 올라온 경제 콘텐츠(리포트, 뉴스, 유튜브 등)을 분석한 데이터입니다.\n\n"
        )
        return header + response.text.strip()
    except Exception as exc:
        deps.logger.error(f"데일리 브리핑 생성 실패: {exc}")
        return f"❌ 브리핑 생성 실패: {exc}"


def generate_weekly_report(
    rows: list[dict],
    indicator_data: dict,
    deps: ReportGenerationDeps,
) -> str:
    if not rows:
        return "📭 이번 주 수집된 영상이 없습니다."

    now_kst = datetime.now(deps.kst)
    monday = now_kst - timedelta(days=now_kst.weekday())
    week_start = monday.strftime("%m월 %d일")
    week_end = now_kst.strftime("%m월 %d일")
    rows_text = deps.rows_to_text(rows)
    indicator_text = deps.indicators_to_text(indicator_data)

    prompt = f"""당신은 10년 경력의 증권사 리서치 애널리스트인데, 이번 주 시장을 돌아보며 친한 친구에게 카톡으로 정리해주는 상황입니다.
전문 지식은 있지만 딱딱하지 않고, 어려운 용어는 쉽게 풀어서 설명하는 친근한 말투로 작성하세요.
(예: "이번 주는 생각보다 변동이 컸어요", "전문가들 사이에서 이 종목이 계속 입에 오르내렸어요")

아래는 이번 주({week_start} ~ {week_end}) 수집된 한국 경제·금융 유튜브 콘텐츠 분석 데이터와 실시간 경제 점검지표입니다.

━━━ 유튜브 콘텐츠 분석 ━━━
{rows_text}

━━━ 경제 점검지표 (실시간) ━━━
{indicator_text}

위 두 가지 데이터를 종합하여 텔레그램 채널 발송용 주간 트렌드 리포트를 작성하세요.
이모지로 섹션을 구분하고, 섹션 제목에 <b>태그</b>를 사용하며, 단락 사이에 빈 줄을 넣어 가독성을 높여주세요.
반드시 아래 5개 섹션을 순서대로 포함하고, 각 섹션은 빈 줄로 구분하세요.

⚠️ 마크다운(*, **, ##, ###) 문법은 절대 사용하지 마세요. HTML 태그(<b>, <i>)만 사용하세요.
⚠️ 불렛 기호는 반드시 '-'만 사용하세요. '*'는 절대 사용 금지입니다.
⚠️ &amp;, &lt;, &gt; 같은 HTML 특수문자 이스케이프를 올바르게 처리하세요.

1. 🏆 <b>이번 주 핫 TOP 3</b>
   여러 영상에서 반복 언급된 섹터·종목 TOP 3와 각각 왜 주목받았는지 쉽게 설명해주세요.

2. 📋 <b>이번 주 전문가들이 눈여겨본 종목들</b>
   언급된 종목 전체를 '-' 불렛으로 정리하고, 각 종목 옆에 한 줄 코멘트를 달아주세요.

3. 🔮 <b>다음 주는 어떨까요?</b>
   점검지표(마스터 신호등·VIX·환율 등)와 전문가 의견을 종합해 다음 주 전망을 3~5문장으로 쉽게 설명해주세요.
   상승/하락/중립 비율도 함께 적어주세요. 예: 상승 60% / 하락 20% / 중립 20%

4. 🤝 <b>전문가들이 입 모아 한 말</b>
   여러 명이 공통적으로 강조한 테마나 메시지를 정리해주세요.

5. 📡 <b>이번 주 주요 지표 마무리</b>
   주요 지표(VIX, SPY, KOSPI, 환율 등)를 '-' 불렛으로 정리하세요.
   전일 수치가 있으면 반드시 비교 형식으로 표시하세요.
   예: - 환율: 1,465원 (전일 1,450원 대비 +15원 ▲)

작성 언어: 한국어"""

    try:
        client = deps.genai_module.Client(api_key=deps.api_key)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        header = (
            f"📅 <b>경제 콘텐츠 위클리</b>\n"
            f"{week_start} ~ {week_end}\n"
            f"──────────────────\n\n"
        )
        return header + response.text.strip()
    except Exception as exc:
        deps.logger.error(f"주간 리포트 생성 실패: {exc}")
        return f"❌ 주간 리포트 생성 실패: {exc}"


@dataclass(slots=True)
class DailySendDeps:
    logger: object
    kst: object
    check_runtime_keys: Callable[[], bool]
    read_sheet_rows: Callable[[], list[dict]]
    read_indicator_sheet: Callable[[], dict]
    filter_rows_by_date: Callable[[list[dict], datetime, datetime], list[dict]]
    build_daily_highlights: Callable[[list[dict]], str]
    generate_daily_briefing: Callable[[list[dict], dict, str], str]
    daily_briefing_reply_markup: Callable[[], Optional[dict]]
    send_telegram_channel_message: Callable[[str, Optional[dict]], bool]
    save_daily_briefing_log: Callable[[str, datetime, datetime, int, int, bool], None]
    save_daily_highlight_log: Callable[[datetime, datetime, str, int, str], None]


async def send_daily_briefing(deps: DailySendDeps) -> None:
    deps.logger.info("📨 데일리 브리핑 생성 시작...")
    if not deps.check_runtime_keys():
        deps.logger.error("❌ 데일리 브리핑 중단: API 키 사전체크 실패")
        return
    now_kst = datetime.now(deps.kst)
    start_kst = (now_kst - timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
    end_kst = now_kst.replace(hour=7, minute=50, second=0, microsecond=0)

    loop = asyncio.get_event_loop()
    rows, indicator_data = await asyncio.gather(
        loop.run_in_executor(None, deps.read_sheet_rows),
        loop.run_in_executor(None, deps.read_indicator_sheet),
    )
    filtered = deps.filter_rows_by_date(rows, start_kst, end_kst)
    deps.logger.info(f"  → 데일리 대상: {len(filtered)}개 영상 (전날 08:00~당일 07:50 KST), 지표 {len(indicator_data.get('indicators', []))}개")
    highlights = deps.build_daily_highlights(filtered)

    briefing = await loop.run_in_executor(None, deps.generate_daily_briefing, filtered, indicator_data, highlights)
    reply_markup = deps.daily_briefing_reply_markup()
    success = await loop.run_in_executor(None, deps.send_telegram_channel_message, briefing, reply_markup)
    await loop.run_in_executor(
        None,
        deps.save_daily_briefing_log,
        briefing,
        start_kst,
        end_kst,
        len(filtered),
        len(indicator_data.get("indicators", [])),
        success,
    )
    await loop.run_in_executor(None, deps.save_daily_highlight_log, start_kst, end_kst, highlights, len(filtered), "rule-based")

    if success:
        deps.logger.info("✅ 데일리 브리핑 발송 완료")
    else:
        deps.logger.error("❌ 데일리 브리핑 발송 실패")


@dataclass(slots=True)
class WeeklySendDeps:
    logger: object
    kst: object
    read_sheet_rows: Callable[[], list[dict]]
    read_indicator_sheet: Callable[[], dict]
    filter_rows_by_date: Callable[[list[dict], datetime, datetime], list[dict]]
    generate_weekly_report: Callable[[list[dict], dict], str]
    send_telegram_channel_message: Callable[[str], bool]


async def send_weekly_report(deps: WeeklySendDeps) -> None:
    deps.logger.info("📨 주간 트렌드 리포트 생성 시작...")
    now_kst = datetime.now(deps.kst)
    monday = now_kst - timedelta(days=now_kst.weekday())
    start_kst = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_kst = now_kst

    loop = asyncio.get_event_loop()
    rows, indicator_data = await asyncio.gather(
        loop.run_in_executor(None, deps.read_sheet_rows),
        loop.run_in_executor(None, deps.read_indicator_sheet),
    )
    filtered = deps.filter_rows_by_date(rows, start_kst, end_kst)
    deps.logger.info(f"  → 주간 대상: {len(filtered)}개 영상 (이번 주 월~금), 지표 {len(indicator_data.get('indicators', []))}개")

    report = await loop.run_in_executor(None, deps.generate_weekly_report, filtered, indicator_data)
    success = await loop.run_in_executor(None, deps.send_telegram_channel_message, report)

    if success:
        deps.logger.info("✅ 주간 리포트 발송 완료")
    else:
        deps.logger.error("❌ 주간 리포트 발송 실패")


@dataclass(slots=True)
class OpsInsightDeps:
    kst: object
    read_sheet_rows: Callable[[], list[dict]]
    parse_row_datetime: Callable[[dict], Optional[datetime]]
    load_failed_url_queue: Callable[[], None]
    failed_url_queue: dict


def build_ops_insight_text(deps: OpsInsightDeps) -> str:
    rows = deps.read_sheet_rows()
    now = datetime.now(deps.kst)
    last24 = 0
    experts: set[str] = set()
    sectors: dict[str, int] = {}
    for row in rows:
        dt = deps.parse_row_datetime(row)
        if not dt:
            continue
        if dt >= now - timedelta(hours=24):
            last24 += 1
            person = (row.get("출연자(소속/직책/이름)", "") or "").strip()
            if person:
                experts.add(person.split(",")[0].strip())
            for sector in (row.get("주요 섹터", "") or "").split(","):
                sector = sector.strip()
                if sector:
                    sectors[sector] = sectors.get(sector, 0) + 1
    deps.load_failed_url_queue()
    top_sectors = sorted(sectors.items(), key=lambda x: -x[1])[:3]
    top_sector_text = ", ".join(f"{k}({v})" for k, v in top_sectors) if top_sectors else "없음"
    return (
        "📌 운영 인사이트 스냅샷\n"
        f"- 최근 24시간 처리 건수: {last24}\n"
        f"- 최근 24시간 출연자 수: {len(experts)}\n"
        f"- 섹터 TOP3: {top_sector_text}\n"
        f"- 실패 URL 큐: {len(deps.failed_url_queue)}건"
    )
