import argparse
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


@dataclass(slots=True)
class CliActions:
    reprocess_all: Callable[[], None]
    dedup_all: Callable[[object], None]
    get_notion_client: Callable[[], object]
    sync_sheets_from_notion: Callable[[], None]
    send_daily_briefing: Callable[[], object]
    send_weekly_report: Callable[[], object]
    sync_people_from_notion: Callable[[], None]
    rebuild_people_db: Callable[[], None]
    check_person_db: Callable[[], None]
    check_people_sync_status: Callable[[], None]
    reconcile_people_sync: Callable[[], None]
    check_non_economic_people: Callable[[], None]
    queue_non_economic_people_review: Callable[[], None]
    clean_people_full: Callable[[], None]
    backfill_person_source_links: Callable[[], None]
    purge_people_without_youtube_source: Callable[[object], dict]
    test_person_flow: Callable[[str], None]
    enrich_all_people: Callable[[], None]
    check_runtime_keys: Callable[[], bool]
    run_healthcheck_once: Callable[[], None]
    run_retry_worker: Callable[[], None]
    send_telegram_channel_message: Callable[[str], bool]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="경제 콘텐츠 자동화 에이전트")
    parser.add_argument("--reprocess", action="store_true", help="노션 DB의 모든 URL 항목을 강제 재분석 (텔레그램 봇 미실행)")
    parser.add_argument("--dedup", action="store_true", help="노션 DB에서 중복 URL 페이지를 삭제 (오래된 것 1개 유지)")
    parser.add_argument("--sync-sheets", action="store_true", help="노션 DB의 완성된 항목을 구글 시트로 일괄 동기화 (텔레그램 봇 미실행)")
    parser.add_argument("--test-channel", action="store_true", help="텔레그램 채널에 테스트 메시지 발송 (HTML 포맷 확인용)")
    parser.add_argument("--send-daily", action="store_true", help="오늘자 데일리 브리핑을 즉시 생성·발송")
    parser.add_argument("--send-weekly", action="store_true", help="이번 주 주간 리포트를 즉시 생성·발송")
    parser.add_argument("--sync-people", action="store_true", help="기존 콘텐츠 DB 영상들을 인물 DB에 등록·연결 (텔레그램 봇 미실행)")
    parser.add_argument("--rebuild-people", action="store_true", help="인물 DB 중복 제거·병합 후 Economic_Expert 시트 전체 재구성")
    parser.add_argument("--test-person", metavar="URL", help="유튜브 URL로 인물 DB 전체 플로우를 단계별 테스트")
    parser.add_argument("--check-person-db", action="store_true", help="YouTube URL 없이 Notion 인물 DB 스키마·쓰기와 Google Sheets 쓰기를 단독 진단")
    parser.add_argument("--check-people-sync", action="store_true", help="Notion 인물 DB와 Economic_Expert 시트 불일치/링크누락/이름+직책 붙음 점검")
    parser.add_argument("--reconcile-people-sync", action="store_true", help="Notion 기준으로 시트 누락 보강 + 이름/소속/직책 정규화 보정")
    parser.add_argument("--check-non-econ-people", action="store_true", help="경제/투자/증권 비관련 인물 의심 항목 상세 점검")
    parser.add_argument("--queue-non-econ-review", action="store_true", help="비경제/허용 카테고리 외 인물 의심 항목을 텔레그램 검토 큐로 전송")
    parser.add_argument("--clean-people-full", action="store_true", help="인물 DB 전체 클렌징 1회 (동기화+중복정리+이름정리+시트재구성)")
    parser.add_argument("--backfill-source-links", action="store_true", help="인물 DB/시트의 근거 링크를 강제로 백필")
    parser.add_argument("--purge-people-no-youtube-source", action="store_true", help="근거 링크가 없거나 유튜브 링크가 아닌 인물을 Notion/시트에서 삭제")
    parser.add_argument("--enrich-people", action="store_true", help="인물 DB 전체 프로필 보강 (Google Search + 콘텐츠 DB 교차 참조 + Gemini 종합)")
    parser.add_argument("--check-keys", action="store_true", help="Gemini/Telegram API 키 유효성만 즉시 점검")
    parser.add_argument("--healthcheck", action="store_true", help="운영 점검 1회 실행 (키 유효성 + 인물 동기화 정합성)")
    parser.add_argument("--healthcheck-loop-min", type=int, default=0, help="healthcheck 반복 주기(분). 0이면 1회만 실행")
    parser.add_argument("--run-retry-worker", action="store_true", help="실패 URL 전용 retry worker만 실행")
    return parser


def run_cli(
    args: argparse.Namespace,
    actions: CliActions,
    *,
    kst,
    weekday_ko: dict[int, str],
) -> bool:
    if args.reprocess:
        print("🔄 강제 재처리 모드 시작\n")
        actions.reprocess_all()
    elif args.dedup:
        print("🧹 중복 제거 모드 시작\n")
        actions.dedup_all(actions.get_notion_client())
    elif args.sync_sheets:
        print("📊 구글 시트 동기화 모드 시작\n")
        actions.sync_sheets_from_notion()
    elif args.send_daily:
        asyncio.run(actions.send_daily_briefing())
    elif args.send_weekly:
        asyncio.run(actions.send_weekly_report())
    elif args.sync_people:
        print("👥 인물 DB 동기화 모드 시작\n")
        actions.sync_people_from_notion()
    elif args.rebuild_people:
        print("🔧 인물 DB 정리·재구성 모드 시작\n")
        actions.rebuild_people_db()
    elif args.check_person_db:
        actions.check_person_db()
    elif args.check_people_sync:
        actions.check_people_sync_status()
    elif args.reconcile_people_sync:
        actions.reconcile_people_sync()
    elif args.check_non_econ_people:
        actions.check_non_economic_people()
    elif args.queue_non_econ_review:
        actions.queue_non_economic_people_review()
    elif args.clean_people_full:
        actions.clean_people_full()
    elif args.backfill_source_links:
        actions.backfill_person_source_links()
    elif args.purge_people_no_youtube_source:
        notion = actions.get_notion_client()
        stats = actions.purge_people_without_youtube_source(notion)
        print(
            f"🧼 근거 유튜브 링크 기준 정리 완료: total={stats['total']}, "
            f"kept={stats['kept']}, purged={stats['purged']}"
        )
        if stats["samples"]:
            print(f"  · 삭제 샘플: {stats['samples']}")
    elif args.test_person:
        actions.test_person_flow(args.test_person)
    elif args.enrich_people:
        print("🔬 인물 DB 전체 보강 모드 시작\n")
        actions.enrich_all_people()
    elif args.check_keys:
        print("🔐 API 키 점검 시작\n")
        ok = actions.check_runtime_keys()
        print("\n✅ 키 점검 통과" if ok else "\n❌ 키 점검 실패")
    elif args.run_retry_worker:
        actions.run_retry_worker()
    elif args.healthcheck:
        if args.healthcheck_loop_min and args.healthcheck_loop_min > 0:
            print(f"🩺 Healthcheck loop 시작: {args.healthcheck_loop_min}분 간격")
            while True:
                actions.run_healthcheck_once()
                time.sleep(max(30, args.healthcheck_loop_min * 60))
        else:
            actions.run_healthcheck_once()
    elif args.test_channel:
        now_kst = datetime.now(kst)
        today_str = now_kst.strftime("%Y년 %m월 %d일")
        weekday = weekday_ko[now_kst.weekday()]
        test_msg = (
            f"📊 <b>경제 콘텐츠 데일리</b>\n"
            f"{today_str} ({weekday})\n"
            f"──────────────────\n\n"
            f"✅ <b>채널 발송 테스트</b>\n\n"
            f"HTML 포맷 확인 항목:\n"
            f"• <b>굵게</b> / <i>기울임</i>\n"
            f"• 이모지: 📊 🔥 👥 📈 💹\n"
            f"• <a href=\"https://youtube.com\">링크 텍스트</a>\n\n"
            f"📅 <b>경제 콘텐츠 위클리</b> 헤더 샘플\n"
            f"03월 01일 ~ 03월 07일\n"
            f"──────────────────"
        )
        ok = actions.send_telegram_channel_message(test_msg)
        print("✅ 발송 완료" if ok else "❌ 발송 실패")
    else:
        return False
    return True
