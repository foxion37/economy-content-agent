import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


@dataclass(slots=True)
class ReportLoopDeps:
    logger: object
    kst: object
    send_daily_briefing: Callable
    send_weekly_report: Callable
    was_briefing_sent: Callable[[str, str], bool]
    claim_briefing_dispatch: Callable[[str, str, str], bool]
    update_briefing_dispatch_status: Callable[[str, str, str, str], None]


async def report_scheduler(deps: ReportLoopDeps) -> None:
    deps.logger.info("🕐 리포트 스케줄러 시작 (매일 08:00 KST 데일리 / 금요일 08:00 KST 주간)")

    while True:
        try:
            now = datetime.now(deps.kst)
            date_key = now.strftime("%Y-%m-%d")
            week_key = now.strftime("%Y-%W")
            sent_at = now.isoformat()

            if now.hour == 8 and now.minute <= 2 and not deps.was_briefing_sent("daily", date_key):
                if deps.claim_briefing_dispatch("daily", date_key, sent_at):
                    try:
                        await deps.send_daily_briefing()
                        deps.update_briefing_dispatch_status(
                            "daily",
                            date_key,
                            "sent",
                            datetime.now(deps.kst).isoformat(),
                        )
                    except Exception:
                        deps.update_briefing_dispatch_status(
                            "daily",
                            date_key,
                            "failed",
                            datetime.now(deps.kst).isoformat(),
                        )
                        raise

            if now.weekday() == 4 and now.hour == 8 and now.minute <= 2 and not deps.was_briefing_sent("weekly", week_key):
                if deps.claim_briefing_dispatch("weekly", week_key, sent_at):
                    try:
                        await deps.send_weekly_report()
                        deps.update_briefing_dispatch_status(
                            "weekly",
                            week_key,
                            "sent",
                            datetime.now(deps.kst).isoformat(),
                        )
                    except Exception:
                        deps.update_briefing_dispatch_status(
                            "weekly",
                            week_key,
                            "failed",
                            datetime.now(deps.kst).isoformat(),
                        )
                        raise
        except Exception as exc:
            deps.logger.error(f"스케줄러 오류: {exc}")

        await asyncio.sleep(30)


@dataclass(slots=True)
class PeopleLoopDeps:
    logger: object
    kst: object
    people_light_interval_min: int
    people_sync_on_start: bool
    people_dawn_enabled: bool
    people_dawn_hour: int
    people_dawn_minute: int
    run_light: Callable[[], None]
    run_full: Callable[[], None]


async def people_maintenance_scheduler(deps: PeopleLoopDeps) -> None:
    if deps.people_light_interval_min <= 0 and not deps.people_sync_on_start and not deps.people_dawn_enabled:
        deps.logger.info("👥 인물 자동 클렌징 비활성화 (on_start=0, interval<=0, dawn=0)")
        return

    deps.logger.info(
        f"👥 인물 자동 클렌징 스케줄러 시작 "
        f"(on_start={deps.people_sync_on_start}, light_interval={deps.people_light_interval_min}분, "
        f"dawn={deps.people_dawn_enabled} {deps.people_dawn_hour:02d}:{deps.people_dawn_minute:02d} KST)"
    )
    loop = asyncio.get_event_loop()
    last_dawn_key = ""
    next_interval_run = time.time() + max(deps.people_light_interval_min, 1) * 60 if deps.people_light_interval_min > 0 else None

    if deps.people_sync_on_start:
        try:
            await loop.run_in_executor(None, deps.run_light)
        except Exception as exc:
            deps.logger.error(f"인물 자동 클렌징(시작 시 경량 1회) 실패: {exc}", exc_info=True)

    while True:
        try:
            now = datetime.now(deps.kst)
            if deps.people_dawn_enabled and now.hour == deps.people_dawn_hour and now.minute == deps.people_dawn_minute:
                date_key = now.strftime("%Y-%m-%d")
                if date_key != last_dawn_key:
                    await loop.run_in_executor(None, deps.run_full)
                    last_dawn_key = date_key

            if next_interval_run is not None and time.time() >= next_interval_run:
                await loop.run_in_executor(None, deps.run_light)
                next_interval_run = time.time() + max(deps.people_light_interval_min, 1) * 60
        except Exception as exc:
            deps.logger.error(f"인물 자동 클렌징(주기 실행) 실패: {exc}", exc_info=True)
        await asyncio.sleep(30)


@dataclass(slots=True)
class FailedRetryLoopDeps:
    logger: object
    enabled: bool
    failed_url_retry_interval_min: int
    retry_once: Callable[[], dict]


async def failed_retry_scheduler(deps: FailedRetryLoopDeps) -> None:
    if not deps.enabled:
        deps.logger.info("🔁 실패 URL 재시도 스케줄러 비활성화")
        return
    deps.logger.info(f"🔁 실패 URL 재시도 스케줄러 시작 ({deps.failed_url_retry_interval_min}분)")
    loop = asyncio.get_event_loop()
    while True:
        try:
            stats = await loop.run_in_executor(None, deps.retry_once)
            if stats.get("requeued", 0) > 0:
                deps.logger.info(f"🔁 실패 URL 재주입: {stats}")
        except Exception as exc:
            deps.logger.error(f"실패 URL 재시도 스케줄러 오류: {exc}", exc_info=True)
        await asyncio.sleep(max(deps.failed_url_retry_interval_min, 1) * 60)


@dataclass(slots=True)
class OpsLoopDeps:
    logger: object
    enabled: bool
    interval_min: int
    chat_id: str | None
    build_text: Callable[[], str]
    send_message_to_chat: Callable[[str, str, object], object]
    send_review_message: Callable[[str], object]


async def ops_insight_scheduler(deps: OpsLoopDeps) -> None:
    if not deps.enabled:
        deps.logger.info("📌 운영 인사이트 스케줄러 비활성화")
        return
    deps.logger.info(f"📌 운영 인사이트 스케줄러 시작 ({deps.interval_min}분)")
    loop = asyncio.get_event_loop()
    while True:
        try:
            text = await loop.run_in_executor(None, deps.build_text)
            deps.logger.info(text.replace("\n", " | "))
            if deps.chat_id:
                await loop.run_in_executor(None, deps.send_message_to_chat, deps.chat_id, text, None)
            else:
                await loop.run_in_executor(None, deps.send_review_message, text)
        except Exception as exc:
            deps.logger.error(f"운영 인사이트 스케줄러 오류: {exc}", exc_info=True)
        await asyncio.sleep(max(deps.interval_min, 30) * 60)


@dataclass(slots=True)
class PollerDeps:
    notion_client_factory: Callable[[], object]
    get_unprocessed_pages: Callable[[object], list[dict]]
    process_page: Callable[[object, dict], bool]
    dequeue_failed_url: Callable[[str], None]
    enqueue_failed_url: Callable[[str, str], None]
    poll_interval: int


async def notion_poller(deps: PollerDeps) -> None:
    notion = deps.notion_client_factory()
    loop = asyncio.get_event_loop()

    while True:
        try:
            pages = deps.get_unprocessed_pages(notion)
            if pages:
                print(f"📋 미처리 페이지 {len(pages)}개 발견")
                for page in pages:
                    try:
                        notion.pages.update(
                            page_id=page["id"],
                            properties={"주제": {"rich_text": [{"text": {"content": "처리 중..."}}]}},
                        )
                    except Exception as exc:
                        print(f"  ⚠️ 처리 중 표시 실패: {exc}")
                    result = await loop.run_in_executor(None, deps.process_page, notion, page)
                    page_url = page.get("properties", {}).get("URL", {}).get("url") or ""
                    if result:
                        deps.dequeue_failed_url(page_url)
                    elif page_url:
                        deps.enqueue_failed_url(page_url, "notion_poller_process_failed")
                    await asyncio.sleep(2)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 대기 중...", end="\r")
        except Exception as exc:
            print(f"\n⚠️ 폴러 오류: {exc}")

        await asyncio.sleep(deps.poll_interval)
