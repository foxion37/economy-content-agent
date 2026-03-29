"""
경제 콘텐츠 자동화 에이전트
- 텔레그램으로 유튜브 링크 수신 → 노션 DB 저장 → 분석 후 텔레그램으로 결과 전송
- 노션 DB 폴링: url 컬럼이 있고 주제가 비어있는 행을 주기적으로 처리
- YouTube Data API로 제목/설명 추출
- Claude API로 분석 (주제, 요약, 출연자, 의견)
- Google Search로 인물명 오타 검증
"""

import os
import re
import logging
from typing import Optional
import time
import json
import asyncio
import httpx
import anthropic
import threading
import sys
from pathlib import Path
from difflib import SequenceMatcher
from dotenv import load_dotenv
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), ".env"),
    override=True,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

if sys.version_info < (3, 10):
    raise RuntimeError(
        f"Python 3.10+ 필요 (현재: {sys.version.split()[0]}). "
        "launchd/터미널 모두 python3.10으로 실행하세요."
    )

# \u2028(LINE SEPARATOR), \u2029(PARAGRAPH SEPARATOR) 등이 anthropic/httpx의
# ASCII 인코딩 경로를 통과할 때 오류를 일으키므로 데이터 수신 즉시 제거
def _clean(text: str) -> str:
    if not text:
        return text
    return (
        text
        .replace('\u2028', ' ')
        .replace('\u2029', ' ')
        .replace('\u200b', '')
        .replace('\ufeff', '')
    )

from notion_client import Client as NotionClient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
from google import genai
from telegram import Update
from telegram.ext import ContextTypes
from app.config import AppRuntimeConfig
from adapters.telegram_gateway import send_message_to_chat as adapter_send_message_to_chat
from adapters.gemini_client import analyze_with_gemini as adapter_analyze_with_gemini
from adapters.notion_content_repo import (
    create_page_from_url as adapter_create_page_from_url,
    get_unprocessed_pages as adapter_get_unprocessed_pages,
    write_result as adapter_write_notion_result,
)
from adapters.sheets_repo import (
    append_analysis_row as adapter_append_analysis_row,
    append_briefing_log as adapter_append_briefing_log,
    read_indicator_rows as adapter_read_indicator_rows,
    read_rows as adapter_read_rows,
)
from adapters.youtube_client import (
    extract_video_id as adapter_extract_video_id,
    fetch_channel_about as adapter_fetch_channel_about,
    fetch_channel_recent_video_titles as adapter_fetch_channel_recent_video_titles,
    fetch_transcript as adapter_fetch_transcript,
    fetch_youtube_comments as adapter_fetch_youtube_comments,
    fetch_youtube_metadata as adapter_fetch_youtube_metadata,
)
from domain.models import PageContext, PipelineResult, VideoJob
from entrypoints.background_loops import (
    FailedRetryLoopDeps,
    OpsLoopDeps,
    PeopleLoopDeps,
    PollerDeps,
    ReportLoopDeps,
    failed_retry_scheduler as entrypoint_failed_retry_scheduler,
    notion_poller as entrypoint_notion_poller,
    ops_insight_scheduler as entrypoint_ops_insight_scheduler,
    people_maintenance_scheduler as entrypoint_people_maintenance_scheduler,
    report_scheduler as entrypoint_report_scheduler,
)
from entrypoints.bot_runtime import build_bot_application, run_bot_runtime
from entrypoints.cli import CliActions, build_parser, run_cli
from entrypoints.telegram_handlers import handle_start_payload
from entrypoints.retry_worker import RetryWorkerDeps, run_retry_worker as entrypoint_run_retry_worker
from services.reports import (
    DailySendDeps,
    OpsInsightDeps as ReportOpsInsightDeps,
    ReportGenerationDeps,
    WeeklySendDeps,
    build_ops_insight_text as service_build_ops_insight_text,
    generate_daily_briefing as service_generate_daily_briefing,
    generate_weekly_report as service_generate_weekly_report,
    send_daily_briefing as service_send_daily_briefing,
    send_weekly_report as service_send_weekly_report,
)
from services.content_sync import (
    ContentSyncRecord,
    classify_sync as service_classify_content_sync,
    render_sync_report as service_render_content_sync_report,
)
from services.people import (
    ExpertSheetSaveDeps,
    PeopleVerificationDeps,
    PeopleMaintenanceDeps,
    ProcessPersonDeps,
    RebuildPeopleDeps,
    SyncPersonAndLinkDeps,
    SyncPeopleDeps,
    process_person_db as service_process_person_db,
    rebuild_people_db as service_rebuild_people_db,
    run_people_maintenance_light as service_run_people_maintenance_light,
    run_people_maintenance_once as service_run_people_maintenance_once,
    save_person_to_expert_sheet as service_save_person_to_expert_sheet,
    sync_person_and_link as service_sync_person_and_link,
    sync_people_from_notion as service_sync_people_from_notion,
    verify_person as service_verify_person,
)
from services.person_identity import (
    GeminiSamePersonDeps,
    NonEconomicSuspectsDeps,
    UncertainPersonDeps,
    gemini_verify_same_person as service_gemini_verify_same_person,
    is_uncertain_person as service_is_uncertain_person,
    non_economic_person_suspects as service_non_economic_person_suspects,
    score_person_confidence as service_score_person_confidence,
)
from services.person_lookup import (
    PersonLookupDeps,
    candidate_hint_score as service_candidate_hint_score,
    find_conflicting_candidates as service_find_conflicting_candidates,
    find_person_in_notion_db as service_find_person_in_notion_db,
    find_recent_person_match as service_find_recent_person_match,
    forget_person_match as service_forget_person_match,
    person_aff_key as service_person_aff_key,
    person_lookup_keys as service_person_lookup_keys,
    person_name_key as service_person_name_key,
    remember_person_match as service_remember_person_match,
)
from services.dedup import (
    PersonDedupeDeps,
    auto_dedup_people_db as service_auto_dedup_people_db,
    cluster_groups_by_edges as service_cluster_groups_by_edges,
    duplicate_score as service_duplicate_score,
    find_duplicate_person_groups_hybrid as service_find_duplicate_person_groups_hybrid,
    google_confirm_duplicate as service_google_confirm_duplicate,
    merge_person_group as service_merge_person_group,
    person_fingerprint as service_person_fingerprint,
)
from services.ops import (
    claim_briefing_dispatch as service_claim_briefing_dispatch,
    FailedQueueDeps,
    append_ops_event,
    dequeue_failed_url as service_dequeue_failed_url,
    enqueue_failed_url as service_enqueue_failed_url,
    get_person_review_row as service_get_person_review_row,
    load_failed_url_queue as service_load_failed_url_queue,
    load_person_review_rows as service_load_person_review_rows,
    retry_failed_urls_once as service_retry_failed_urls_once,
    save_failed_url_queue as service_save_failed_url_queue,
    save_person_review_row as service_save_person_review_row,
    update_briefing_dispatch_status as service_update_briefing_dispatch_status,
    was_briefing_sent as service_was_briefing_sent,
)
from services.trust_store import (
    bootstrap_person_trust_rows as service_bootstrap_person_trust_rows,
    ingest_claim_samples as service_ingest_claim_samples,
    load_claim_override_registry as service_load_claim_override_registry,
    init_trust_data_db as service_init_trust_data_db,
    load_claim_sample_docs as service_load_claim_sample_docs,
    load_person_trust_rows as service_load_person_trust_rows,
    load_review_queue_rows as service_load_review_queue_rows,
    load_symbol_mapping_registry as service_load_symbol_mapping_registry,
    refresh_claim_outcomes as service_refresh_claim_outcomes,
    recompute_person_trust_scores as service_recompute_person_trust_scores,
    save_claim_override_registry as service_save_claim_override_registry,
)
from workflows.content_pipeline import ContentPipelineDeps, run_content_pipeline

BASE_DIR = os.path.dirname(__file__)

# ── 설정 ────────────────────────────────────────────────
DEFAULT_NOTION_DATABASE_ID = "314883f1-56f5-809e-97ba-fa187bea7e2e"
DEFAULT_GOOGLE_SHEET_ID = "1UQZnkRUNAn1iGiLGtnqfMwEhYbnTBoKpr9J9Y51eYYw"
DEFAULT_INDICATOR_SHEET_ID = "1fMb1g5HEaAjDjJq5D_PtQEsmBeus4TXr6v8L-36bcp0"
DEFAULT_PERSON_DB_ID = "31a883f1-56f5-8075-a3ff-e8ee3d83d254"

def _ascii_env(key: str) -> Optional[str]:
    """환경변수에서 비-ASCII 문자(예: \\u2028 LINE SEPARATOR)를 제거하여 반환"""
    val = os.environ.get(key)
    if val is None:
        return None
    return val.encode("ascii", errors="ignore").decode("ascii")

NOTION_API_KEY     = _ascii_env("NOTION_API_KEY")
YOUTUBE_API_KEY    = _ascii_env("YOUTUBE_API_KEY")
ANTHROPIC_API_KEY  = _ascii_env("ANTHROPIC_API_KEY")
GEMINI_API_KEY     = _ascii_env("GEMINI_API_KEY")
GOOGLE_CSE_ID      = _ascii_env("GOOGLE_CSE_ID")
GOOGLE_CSE_API_KEY = _ascii_env("GOOGLE_CSE_API_KEY")
SERPER_API_KEY     = _ascii_env("SERPER_API_KEY")
EXPECTED_NOTION_BOT_NAME = (os.environ.get("EXPECTED_NOTION_BOT_NAME") or "노션-개발").strip() or "노션-개발"
NOTION_DATABASE_ID = _ascii_env("NOTION_DATABASE_ID") or DEFAULT_NOTION_DATABASE_ID
TELEGRAM_BOT_TOKEN  = _ascii_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = _ascii_env("TELEGRAM_CHANNEL_ID")
TELEGRAM_REVIEW_CHAT_ID = _ascii_env("TELEGRAM_REVIEW_CHAT_ID")
TELEGRAM_BOT_USERNAME = _ascii_env("TELEGRAM_BOT_USERNAME")
TELEGRAM_ADMIN_CHAT_IDS = set(
    x.strip() for x in (os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", "") or "").split(",") if x.strip()
)
REVIEW_ALERTS_ENABLED = (os.environ.get("REVIEW_ALERTS_ENABLED", "1").strip() not in ("0", "false", "False"))

POLL_INTERVAL = 60  # 초 단위 노션 폴링 간격
KST = timezone(timedelta(hours=9))  # 한국 표준시

GOOGLE_SHEET_ID = _ascii_env("GOOGLE_SHEET_ID") or DEFAULT_GOOGLE_SHEET_ID
INDICATOR_SHEET_ID = _ascii_env("INDICATOR_SHEET_ID") or DEFAULT_INDICATOR_SHEET_ID
DAILY_BRIEFING_SHEET_ID = _ascii_env("DAILY_BRIEFING_SHEET_ID") or GOOGLE_SHEET_ID
DAILY_BRIEFING_TAB = (os.environ.get("DAILY_BRIEFING_TAB") or "Daily_Briefing_Log").strip() or "Daily_Briefing_Log"
_DAILY_BRIEFING_HEADERS = [
    "기록시각", "집계시작", "집계종료", "영상수", "지표수", "발송상태", "피드백", "브리핑본문",
]
DAILY_HIGHLIGHT_TAB = (os.environ.get("DAILY_HIGHLIGHT_TAB") or "Daily_Highlights").strip() or "Daily_Highlights"
_DAILY_HIGHLIGHT_HEADERS = [
    "기록시각", "집계시작", "집계종료", "하이라이트", "영상수", "생성방식",
]
_SHEET_HEADERS = [
    "유튜브 URL", "영상 제목", "채널명", "주제(해시태그)", "한줄 요약",
    "출연자(소속/직책/이름)", "인물 의견", "언급 상품", "주요 섹터", "경기 전망", "처리일시",
]

PERSON_DB_ID = _ascii_env("PERSON_DB_ID") or DEFAULT_PERSON_DB_ID
EXPERT_SHEET_ID = _ascii_env("EXPERT_SHEET_ID") or GOOGLE_SHEET_ID
EXPERT_SHEET_TAB = (os.environ.get("EXPERT_SHEET_TAB") or "Economic_Expert").strip() or "Economic_Expert"
_EXPERT_SHEET_HEADERS = [
    "이름", "소속", "직책", "주요 경력", "전문 분야", "등장 횟수",
    "최근 발언일", "최근 채널", "최근 발언", "대표 채널", "채널 TOP3", "일관성 요약",
    "근거 링크", "신뢰도 점수", "신뢰도 상태", "닉네임",
]
EXPERT_SNAPSHOT_TAB = (os.environ.get("EXPERT_SNAPSHOT_TAB") or "Expert_Snapshot").strip() or "Expert_Snapshot"
_EXPERT_SNAPSHOT_HEADERS = [
    "person_id", "이름", "닉네임", "identity_status", "person_types",
    "소속", "직책", "대표 채널", "trust_score_total", "trust_score_band",
    "trust_score_confidence", "resolved_claim_count", "pending_claim_count",
    "direction_accuracy", "alpha_score", "source_transparency_score",
    "contradiction_flag_count", "last_trustscore_updated_at",
]
REVIEW_QUEUE_TAB = (os.environ.get("REVIEW_QUEUE_TAB") or "Review_Queue").strip() or "Review_Queue"
_REVIEW_QUEUE_HEADERS = [
    "queue_id", "created_at", "queue_type", "priority", "status",
    "target_type", "target_id", "target_name", "source_url", "evidence_summary",
    "suggested_action", "owner", "last_reviewed_at", "notes",
]
CONTENT_SYNC_REPORT_PATH = str(Path(BASE_DIR) / "handoffs" / f"content-sync-report-{datetime.now(KST).strftime('%Y-%m-%d')}.md")
CONTENT_PERSON_RELATION_PROP = os.environ.get("CONTENT_PERSON_RELATION_PROP", "인물").strip() or "인물"
PEOPLE_SYNC_ON_START = (os.environ.get("PEOPLE_SYNC_ON_START", "1").strip() not in ("0", "false", "False"))
PEOPLE_SYNC_INTERVAL_MIN = int(os.environ.get("PEOPLE_SYNC_INTERVAL_MIN", "5") or "5")
PEOPLE_LIGHT_INTERVAL_MIN = int(os.environ.get("PEOPLE_LIGHT_INTERVAL_MIN", str(PEOPLE_SYNC_INTERVAL_MIN)) or str(PEOPLE_SYNC_INTERVAL_MIN))
PEOPLE_DEDUP_INTERVAL_MIN = int(os.environ.get("PEOPLE_DEDUP_INTERVAL_MIN", "5") or "5")
PEOPLE_DAWN_ENABLED = (os.environ.get("PEOPLE_DAWN_ENABLED", "1").strip() not in ("0", "false", "False"))
PEOPLE_DAWN_HOUR = int(os.environ.get("PEOPLE_DAWN_HOUR", "3") or "3")
PEOPLE_DAWN_MINUTE = int(os.environ.get("PEOPLE_DAWN_MINUTE", "30") or "30")
PEOPLE_FULL_CLEAN_WEEKDAY = int(os.environ.get("PEOPLE_FULL_CLEAN_WEEKDAY", "0") or "0")
PEOPLE_ACCUMULATE_MODE = (os.environ.get("PEOPLE_ACCUMULATE_MODE", "1").strip() not in ("0", "false", "False"))
PEOPLE_PURGE_ON_MAINTENANCE = (os.environ.get("PEOPLE_PURGE_ON_MAINTENANCE", "0").strip() in ("1", "true", "True"))
PEOPLE_REBUILD_ON_MAINTENANCE = (os.environ.get("PEOPLE_REBUILD_ON_MAINTENANCE", "0").strip() in ("1", "true", "True"))
PERSON_UNCERTAIN_ACTION = (os.environ.get("PERSON_UNCERTAIN_ACTION", "notify").strip().lower() or "notify")
PERSON_DB_SCHEMA_CACHE_SEC = int(os.environ.get("PERSON_DB_SCHEMA_CACHE_SEC", "300") or "300")


def _sheet_end_col(header_count: int) -> str:
    idx = max(header_count, 1)
    chars: list[str] = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        chars.append(chr(ord("A") + rem))
    return "".join(reversed(chars))


EXPERT_SHEET_END_COL = _sheet_end_col(len(_EXPERT_SHEET_HEADERS))
EXPERT_SHEET_FULL_RANGE = f"'{EXPERT_SHEET_TAB}'!A:{EXPERT_SHEET_END_COL}"
EXPERT_SNAPSHOT_END_COL = _sheet_end_col(len(_EXPERT_SNAPSHOT_HEADERS))
EXPERT_SNAPSHOT_FULL_RANGE = f"'{EXPERT_SNAPSHOT_TAB}'!A:{EXPERT_SNAPSHOT_END_COL}"
REVIEW_QUEUE_END_COL = _sheet_end_col(len(_REVIEW_QUEUE_HEADERS))
REVIEW_QUEUE_FULL_RANGE = f"'{REVIEW_QUEUE_TAB}'!A:{REVIEW_QUEUE_END_COL}"
PERSON_DB_LIST_CACHE_SEC = int(os.environ.get("PERSON_DB_LIST_CACHE_SEC", "20") or "20")
PERSON_SYNC_SLEEP_SEC = float(os.environ.get("PERSON_SYNC_SLEEP_SEC", "0.2") or "0.2")
PERSON_CONFIDENCE_MIN = float(os.environ.get("PERSON_CONFIDENCE_MIN", "0.62") or "0.62")
PERSON_CONFIDENCE_STRICT_MIN = float(os.environ.get("PERSON_CONFIDENCE_STRICT_MIN", "0.48") or "0.48")

APP_RUNTIME_CONFIG = AppRuntimeConfig(
    notion_database_id=NOTION_DATABASE_ID,
    person_db_id=PERSON_DB_ID,
    google_sheet_id=GOOGLE_SHEET_ID,
    expert_sheet_id=EXPERT_SHEET_ID,
    telegram_channel_id=TELEGRAM_CHANNEL_ID or "",
    people_accumulate_mode=PEOPLE_ACCUMULATE_MODE,
    people_dawn_hour=PEOPLE_DAWN_HOUR,
    people_dawn_minute=PEOPLE_DAWN_MINUTE,
    person_confidence_min=PERSON_CONFIDENCE_MIN,
    person_confidence_strict_min=PERSON_CONFIDENCE_STRICT_MIN,
)

# 텔레그램 수동 수정 티켓: ticket_id -> {page_id, name, affiliation, role, created_at}
_pending_telegram: dict[str, dict] = {}
_person_form_sessions: dict[str, dict] = {}
_unknown_person_sessions: dict[str, dict] = {}
_ticket_review_sessions: dict[str, dict] = {}
_PERSON_DB_LOCK = threading.RLock()
_PERSON_NAME_ALIASES = {
    "홍박사": "홍춘욱",
    "홍춘욱 박사": "홍춘욱",
    "슈카": "전석재",
    "수페": "송민섭",
    "송림": "송팀장",
}
_PERSON_CANONICAL_OVERRIDES = {
    "슈카": "전석재",
    "수페": "송민섭",
}
_PERSON_ALIAS_ONLY_CANONICALS = {
    "송팀장",
}
_PERSON_CHANNEL_URL_OVERRIDES = {
    "송팀장": "https://www.youtube.com/@CapitalSong",
}
_PERSON_ALIAS_SEARCH_CACHE: dict[str, tuple[str, str]] = {}
_PERSON_RECENT_MATCH: dict[str, tuple[str, float]] = {}
PERSON_MATCH_COOLDOWN_SEC = int(os.environ.get("PERSON_MATCH_COOLDOWN_SEC", "600") or "600")
_PERSON_DB_SCHEMA_CACHE: dict[str, any] = {"ts": 0.0, "schema": None}
_PERSON_DB_LIST_CACHE: dict[str, any] = {"ts": 0.0, "pages": None}
_PERSON_REVIEW_MEMORY_PATH = (
    os.environ.get("PERSON_REVIEW_MEMORY_PATH")
    or os.path.join(BASE_DIR, "person_review_memory.json")
)
_PERSON_REVIEW_BACKUP_DIR = (
    os.environ.get("PERSON_REVIEW_BACKUP_DIR")
    or os.path.join(BASE_DIR, "backups")
)
_PERSON_REVIEW_MEMORY: dict[str, dict] = {}
_PERSON_REVIEW_MEMORY_LOADED = False
_PERSON_REVIEW_RETENTION_DAYS = int(os.environ.get("PERSON_REVIEW_RETENTION_DAYS", "90") or "90")
FAILED_URL_QUEUE_PATH = (
    os.environ.get("FAILED_URL_QUEUE_PATH")
    or os.path.join(BASE_DIR, "failed_url_queue.json")
)
FAILED_URL_RETRY_MAX = int(os.environ.get("FAILED_URL_RETRY_MAX", "3") or "3")
FAILED_URL_RETRY_INTERVAL_MIN = int(os.environ.get("FAILED_URL_RETRY_INTERVAL_MIN", "10") or "10")
FAILED_RETRY_SCHEDULER_ENABLED = (os.environ.get("FAILED_RETRY_SCHEDULER_ENABLED", "1").strip() not in ("0", "false", "False"))
EXPERT_SHEET_VERIFY_WRITE = (os.environ.get("EXPERT_SHEET_VERIFY_WRITE", "0").strip() not in ("0", "false", "False"))
OPS_INSIGHT_INTERVAL_MIN = int(os.environ.get("OPS_INSIGHT_INTERVAL_MIN", "360") or "360")
OPS_INSIGHT_ENABLED = (os.environ.get("OPS_INSIGHT_ENABLED", "1").strip() not in ("0", "false", "False"))
OPS_INSIGHT_CHAT_ID = _ascii_env("OPS_INSIGHT_CHAT_ID")
OPS_EVENT_LOG_PATH = (
    os.environ.get("OPS_EVENT_LOG_PATH")
    or os.path.join(BASE_DIR, "ops_events.jsonl")
)
TRUST_DATA_DB_PATH = (
    os.environ.get("TRUST_DATA_DB_PATH")
    or os.path.join(BASE_DIR, "trust_data.sqlite3")
)
CLAIM_SAMPLE_DIR = (
    os.environ.get("CLAIM_SAMPLE_DIR")
    or os.path.join(BASE_DIR, "analysis", "pilot_samples")
)
TRUST_SYMBOL_MAP_PATH = (
    os.environ.get("TRUST_SYMBOL_MAP_PATH")
    or os.path.join(BASE_DIR, "data", "trust_symbol_mappings.json")
)
TRUST_CLAIM_OVERRIDE_PATH = (
    os.environ.get("TRUST_CLAIM_OVERRIDE_PATH")
    or os.path.join(BASE_DIR, "data", "trust_claim_overrides.local.json")
)
_FAILED_URL_QUEUE: dict[str, dict] = {}
_FAILED_URL_QUEUE_LOADED = False

_PERSON_FORM_FIELDS = [
    ("name", "이름"),
    ("affiliation", "소속"),
    ("role", "직책"),
    ("career", "주요 경력"),
    ("expertise", "전문 분야"),
    ("source_url", "근거 링크(URL)"),
]


def check_runtime_keys() -> bool:
    """Gemini/Telegram 키 유효성 사전 점검."""
    ok = True

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY 미설정")
        ok = False
    else:
        try:
            r = httpx.get(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": GEMINI_API_KEY},
                timeout=15,
            )
            if r.status_code != 200:
                logger.error(f"GEMINI_API_KEY 점검 실패: {r.status_code} {r.text[:180]}")
                ok = False
            else:
                logger.info("✓ GEMINI_API_KEY 유효")
        except Exception as e:
            logger.error(f"GEMINI_API_KEY 점검 실패: {e}")
            ok = False

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN 미설정")
        ok = False
    else:
        try:
            r = httpx.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe",
                timeout=15,
            )
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if r.status_code != 200 or not data.get("ok"):
                logger.error(f"TELEGRAM_BOT_TOKEN 점검 실패: {r.status_code} {r.text[:180]}")
                ok = False
            else:
                logger.info("✓ TELEGRAM_BOT_TOKEN 유효")
        except Exception as e:
            logger.error(f"TELEGRAM_BOT_TOKEN 점검 실패: {e}")
            ok = False

    return ok


def check_notion_access() -> None:
    """Notion 토큰과 DB 권한을 분리해서 진단한다."""
    print("🔎 Notion token / database access 진단")
    if not NOTION_API_KEY:
        print("❌ NOTION_API_KEY 미설정")
        return
    try:
        me_resp = httpx.get(
            "https://api.notion.com/v1/users/me",
            headers={
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28",
            },
            timeout=20,
        )
        print(f"users/me: {me_resp.status_code}")
        if me_resp.status_code == 200:
            try:
                data = me_resp.json()
                name = data.get("name", "")
                print(f"  · bot: {data.get('bot', {}).get('owner', {}).get('type', '')}")
                print(f"  · name: {name}")
                print(f"  · expected: {EXPECTED_NOTION_BOT_NAME}")
                if name and name != EXPECTED_NOTION_BOT_NAME:
                    print("  ⚠️ 현재 런타임 integration이 개발용 기준과 다릅니다.")
            except Exception:
                pass
        else:
            print(f"  · body: {me_resp.text[:200]}")
    except Exception as exc:
        print(f"users/me 요청 실패: {exc}")
        return

    try:
        db_resp = httpx.get(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
            headers={
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28",
            },
            timeout=20,
        )
        print(f"databases/{NOTION_DATABASE_ID}: {db_resp.status_code}")
        if db_resp.status_code == 200:
            try:
                data = db_resp.json()
                print(f"  · title: {''.join(t.get('plain_text', '') for t in data.get('title', []))}")
                print(f"  · archived: {data.get('archived', False)}")
            except Exception:
                pass
        else:
            print(f"  · body: {db_resp.text[:200]}")
            if db_resp.status_code == 401:
                print("  → 토큰 자체가 유효하지 않거나 integration 권한이 끊겼을 가능성이 큽니다.")
            elif db_resp.status_code == 404:
                print("  → DB가 다른 워크스페이스로 이동했거나 integration에 share되지 않았을 가능성이 큽니다.")
    except Exception as exc:
        print(f"database 조회 실패: {exc}")


# ── 유튜브 ID 추출 ───────────────────────────────────────
def extract_video_id(url: str):
    return adapter_extract_video_id(url)


def extract_youtube_url(text: str) -> Optional[str]:
    """텍스트에서 유튜브 URL 추출"""
    pattern = r"https?://(?:(?:www\.|m\.)?youtube\.com/(?:watch\?[^\s]*v=|shorts/)|youtu\.be/)[A-Za-z0-9_\-?=&%]+"
    m = re.search(pattern, text)
    return m.group(0) if m else None


# ── YouTube 메타데이터 가져오기 ──────────────────────────
def fetch_youtube_metadata(video_id: str) -> dict:
    return adapter_fetch_youtube_metadata(video_id, YOUTUBE_API_KEY, _clean)


# ── YouTube 댓글 조회 (인물 식별 1순위 보조) ──────────────
def fetch_youtube_comments(video_id: str, channel_id: str = "", max_results: int = 20) -> list[str]:
    """상위 댓글 텍스트 반환. 채널 오너(핀고정 포함) 댓글을 최상단에 배치."""
    try:
        comments = adapter_fetch_youtube_comments(
            video_id,
            YOUTUBE_API_KEY,
            channel_id=channel_id,
            max_results=max_results,
        )
        owner_comments = comments[:]
        if owner_comments:
            print(f"  → 채널 오너 댓글 {len(owner_comments)}개 우선 배치")
        return comments
    except Exception as e:
        print(f"  ⚠️ 댓글 조회 실패 (무시): {e}")
        return []


# ── YouTube 채널 소개글(about) 조회 (고정 출연자 단서) ───────
def fetch_channel_about(channel_id: str) -> str:
    """채널 소개글 반환. 고정 출연자·전문가 정보가 포함된 경우 多."""
    try:
        return adapter_fetch_channel_about(channel_id, YOUTUBE_API_KEY, _clean)
    except Exception as e:
        print(f"  ⚠️ 채널 소개글 조회 실패 (무시): {e}")
        return ""


# ── YouTube 채널 최근 영상 제목 조회 (반복 출연 패턴 감지) ──
def fetch_channel_recent_video_titles(channel_id: str, max_results: int = 10) -> list[str]:
    """채널 uploads 플레이리스트에서 최근 영상 제목 반환."""
    try:
        return adapter_fetch_channel_recent_video_titles(
            channel_id,
            YOUTUBE_API_KEY,
            _clean,
            max_results=max_results,
        )
    except Exception as e:
        print(f"  ⚠️ 채널 영상 목록 조회 실패 (무시): {e}")
        return []


# ── YouTube 자막 추출 ────────────────────────────────────
def fetch_transcript(video_id: str) -> str:
    """한국어 자막 우선, 없으면 영어, 둘 다 없으면 빈 문자열 반환"""
    try:
        text = adapter_fetch_transcript(video_id, _clean)
        if text:
            print(f"  → 자막 추출 완료 ({len(text)}자)")
        else:
            print("  → 자막 없음/실패 (메타데이터로 대체)")
        return text
    except Exception as e:
        print(f"  → 자막 추출 실패: {e} (메타데이터로 대체)")
        return ""


# ── 자막 화자(speaker) 태그 추출 ─────────────────────────
_SPEAKER_TAG_PATTERNS = [
    # [홍길동] 또는 [홍길동:] 줄 앞머리
    re.compile(r"^\[\s*([가-힣A-Za-z]{2,10})\s*\]", re.MULTILINE),
    # 홍길동: 줄 앞머리 (2~10자 이름 + 콜론)
    re.compile(r"^([가-힣A-Za-z]{2,10})\s*:", re.MULTILINE),
    # ▶홍길동 또는 >>홍길동
    re.compile(r"[▶>]{1,2}\s*([가-힣A-Za-z]{2,10})"),
]


def _extract_speaker_from_transcript(transcript: str) -> str:
    """자막 텍스트에서 화자 표시 패턴 추출. 가장 빈번한 화자명 반환."""
    if not transcript:
        return ""
    from collections import Counter
    counts: Counter = Counter()
    for pat in _SPEAKER_TAG_PATTERNS:
        for m in pat.finditer(transcript):
            name = m.group(1).strip()
            # 일반적인 한국 이름 형식(2~4자)만 허용
            if re.fullmatch(r"[가-힣]{2,4}", name):
                counts[name] += 1
    if not counts:
        return ""
    top_name, freq = counts.most_common(1)[0]
    if freq >= 2:
        print(f"  → 자막 화자 감지: {top_name} ({freq}회)")
        return top_name
    return ""


# ── 채널 제목 반복 인물 패턴 감지 ────────────────────────
def _detect_recurring_person_from_titles(titles: list[str]) -> str:
    """채널 최근 영상 제목에서 2회 이상 반복 등장하는 한국식 이름 반환."""
    if not titles:
        return ""
    from collections import Counter
    counts: Counter = Counter()
    # 제목에서 한국 이름(2~4자) 후보 추출
    name_pat = re.compile(r"[가-힣]{2,4}")
    # 1글자 성+단어 패턴은 오탐 많으므로 최소 2자 연속 한글만
    stop_words = {
        "경제", "주식", "투자", "금융", "시장", "전망", "분석", "뉴스", "이슈",
        "오늘", "최근", "현재", "국내", "해외", "미국", "중국", "한국", "글로벌",
        "금리", "환율", "물가", "부동산", "주가", "코스피", "나스닥", "달러",
        "증시", "채권", "원자재", "에너지", "반도체", "기술주", "성장주",
    }
    for title in titles:
        for m in name_pat.finditer(title):
            candidate = m.group()
            if candidate not in stop_words and len(candidate) >= 2:
                counts[candidate] += 1
    for name, freq in counts.most_common(5):
        if freq >= 2 and re.fullmatch(r"[가-힣]{2,4}", name):
            print(f"  → 채널 제목 반복 인물 감지: {name} ({freq}/{len(titles)}편)")
            return name
    return ""


# ── 분석 결과 필드 정규화 (하위 호환) ───────────────────
def _normalize_analysis(result: dict) -> dict:
    """새 JSON 스키마 필드를 기존 코드가 사용하는 필드명으로 매핑"""
    # recommended_assets → mentioned_products (구글 시트·노션 속성용)
    if "recommended_assets" in result and "mentioned_products" not in result:
        result["mentioned_products"] = [
            a.get("name", "") for a in result.get("recommended_assets", []) if a.get("name")
        ]
    # market_outlook → economic_outlook (write_to_sheet 호환)
    if "market_outlook" in result and "economic_outlook" not in result:
        result["economic_outlook"] = {
            "direction": "",
            "description": result.get("market_outlook", "정보 없음"),
        }
    if "key_sectors" not in result:
        result["key_sectors"] = []
    return result


# ── Gemini로 분석 ────────────────────────────────────────
def analyze_with_gemini(
    metadata: dict,
    transcript: str = "",
    comments: Optional[list[str]] = None,
    channel_titles: Optional[list[str]] = None,
    channel_about: str = "",
    speaker_hint: str = "",
    recurring_person: str = "",
) -> dict:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")

    def _build_prompt(
        metadata: dict,
        transcript: str = "",
        comments: Optional[list[str]] = None,
        channel_titles: Optional[list[str]] = None,
        channel_about: str = "",
        speaker_hint: str = "",
        recurring_person: str = "",
    ) -> str:
        description_full = metadata.get("description", "")
        content_section = f"""━━━ 영상 제목 (인물 식별 최우선 참조) ━━━
{metadata.get('title', '')}

━━━ 채널명 ━━━
{metadata.get('channel', '')}

━━━ 태그 ━━━
{', '.join(metadata.get('tags', []))}

━━━ 설명글 전체 (출연자·게스트 정보 포함 가능) ━━━
{description_full}"""

        if channel_about:
            content_section += f"""

━━━ 채널 소개글 (고정 출연자·전문가 정보) ━━━
{channel_about}"""

        if speaker_hint:
            content_section += f"""

━━━ 자막 화자 감지 결과 (가장 자주 등장한 화자명) ━━━
{speaker_hint}"""

        if recurring_person:
            content_section += f"""

━━━ 채널 제목 반복 인물 (최근 {len(channel_titles or [])}편에서 반복 감지) ━━━
{recurring_person}"""

        if transcript:
            content_section += f"""

━━━ 영상 자막 (발언 내용) ━━━
{transcript}"""

        if comments:
            comments_text = "\n".join(f"- {c}" for c in comments[:10])
            content_section += f"""

━━━ 시청자 댓글 상위 10개 (채널 오너 댓글 우선 배치됨) ━━━
{comments_text}"""

        if channel_titles:
            titles_text = "\n".join(f"- {t}" for t in channel_titles)
            content_section += f"""

━━━ 채널 최근 영상 제목 (반복 출연자 패턴 참조) ━━━
{titles_text}"""

        return f"""당신은 증권사의 시니어 리서치 애널리스트입니다. 채널에 출연한 전문가의 분석을 개인 고객에게 설명하는 역할입니다.
아래 한국 경제·금융 유튜브 콘텐츠를 분석하여 현재 시장 상황과 모멘텀을 분석하고 시장을 전망하세요.

{content_section}

정보를 찾을 수 없는 항목은 반드시 "정보 없음"으로 표기하세요.
JSON으로만 응답하세요. 다른 텍스트는 절대 포함하지 마세요.
문자열 필드는 정보 없으면 "정보 없음", 배열 필드는 정보 없으면 빈 배열 []로 표기하세요.
타임스탬프는 자막(발언 내용)이 제공된 경우에만 "[분:초]" 형식으로 표기하고, 없으면 빈 문자열로 두세요.

{{
  "hashtags": ["#주제1", "#주제2", "#주제3"],
  "summary": "핵심 요약 (1~2문장, 100자 이내). 없으면 '정보 없음'",
  "opinion": "출연자의 핵심 투자 의견·경제 전망 (100자 이내). 없으면 '정보 없음'",
  "person": {{
    "name": "출연자/강연자 실명. 영상 제목·설명글·채널소개·자막화자·댓글을 최우선으로 참조. 끝내 모르면 '미상'",
    "role": "직책 전체 (애널리스트/이코노미스트/펀드매니저/기자/교수 등). 끝내 모르면 '미상'",
    "affiliation": "소속 기관·회사 정식명칭. 모르면 채널명 그대로 사용",
    "background": "출연자 소개 및 주요 경력 배경 (2~3문장). 없으면 '정보 없음'"
  }},
  "key_summary": [
    "영상 메인 주제 요약 1줄",
    "영상 메인 주제 요약 2줄",
    "영상 메인 주제 요약 3줄"
  ],
  "economic_indicators": [
    {{
      "indicator": "지표명 (예: 기준금리, 물가상승률, 실업률)",
      "value": "구체적 수치 (예: 5.25%, 3.2%)",
      "context": "해당 수치의 의미 및 맥락 설명",
      "timestamp": "언급 시점 (분:초, 예: 3:45). 없으면 빈 문자열"
    }}
  ],
  "market_momentum": {{
    "positive": ["긍정적 요인(호재) 1", "긍정적 요인(호재) 2"],
    "negative": ["부정적 요인(악재) 1", "부정적 요인(악재) 2"]
  }},
  "market_outlook": "전문가/패널이 예상하는 향후 시장 방향성 (2~3문장). 없으면 '정보 없음'",
  "recommended_assets": [
    {{
      "name": "영상에서 추천하거나 긍정적으로 언급한 자산/종목명",
      "reason": "추천 및 긍정 평가의 구체적인 근거 (실적, 테마, 매크로 환경 등)",
      "target_or_risk": "언급된 목표가 또는 리스크 요인. 없으면 빈 문자열",
      "timestamp": "언급 시점 (분:초). 없으면 빈 문자열"
    }}
  ],
  "is_lecture": false,
  "lecture_analysis": {{
    "root_cause": "영상이 구조적 경제 흐름을 설명하는 강의인 경우 — 현상의 원인 (근거 데이터 + 타임스탬프 포함). 강의가 아니면 빈 문자열",
    "impact": "이 현상이 어떤 산업이나 자산 시장에 미치는 파급 효과. 강의가 아니면 빈 문자열",
    "conclusion_strategy": "강연자의 최종 결론 및 투자자 대응 전략. 강의가 아니면 빈 문자열"
  }}
}}"""
    return adapter_analyze_with_gemini(
        GEMINI_API_KEY,
        metadata,
        build_prompt=_build_prompt,
        normalize=_normalize_analysis,
        transcript=transcript,
        comments=comments,
        channel_titles=channel_titles,
        channel_about=channel_about,
        speaker_hint=speaker_hint,
        recurring_person=recurring_person,
    )


def _repair_json(text: str) -> str:
    """LLM이 자주 만드는 JSON 오류를 수정 후 반환."""
    # 1) 코드 펜스 제거
    text = re.sub(r"```(?:json)?\s*|\s*```", "", text).strip()
    # 2) trailing comma: ,} / ,] 제거
    text = re.sub(r",\s*([}\]])", r"\1", text)
    # 3) 문자열 내 unescaped 개행 → \\n 으로 변환
    #    큰따옴표로 열린 문자열 안의 리터럴 \n/\r 을 이스케이프
    def _fix_newlines(m: re.Match) -> str:
        return m.group(0).replace("\n", "\\n").replace("\r", "\\r")
    text = re.sub(r'"(?:[^"\\]|\\.)*"', _fix_newlines, text, flags=re.DOTALL)
    return text


def _google_search_items(query: str, num: int = 3) -> list[dict]:
    """검색 결과 원본 항목 반환 (serper 우선, 실패 시 CSE fallback)."""
    # 1) serper.dev 우선
    if SERPER_API_KEY:
        try:
            resp = httpx.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": max(1, min(int(num or 3), 10))},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            organic = data.get("organic", []) or []
            items = []
            for it in organic:
                items.append(
                    {
                        "title": it.get("title", ""),
                        "snippet": it.get("snippet", ""),
                        "link": it.get("link", ""),
                    }
                )
            if items:
                return items
        except Exception:
            pass

    # 2) Google CSE fallback
    if GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID:
        try:
            service = build("customsearch", "v1", developerKey=GOOGLE_CSE_API_KEY)
            result = service.cse().list(q=query, cx=GOOGLE_CSE_ID, num=num).execute()
            return result.get("items", []) or []
        except Exception:
            pass

    return []


def _pick_best_source_link(items: list[dict], name: str, affiliation: str) -> str:
    """검색 결과 중 신뢰도 높은 링크 1개 선택."""
    if not items:
        return ""

    name_n = _compact_identity_text(name)
    aff_n = _compact_identity_text(affiliation)
    trusted_domains = ("wikipedia.org", "fnnews.com", "mk.co.kr", "hankyung.com", "sedaily.com", "naver.com")

    best_score = -1.0
    best_link = ""
    for it in items:
        title = _normalize_identity_text(it.get("title", ""))
        snippet = _normalize_identity_text(it.get("snippet", ""))
        link = it.get("link", "") or ""
        blob = _compact_identity_text(f"{title} {snippet}")
        domain_bonus = 0.2 if any(d in link for d in trusted_domains) else 0.0
        name_hit = 0.5 if (name_n and name_n in blob) else 0.0
        aff_hit = 0.3 if (aff_n and aff_n in blob) else 0.0
        score = domain_bonus + name_hit + aff_hit
        if score > best_score and link:
            best_score = score
            best_link = link

    return best_link or (items[0].get("link", "") if items else "")


# ── Google Search 헬퍼 ───────────────────────────────────
def _google_search(query: str, num: int = 3) -> str:
    """Google CSE 검색 후 snippet 합산 문자열 반환. 실패 시 빈 문자열."""
    items = _google_search_items(query, num=num)
    return " ".join(i.get("snippet", "") for i in items)


def _gemini_json(prompt: str):
    """Gemini로 JSON 응답 요청. 파싱 실패 시 None 반환."""
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        raw = response.text.strip()
        for attempt in (raw, _repair_json(raw)):
            try:
                return json.loads(attempt)
            except json.JSONDecodeError:
                pass
            m = re.search(r'[\[{][\s\S]*[\]}]', attempt)
            if m:
                try:
                    return json.loads(m.group(0))
                except json.JSONDecodeError:
                    try:
                        return json.loads(_repair_json(m.group(0)))
                    except json.JSONDecodeError:
                        pass
        return None
    except Exception:
        return None


# ── 설명글/제목에서 출연자 패턴 추출 (1순위 보조) ──────────
_PERSON_ROLE_SUFFIX = (
    "이코노미스트", "애널리스트", "펀드매니저", "전략가", "연구원", "소장", "원장",
    "위원", "이사", "본부장", "부장", "팀장", "교수", "기자", "대표", "대표이사",
    "회장", "부회장", "전무", "상무", "과장", "차장", "책임", "수석",
)
_PERSON_DESC_PATTERNS = [
    # "출연 : 홍길동 이코노미스트" / "게스트: 홍길동 KB증권 애널리스트"
    r"(?:출연|게스트|인터뷰|진행|강연자|강사|패널)\s*[：:]\s*([가-힣A-Za-z]{2,10})\s*((?:[가-힣A-Za-z·\s]{0,20}?(?:" + "|".join(_PERSON_ROLE_SUFFIX) + r"))?)",
    # "with 홍길동" (영어권 채널)
    r"\bwith\s+([A-Za-z가-힣]{2,20})\s*((?:[A-Za-z가-힣\s]{0,20}?(?:" + "|".join(_PERSON_ROLE_SUFFIX) + r"))?)",
    # 제목에서 "[홍길동의 경제이야기]" 패턴
    r"[\[【]([가-힣A-Za-z]{2,8})의\s",
]


def _extract_person_from_description(description: str, title: str = "") -> tuple[str, str, str]:
    """설명글·제목에서 출연자 명시 패턴 추출. 반환: (name, affiliation, role)"""
    for text in [description, title]:
        if not text:
            continue
        for pat in _PERSON_DESC_PATTERNS:
            m = re.search(pat, text)
            if m:
                name = m.group(1).strip()
                role_hint = (m.group(2).strip() if len(m.groups()) >= 2 else "")
                if name and len(name) >= 2:
                    return name, "", role_hint
    return "", "", ""


# ── 전문가 시트 채널 기반 인물 조회 (2순위) ─────────────────
def lookup_person_from_expert_sheet_by_channel(channel: str) -> Optional[dict]:
    """채널명으로 전문가 시트에서 인물 검색. dominant_channel/top_channels 열 대조."""
    if not channel or not EXPERT_SHEET_ID:
        return None
    try:
        service = _get_sheets_service()
        resp = service.spreadsheets().values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=EXPERT_SHEET_FULL_RANGE,
        ).execute()
        rows = resp.get("values", [])
        if len(rows) < 2:
            return None
        channel_norm = _compact_identity_text(channel)
        best: Optional[dict] = None
        best_score = 0.0
        for row in rows[1:]:
            if not row:
                continue
            row_name = row[0] if len(row) > 0 else ""
            row_aff = row[1] if len(row) > 1 else ""
            row_role = row[2] if len(row) > 2 else ""
            row_career = row[3] if len(row) > 3 else ""
            row_expertise = row[4] if len(row) > 4 else ""
            dom_ch = row[9] if len(row) > 9 else ""   # dominant_channel
            top_ch = row[10] if len(row) > 10 else ""  # top_channels
            dom_norm = _compact_identity_text(dom_ch)
            top_norm = _compact_identity_text(top_ch)
            if not dom_norm and not top_norm:
                continue
            score = 0.0
            if channel_norm and dom_norm:
                if channel_norm == dom_norm or channel_norm in dom_norm or dom_norm in channel_norm:
                    score = 1.0
                else:
                    score = max(score, _similarity(channel_norm, dom_norm))
            if channel_norm and top_norm:
                if channel_norm in top_norm:
                    score = max(score, 0.85)
                else:
                    score = max(score, _similarity(channel_norm, top_norm) * 0.8)
            if score > best_score and score >= 0.70:
                best_score = score
                best = {
                    "name": row_name,
                    "affiliation": row_aff,
                    "role": row_role,
                    "career": row_career,
                    "expertise": row_expertise,
                }
        if best:
            print(f"  [VERIFY] 전문가 시트 채널 매칭 (score={best_score:.2f}): {best['name']} ← {channel}")
        return best
    except Exception as e:
        print(f"  ⚠️ 전문가 시트 채널 조회 실패 (무시): {e}")
        return None


# ── Google Search로 인물 검증 (3순위) ────────────────────
def verify_person(name: str, affiliation: str, role: str, channel: str = "") -> tuple[str, str, str]:
    return service_verify_person(
        name,
        affiliation,
        role,
        channel,
        PeopleVerificationDeps(
            lookup_person_from_expert_sheet_by_channel=lookup_person_from_expert_sheet_by_channel,
            compact_identity_text=_compact_identity_text,
            sanitize_person_fields=_sanitize_person_fields,
            google_search=_google_search,
            gemini_json=_gemini_json,
            print_fn=print,
        ),
    )


# ── YouTube 검색으로 인물 존재 확인 (신뢰도 신호) ─────────
_YOUTUBE_PERSON_SEARCH_CACHE: dict[str, dict] = {}


def search_youtube_for_person(name: str, affiliation: str, role: str) -> dict:
    """인물명+소속+직책으로 YouTube 검색해 등장 여부 확인.

    Returns:
        {
          "found": bool,           # 1건 이상 관련 영상 존재 여부
          "confidence": float,     # 0.0~1.0 신뢰도 점수
          "video_count": int,      # 매칭된 영상 수
          "matched_channels": list[str],  # 등장 채널 목록
          "reason": str,           # 판정 근거
        }
    """
    _MISSING = {"미상", "정보 없음", ""}
    if not YOUTUBE_API_KEY or not name or name in _MISSING:
        return {"found": False, "confidence": 0.5, "video_count": 0,
                "matched_channels": [], "reason": "검색 스킵 (이름 없음)"}

    cache_key = _compact_identity_text(f"{name}|{affiliation}|{role}")
    if cache_key in _YOUTUBE_PERSON_SEARCH_CACHE:
        return _YOUTUBE_PERSON_SEARCH_CACHE[cache_key]

    name_n = _compact_identity_text(name)
    aff_n = _compact_identity_text(affiliation) if affiliation not in _MISSING else ""
    role_n = _compact_identity_text(role) if role not in _MISSING else ""

    # 쿼리 우선순위: 세부 → 일반
    queries: list[str] = []
    if aff_n and role_n:
        queries.append(f"{name} {affiliation} {role}")
    if aff_n:
        queries.append(f"{name} {affiliation} 경제")
    if role_n:
        queries.append(f"{name} {role}")
    queries.append(f"{name} 경제 전문가")

    result: dict = {"found": False, "confidence": 0.2, "video_count": 0,
                    "matched_channels": [], "reason": "YouTube 검색 결과 없음"}
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        for q in queries:
            resp = youtube.search().list(
                part="snippet",
                q=q,
                type="video",
                maxResults=5,
                relevanceLanguage="ko",
            ).execute()
            items = resp.get("items", [])
            if not items:
                continue

            matched = 0
            channels: list[str] = []
            for item in items:
                snip = item.get("snippet", {})
                title = _compact_identity_text(snip.get("title", ""))
                desc = _compact_identity_text(snip.get("description", ""))
                ch = _clean(snip.get("channelTitle", ""))
                ch_n = _compact_identity_text(ch)
                blob = f"{title} {desc} {ch_n}"

                name_hit = name_n and name_n in blob
                aff_hit = aff_n and aff_n in blob
                role_hit = role_n and role_n in blob

                if name_hit:
                    matched += 1
                    if ch and ch not in channels:
                        channels.append(ch)
                elif aff_hit or role_hit:
                    matched += 1  # 이름은 없지만 소속/직책 일치 (부분 신뢰)

            if matched == 0:
                # 검색 결과는 있지만 이름·소속·직책 미등장 → 동명이인 가능성
                result = {
                    "found": False,
                    "confidence": 0.15,
                    "video_count": 0,
                    "matched_channels": [],
                    "reason": f"YouTube 검색 결과 {len(items)}건이나 이름·소속 미일치 (동명이인 가능성)",
                }
            elif matched >= 3:
                result = {
                    "found": True,
                    "confidence": 0.90,
                    "video_count": matched,
                    "matched_channels": channels,
                    "reason": f"YouTube {matched}건 이상 일치 (채널: {', '.join(channels[:3])})",
                }
            elif matched >= 1:
                conf = 0.55 + min(matched - 1, 2) * 0.10  # 1건=0.55, 2건=0.65
                result = {
                    "found": True,
                    "confidence": conf,
                    "video_count": matched,
                    "matched_channels": channels,
                    "reason": f"YouTube {matched}건 부분 일치",
                }
            break  # 1개 쿼리에서 결과 얻으면 종료

        print(f"  [YT_VERIFY] {name}: conf={result['confidence']:.2f}, {result['reason']}")
    except Exception as e:
        print(f"  ⚠️ YouTube 인물 검색 실패 (무시): {e}")
        result = {"found": False, "confidence": 0.5, "video_count": 0,
                  "matched_channels": [], "reason": f"API 오류: {e}"}

    _YOUTUBE_PERSON_SEARCH_CACHE[cache_key] = result
    return result


# ── Google Search로 투자상품 명칭 검증 ───────────────────
def verify_products(products: list[str]) -> list[str]:
    if not products:
        return products

    try:
        # 상품별 검색 결과 수집
        snippets_by_product = {}
        for product in products:
            snippets_by_product[product] = _google_search(
                f"{product} 주식 종목코드 ETF 공식명칭", num=2
            )

        products_with_context = "\n".join(
            f"- {p}: {snippets_by_product.get(p, '')[:200]}"
            for p in products
        )

        corrected = _gemini_json(f"""아래 투자상품/종목 목록을 검색 결과를 참고하여 정확한 공식 명칭으로 교정하세요.

{products_with_context}

교정 규칙:
- 한국 주식: "종목명(종목코드)" 형식 (예: 삼성전자(005930), SK하이닉스(000660))
- 해외 주식: "종목명(티커)" 형식 (예: 엔비디아(NVDA), 애플(AAPL))
- 국내 ETF: 공식 ETF 명칭 그대로 (예: TIGER 미국S&P500, KODEX 반도체)
- 해외 ETF: "명칭(티커)" 형식 (예: QQQ, SPY)
- 지수/기타: 원본 유지
- 정보가 불확실하면 원본 그대로 유지

JSON 배열로만 응답 (입력 순서 그대로, 개수 동일):
{json.dumps(products, ensure_ascii=False)}""")

        if isinstance(corrected, list) and len(corrected) == len(products):
            return [str(c) for c in corrected]

    except Exception as e:
        print(f"  ⚠️  상품 검증 스킵 ({e})")

    return products


# ── 구글 시트 클라이언트 ─────────────────────────────────
def _get_sheets_service():
    credentials_path = (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
        or os.path.join(BASE_DIR, "credentials.json")
    )
    creds = ServiceAccountCredentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def _ensure_tab_exists(spreadsheet_id: str, tab: str):
    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in existing:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()


def save_daily_briefing_log(
    briefing: str,
    start_kst: datetime,
    end_kst: datetime,
    video_count: int,
    indicator_count: int,
    sent_ok: bool,
):
    """데일리 브리핑 본문/메타를 시트 로그에 저장."""
    try:
        _ensure_tab_exists(DAILY_BRIEFING_SHEET_ID, DAILY_BRIEFING_TAB)
        service = _get_sheets_service()
        adapter_append_briefing_log(
            service=service,
            spreadsheet_id=DAILY_BRIEFING_SHEET_ID,
            tab=DAILY_BRIEFING_TAB,
            headers=_DAILY_BRIEFING_HEADERS,
            briefing=briefing,
            start_kst=start_kst,
            end_kst=end_kst,
            video_count=video_count,
            indicator_count=indicator_count,
            sent_ok=sent_ok,
        )
    except Exception as e:
        logger.error(f"데일리 브리핑 로그 저장 실패: {e}")


def _tokenize_csv_field(raw: str) -> list[str]:
    vals: list[str] = []
    for x in (raw or "").replace("/", ",").split(","):
        t = x.strip()
        if not t or t in ("정보 없음", "미상"):
            continue
        vals.append(t)
    return vals


def _build_daily_highlights(rows: list[dict], max_lines: int = 5) -> str:
    """당일 데이터에서 빈도 기반 하이라이트 생성(저비용/저토큰)."""
    if not rows:
        return "하이라이트 없음"

    sector_count: dict[str, int] = {}
    product_count: dict[str, int] = {}
    channel_count: dict[str, int] = {}
    outlook_count: dict[str, int] = {}

    for r in rows:
        for s in _tokenize_csv_field(r.get("주요 섹터", "")):
            sector_count[s] = sector_count.get(s, 0) + 1
        for p in _tokenize_csv_field(r.get("언급 상품", "")):
            product_count[p] = product_count.get(p, 0) + 1
        ch = (r.get("채널명", "") or "").strip()
        if ch:
            channel_count[ch] = channel_count.get(ch, 0) + 1
        out = (r.get("경기 전망", "") or "").strip()
        if out:
            key = out.split(":", 1)[0].strip() if ":" in out else out[:24]
            if key:
                outlook_count[key] = outlook_count.get(key, 0) + 1

    def _top_items(counter: dict[str, int], n: int = 3) -> list[tuple[str, int]]:
        return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:n]

    lines: list[str] = []
    tops = _top_items(sector_count, 3)
    if tops:
        lines.append("📌 섹터: " + ", ".join(f"{k}({v})" for k, v in tops))
    tops = _top_items(product_count, 3)
    if tops:
        lines.append("💹 종목/ETF: " + ", ".join(f"{k}({v})" for k, v in tops))
    tops = _top_items(channel_count, 2)
    if tops:
        lines.append("📺 주요 채널: " + ", ".join(f"{k}({v})" for k, v in tops))
    tops = _top_items(outlook_count, 3)
    if tops:
        lines.append("🧭 전망 키워드: " + ", ".join(f"{k}({v})" for k, v in tops))

    if not lines:
        lines.append("유의미한 하이라이트 자동 추출 결과가 없습니다.")
    return "\n".join(lines[:max_lines])


def save_daily_highlight_log(
    start_kst: datetime,
    end_kst: datetime,
    highlights: str,
    video_count: int,
    method: str = "rule-based",
):
    """데일리 하이라이트를 별도 시트 탭에 적재."""
    try:
        _ensure_tab_exists(DAILY_BRIEFING_SHEET_ID, DAILY_HIGHLIGHT_TAB)
        service = _get_sheets_service()
        sheet = service.spreadsheets()
        tab = DAILY_HIGHLIGHT_TAB
        values = sheet.values().get(
            spreadsheetId=DAILY_BRIEFING_SHEET_ID,
            range=f"'{tab}'!A:F",
        ).execute().get("values", [])
        if not values:
            sheet.values().update(
                spreadsheetId=DAILY_BRIEFING_SHEET_ID,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                body={"values": [_DAILY_HIGHLIGHT_HEADERS]},
            ).execute()

        row = [
            datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            start_kst.strftime("%Y-%m-%d %H:%M"),
            end_kst.strftime("%Y-%m-%d %H:%M"),
            (highlights or "")[:45000],
            video_count,
            method,
        ]
        sheet.values().append(
            spreadsheetId=DAILY_BRIEFING_SHEET_ID,
            range=f"'{tab}'!A:F",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as e:
        logger.error(f"데일리 하이라이트 로그 저장 실패: {e}")


def save_daily_feedback(feedback_label: str, user_id: str, username: str = ""):
    """브리핑 로그 최신 행의 피드백 컬럼에 누적 기록."""
    try:
        _ensure_tab_exists(DAILY_BRIEFING_SHEET_ID, DAILY_BRIEFING_TAB)
        service = _get_sheets_service()
        sheet = service.spreadsheets()
        tab = DAILY_BRIEFING_TAB
        values = sheet.values().get(
            spreadsheetId=DAILY_BRIEFING_SHEET_ID,
            range=f"'{tab}'!A:H",
        ).execute().get("values", [])
        if len(values) < 2:
            return
        row_idx = len(values)
        prev = values[-1][6] if len(values[-1]) > 6 else ""
        who = f"{username}({user_id})" if username else str(user_id)
        stamp = datetime.now(KST).strftime("%m-%d %H:%M")
        item = f"[{stamp}] {feedback_label}:{who}"
        merged = f"{prev} | {item}".strip(" |") if prev else item
        sheet.values().update(
            spreadsheetId=DAILY_BRIEFING_SHEET_ID,
            range=f"'{tab}'!G{row_idx}",
            valueInputOption="RAW",
            body={"values": [[merged[:45000]]]},
        ).execute()
    except Exception as e:
        logger.error(f"데일리 피드백 저장 실패: {e}")


_MISSING = {"정보 없음", "미상", ""}


def _is_complete(summary: str, person_str: str, opinion: str) -> bool:
    """핵심 3개 필드가 모두 유효한 값일 때만 True"""
    for val in (summary, person_str, opinion):
        if not val or val.strip() in _MISSING:
            return False
    return True


def write_to_sheet(
    url: str,
    video_title: str,
    channel: str,
    analysis: dict,
    person_str: str,
    timestamp: str,
):
    """분석 결과를 구글 시트에 기록. 완성도 미달 또는 URL 중복 시 건너뜀."""
    try:
        service = _get_sheets_service()
        adapter_append_analysis_row(
            service,
            GOOGLE_SHEET_ID,
            _SHEET_HEADERS,
            url,
            video_title,
            channel,
            analysis,
            person_str,
            timestamp,
            is_complete=_is_complete,
            out=print,
        )
    except Exception as e:
        print(f"  ⚠️ 구글 시트 기록 실패: {e}")


# ── 노션 속성 텍스트 추출 헬퍼 ──────────────────────────
def _get_rich_text(prop: dict) -> str:
    return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))


def _extract_from_page_blocks(notion: NotionClient, page_id: str) -> dict:
    """페이지 본문 블록에서 언급 상품·주요 섹터·경기 전망 텍스트 추출"""
    try:
        blocks = notion.blocks.children.list(block_id=page_id).get("results", [])
    except Exception:
        return {}

    result: dict = {"mentioned_products": [], "key_sectors": [], "economic_outlook": ""}
    section_map = {
        # 구버전 헤딩 (하위 호환)
        "💼 언급된 투자상품/종목": "products",
        "💼 추천 및 긍정 언급 자산": "products",
        "🏭 주요 섹터": "sectors",
        "📈 경기 전망": "outlook",
        # 신버전 헤딩
        "🔮 전문가의 시장 전망": "outlook",
    }
    current = None

    for block in blocks:
        btype = block.get("type", "")
        if btype == "heading_2":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("heading_2", {}).get("rich_text", [])
            )
            current = section_map.get(text)
        elif btype == "bulleted_list_item" and current in ("products", "sectors"):
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("bulleted_list_item", {}).get("rich_text", [])
            )
            if text and text != "정보 없음":
                key = "mentioned_products" if current == "products" else "key_sectors"
                result[key].append(text)
        elif btype == "paragraph" and current == "outlook":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("paragraph", {}).get("rich_text", [])
            )
            result["economic_outlook"] = text
            current = None  # 첫 번째 paragraph만 사용
        elif btype == "table" and current == "products":
            # 신버전은 추천 자산을 table로 저장하므로 첫 번째 컬럼(자산/종목명) 추출
            try:
                rows = notion.blocks.children.list(block_id=block["id"]).get("results", [])
                for idx, row in enumerate(rows):
                    if row.get("type") != "table_row":
                        continue
                    if idx == 0:  # 헤더 row 스킵
                        continue
                    cells = row.get("table_row", {}).get("cells", [])
                    if not cells:
                        continue
                    first_cell = "".join(t.get("plain_text", "") for t in cells[0])
                    if first_cell and first_cell != "정보 없음":
                        result["mentioned_products"].append(first_cell)
            except Exception:
                pass

    return result


def _build_complete_content_query_body(cursor: Optional[str] = None) -> dict:
    body: dict = {
        "filter": {
            "and": [
                {"property": "URL", "url": {"is_not_empty": True}},
                {"property": "한 줄 요약", "rich_text": {"is_not_empty": True}},
            ]
        },
        "page_size": 100,
    }
    if cursor:
        body["start_cursor"] = cursor
    return body


def _fetch_complete_notion_pages() -> list[dict]:
    pages: list[dict] = []
    cursor = None
    while True:
        resp = httpx.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers={
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=_build_complete_content_query_body(cursor),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def _count_complete_notion_pages() -> int:
    """노션 DB에서 완성된 페이지(URL + 한 줄 요약 있음) 수 반환"""
    return len(_fetch_complete_notion_pages())


def _count_sheet_rows() -> int:
    """구글 시트의 데이터 행 수(헤더 제외) 반환. 실패 시 -1"""
    try:
        service = _get_sheets_service()
        rows = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range="A:A"
        ).execute().get("values", [])
        return max(0, len(rows) - 1)
    except Exception:
        return -1


def _content_record_from_notion_page(notion: NotionClient, page: dict) -> Optional[ContentSyncRecord]:
    props = page.get("properties", {})
    url = props.get("URL", {}).get("url") or ""
    title_raw = (props.get("콘텐츠 제목", {}) or {}).get("title") or []
    video_title = title_raw[0]["text"]["content"] if title_raw else ""
    summary = _get_rich_text(props.get("한 줄 요약", {}))
    person_str = _get_rich_text(props.get("출연자", {}))
    opinion = _get_rich_text(props.get("인물의견", {}))
    if not _is_complete(summary, person_str, opinion):
        return None

    channel = _get_rich_text(props.get("채널", {})) or _get_rich_text(props.get("채널명", {}))
    hashtags = _get_rich_text(props.get("해시태그", {})) or _get_rich_text(props.get("주제", {}))
    timestamp = _get_rich_text(props.get("처리일시", {}))
    mentioned_products = _get_rich_text(props.get("언급 상품", {}))
    key_sectors = _get_rich_text(props.get("핵심 섹터", {}))
    economic_outlook = _get_rich_text(props.get("경제 전망", {}))

    if not all([mentioned_products, key_sectors, economic_outlook]):
        block_data = _extract_from_page_blocks(notion, page["id"])
        mentioned_products = mentioned_products or ", ".join(block_data.get("mentioned_products", []))
        key_sectors = key_sectors or ", ".join(block_data.get("key_sectors", []))
        economic_outlook = economic_outlook or block_data.get("economic_outlook", "")

    return ContentSyncRecord(
        url=url,
        title=video_title,
        channel=channel,
        hashtags=hashtags,
        summary=summary,
        person_str=person_str,
        opinion=opinion,
        mentioned_products=mentioned_products,
        key_sectors=key_sectors,
        economic_outlook=economic_outlook,
        timestamp=timestamp,
        source="notion",
        source_ref=page.get("id", ""),
    )


def _content_record_from_sheet_row(row: dict[str, str]) -> Optional[ContentSyncRecord]:
    url = (row.get("유튜브 URL", "") or "").strip()
    summary = (row.get("한줄 요약", "") or "").strip()
    person_str = (row.get("출연자(소속/직책/이름)", "") or "").strip()
    opinion = (row.get("인물 의견", "") or "").strip()
    if not url or not _is_complete(summary, person_str, opinion):
        return None

    return ContentSyncRecord(
        url=url,
        title=(row.get("영상 제목", "") or "").strip(),
        channel=(row.get("채널명", "") or "").strip(),
        hashtags=(row.get("주제(해시태그)", "") or "").strip(),
        summary=summary,
        person_str=person_str,
        opinion=opinion,
        mentioned_products=(row.get("언급 상품", "") or "").strip(),
        key_sectors=(row.get("주요 섹터", "") or "").strip(),
        economic_outlook=(row.get("경기 전망", "") or "").strip(),
        timestamp=(row.get("처리일시", "") or "").strip(),
        source="sheet",
        source_ref=url,
    )


def _load_complete_notion_content_records(notion: NotionClient) -> list[ContentSyncRecord]:
    records: list[ContentSyncRecord] = []
    for page in _fetch_complete_notion_pages():
        record = _content_record_from_notion_page(notion, page)
        if record:
            records.append(record)
    return records


def _load_complete_sheet_content_records() -> list[ContentSyncRecord]:
    records: list[ContentSyncRecord] = []
    for row in read_sheet_rows():
        record = _content_record_from_sheet_row(row)
        if record:
            records.append(record)
    return records


def _content_record_to_sheet_row(record: ContentSyncRecord) -> list[str]:
    return [
        record.url,
        record.title,
        record.channel,
        record.hashtags,
        record.summary,
        record.person_str,
        record.opinion,
        record.mentioned_products,
        record.key_sectors,
        record.economic_outlook,
        record.timestamp,
    ]


def _ensure_content_sheet_headers(service) -> set[str]:
    sheet = service.spreadsheets()
    existing_rows = sheet.values().get(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="A:K",
    ).execute().get("values", [])
    if not existing_rows:
        sheet.values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="A1",
            valueInputOption="RAW",
            body={"values": [_SHEET_HEADERS]},
        ).execute()
        return set()
    return {row[0] for row in existing_rows[1:] if row and row[0]}


def _append_content_record_to_sheet(service, record: ContentSyncRecord, existing_urls: set[str]) -> bool:
    if not record.url or record.url in existing_urls:
        return False
    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="A:K",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [_content_record_to_sheet_row(record)]},
    ).execute()
    existing_urls.add(record.url)
    return True


def _rich_text_prop(value: str) -> list[dict]:
    text = (value or "").strip()
    if not text:
        return []
    return [{"text": {"content": text[:2000]}}]


def _update_content_page_properties_from_record(notion: NotionClient, page_id: str, record: ContentSyncRecord) -> None:
    properties: dict[str, dict] = {
        "한 줄 요약": {"rich_text": _rich_text_prop(record.summary)},
        "출연자": {"rich_text": _rich_text_prop(record.person_str)},
        "인물의견": {"rich_text": _rich_text_prop(record.opinion)},
        "처리일시": {"rich_text": _rich_text_prop(record.timestamp)},
        "채널": {"rich_text": _rich_text_prop(record.channel)},
        "해시태그": {"rich_text": _rich_text_prop(record.hashtags)},
        "언급 상품": {"rich_text": _rich_text_prop(record.mentioned_products)},
        "핵심 섹터": {"rich_text": _rich_text_prop(record.key_sectors)},
        "경제 전망": {"rich_text": _rich_text_prop(record.economic_outlook)},
    }
    if record.title.strip():
        properties["콘텐츠 제목"] = {"title": _rich_text_prop(record.title)}
    notion.pages.update(page_id=page_id, properties=properties)


def _find_notion_content_page_by_url(url: str) -> Optional[str]:
    if not url:
        return None
    resp = httpx.post(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
        headers={
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        },
        json={
            "filter": {"property": "URL", "url": {"equals": url}},
            "page_size": 1,
        },
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return results[0]["id"] if results else None


def _create_notion_page_from_record(notion: NotionClient, record: ContentSyncRecord) -> str:
    page_id = _find_notion_content_page_by_url(record.url) or create_notion_page_from_url(notion, record.url)
    _update_content_page_properties_from_record(notion, page_id, record)
    return page_id


def _write_content_sync_report(sync_result: dict[str, object], notion_total: int, sheet_total: int) -> str:
    report = service_render_content_sync_report(
        sync_result,
        notion_total=notion_total,
        sheet_total=sheet_total,
    )
    Path(CONTENT_SYNC_REPORT_PATH).write_text(report + "\n", encoding="utf-8")
    return CONTENT_SYNC_REPORT_PATH


def _write_content_sync_blocked_report(reason: str, sheet_total: int) -> str:
    report = "\n".join(
        [
            "# Content Sync Report",
            "",
            "## Summary",
            "- Status: blocked",
            "- Notion complete rows: unavailable",
            f"- Sheets complete rows: {sheet_total}",
            "",
            "## Blocker",
            f"- {reason}",
            "",
            "## Next Step",
            f"- Share the Notion 경제 콘텐츠 DB with the development integration `{EXPECTED_NOTION_BOT_NAME}`, then rerun `python agent.py --check-content-sync`.",
            "",
        ]
    )
    Path(CONTENT_SYNC_REPORT_PATH).write_text(report + "\n", encoding="utf-8")
    return CONTENT_SYNC_REPORT_PATH


def _print_content_sync_summary(sync_result: dict[str, object], report_path: str = "") -> None:
    print("\n🔍 콘텐츠 동기화 점검")
    if sync_result.get("blocked"):
        print("  - status: blocked")
        print(f"  - reason: {sync_result.get('reason', '')}")
        if report_path:
            print(f"  - report: {report_path}")
        return
    print(f"  - in_sync: {len(sync_result['in_sync'])}")
    print(f"  - only_notion: {len(sync_result['only_notion'])}")
    print(f"  - only_sheet: {len(sync_result['only_sheet'])}")
    print(f"  - field_conflict: {len(sync_result['field_conflict'])}")
    if report_path:
        print(f"  - report: {report_path}")


def check_content_sync_status(write_report: bool = True) -> dict[str, object]:
    sheet_records = _load_complete_sheet_content_records()
    try:
        notion = get_notion_client()
        notion_records = _load_complete_notion_content_records(notion)
    except Exception as exc:
        reason = f"Notion content DB 접근 실패: {exc}"
        sync_result = {
            "in_sync": [],
            "only_notion": [],
            "only_sheet": [],
            "field_conflict": [],
            "blocked": True,
            "reason": reason,
        }
        report_path = _write_content_sync_blocked_report(reason, len(sheet_records)) if write_report else ""
        _print_content_sync_summary(sync_result, report_path)
        return sync_result

    sync_result = service_classify_content_sync(notion_records, sheet_records)
    report_path = _write_content_sync_report(sync_result, len(notion_records), len(sheet_records)) if write_report else ""
    _print_content_sync_summary(sync_result, report_path)
    return sync_result


def reconcile_content_sync() -> dict[str, int]:
    notion = get_notion_client()
    service = _get_sheets_service()
    sync_result = check_content_sync_status(write_report=True)
    if sync_result.get("blocked"):
        print("\n❌ 콘텐츠 동기화 보정 중단 — Notion 콘텐츠 DB 접근부터 복구해야 합니다.")
        return {"sheets_add": 0, "notion_add": 0, "conflicts": 0}

    existing_urls = _ensure_content_sheet_headers(service)
    appended_sheet = 0
    created_notion = 0

    for record in sync_result["only_notion"]:
        if _append_content_record_to_sheet(service, record, existing_urls):
            appended_sheet += 1

    for record in sync_result["only_sheet"]:
        if not record.url:
            continue
        _create_notion_page_from_record(notion, record)
        created_notion += 1

    final_result = check_content_sync_status(write_report=True)
    print(
        "\n✅ 콘텐츠 동기화 반영 완료"
        f" — sheets_add={appended_sheet}, notion_add={created_notion}, conflicts={len(final_result['field_conflict'])}"
    )
    return {
        "sheets_add": appended_sheet,
        "notion_add": created_notion,
        "conflicts": len(final_result["field_conflict"]),
    }


def _validate_and_cleanse_sheets():
    """행 수 차이를 참고로만 보여주고, 실제 판단은 URL 기반 diff로 위임."""
    notion_count = _count_complete_notion_pages()
    sheet_count = _count_sheet_rows()

    print(f"\n🔍 데이터 검증 — 노션: {notion_count}개 / 구글 시트: {sheet_count}개")

    if sheet_count < 0:
        print("❌ 시트 조회 실패 — 검증 생략")
        return

    if notion_count == sheet_count:
        print("✅ 데이터 수 일치 확인")
    else:
        diff = abs(notion_count - sheet_count)
        print(f"⚠️ 데이터 수 불일치 ({diff}개 차이) — URL 기준 diff 점검으로 이어집니다.")

    check_content_sync_status(write_report=True)


# ── 노션 → 구글 시트 일괄 동기화 ────────────────────────
def sync_sheets_from_notion(_skip_validate: bool = False):
    """노션 DB의 완성된 항목을 기준으로 시트에 백필."""
    notion = get_notion_client()
    records = _load_complete_notion_content_records(notion)
    total = len(records)
    print(f"\n📋 동기화 대상 후보: {total}개 페이지\n")
    if total == 0:
        print("동기화할 데이터가 없습니다.")
        return

    try:
        service = _get_sheets_service()
        existing_urls = _ensure_content_sheet_headers(service)
    except Exception as e:
        print(f"❌ 구글 시트 초기화 실패: {e}")
        return

    synced = skipped_dup = 0

    for i, record in enumerate(records, 1):
        label = f"[{i}/{total}] {record.title[:40]}"
        if record.url in existing_urls:
            skipped_dup += 1
            print(f"{label} — 스킵 (중복)")
            continue

        try:
            if _append_content_record_to_sheet(service, record, existing_urls):
                synced += 1
                print(f"{label} — ✓ 기록")
        except Exception as e:
            print(f"{label} — ❌ 실패: {e}")

        time.sleep(0.3)  # Sheets API 레이트 리밋 방지

    print(f"\n{'='*50}")
    print(f"✅ 동기화 완료 — 기록: {synced}개 / 중복 스킵: {skipped_dup}개")
    print(f"{'='*50}")

    if not _skip_validate:
        _validate_and_cleanse_sheets()


# ── 노션 클라이언트 ──────────────────────────────────────
def get_notion_client() -> NotionClient:
    return NotionClient(auth=NOTION_API_KEY, client=httpx.Client(timeout=30.0))


# ── 노션 DB에 새 페이지 생성 (텔레그램 → 노션) ───────────
def create_notion_page_from_url(notion: NotionClient, url: str) -> str:
    """URL로 노션 DB에 새 페이지 생성 후 page_id 반환"""
    return adapter_create_page_from_url(
        notion=notion,
        database_id=NOTION_DATABASE_ID,
        url=url,
    )


# ── 미처리 페이지 조회 ───────────────────────────────────
def get_unprocessed_pages(notion: NotionClient) -> list[dict]:
    """url이 있고 주제가 비어있는 노션 DB 페이지 반환"""
    return adapter_get_unprocessed_pages(
        database_id=NOTION_DATABASE_ID,
        notion_api_key=NOTION_API_KEY,
        httpx_module=httpx,
    )


# ── 노션 본문 블록 생성 ──────────────────────────────────
def _build_notion_body_blocks(
    analysis: dict,
    verified_name: str,
    verified_affiliation: str,
    role: str,
) -> list:
    """분석 결과를 노션 본문 블록 리스트로 변환 (시니어 애널리스트 리포트 형식)"""

    def rt(content: str) -> list:
        return [{"type": "text", "text": {"content": str(content)[:2000]}}]

    def heading2(title: str) -> dict:
        return {"object": "block", "type": "heading_2",
                "heading_2": {"rich_text": rt(title)}}

    def heading3(title: str) -> dict:
        return {"object": "block", "type": "heading_3",
                "heading_3": {"rich_text": rt(title)}}

    def paragraph(content) -> dict:
        text = str(content) if content else "정보 없음"
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": rt(text or "정보 없음")}}

    def bullet(content: str) -> dict:
        return {"object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": rt(str(content))}}

    def table_cell(content: str) -> list:
        return [{"type": "text", "text": {"content": str(content)[:2000]}}]

    blocks = []
    person = analysis.get("person", {}) or {}

    # 👤 출연자/강연자
    blocks.append(heading2("👤 출연자/강연자"))
    if role and role != "미상":
        person_header = f"{verified_affiliation} {role} / {verified_name}"
    else:
        person_header = f"{verified_affiliation} / {verified_name}"
    background = person.get("background") or "정보 없음"
    blocks.append(paragraph(f"{person_header}\n{background}"))

    # 📋 핵심 요약
    blocks.append(heading2("📋 핵심 요약"))
    key_summary = analysis.get("key_summary") or []
    if key_summary:
        for line in key_summary:
            if line:
                blocks.append(bullet(line))
    else:
        blocks.append(paragraph(analysis.get("summary") or "정보 없음"))

    # 📊 주요 경제 지표 및 수치
    blocks.append(heading2("📊 주요 경제 지표 및 수치"))
    indicators = analysis.get("economic_indicators") or []
    if indicators:
        for ind in indicators:
            ts = ind.get("timestamp", "")
            ts_str = f" ({ts})" if ts else ""
            line = f"{ind.get('indicator', '')}: {ind.get('value', '')} — {ind.get('context', '')}{ts_str}"
            blocks.append(bullet(line))
    else:
        blocks.append(paragraph("정보 없음"))

    # 📈 시장 모멘텀 분석
    blocks.append(heading2("📈 시장 모멘텀 분석"))
    momentum = analysis.get("market_momentum") or {}
    positives = momentum.get("positive") or []
    negatives = momentum.get("negative") or []
    if positives or negatives:
        if positives:
            blocks.append(heading3("✅ 긍정적 요인 (호재)"))
            for p in positives:
                blocks.append(bullet(p))
        if negatives:
            blocks.append(heading3("❌ 부정적 요인 (악재)"))
            for n in negatives:
                blocks.append(bullet(n))
    else:
        blocks.append(paragraph("정보 없음"))

    # 🔮 전문가의 시장 전망
    blocks.append(heading2("🔮 전문가의 시장 전망"))
    blocks.append(paragraph(analysis.get("market_outlook") or "정보 없음"))

    # 🏭 주요 섹터
    blocks.append(heading2("🏭 주요 섹터"))
    sectors = analysis.get("key_sectors") or []
    if sectors:
        for sector in sectors:
            if sector:
                blocks.append(bullet(sector))
    else:
        blocks.append(paragraph("정보 없음"))

    # 💼 추천 및 긍정 언급 자산 (있을 때만)
    assets = analysis.get("recommended_assets") or []
    if assets:
        blocks.append(heading2("💼 추천 및 긍정 언급 자산"))
        header_row = {
            "object": "block",
            "type": "table_row",
            "table_row": {
                "cells": [
                    table_cell("자산/종목명"),
                    table_cell("추천 및 긍정 평가 근거"),
                    table_cell("목표가/리스크"),
                    table_cell("타임스탬프"),
                ]
            },
        }
        data_rows = []
        for asset in assets:
            data_rows.append({
                "object": "block",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        table_cell(asset.get("name", "")),
                        table_cell(asset.get("reason", "")),
                        table_cell(asset.get("target_or_risk", "")),
                        table_cell(asset.get("timestamp", "")),
                    ]
                },
            })
        blocks.append({
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 4,
                "has_column_header": True,
                "has_row_header": False,
                "children": [header_row] + data_rows,
            },
        })

    # 🎓 강의 핵심 논리 분석 (is_lecture일 때만)
    if analysis.get("is_lecture"):
        lecture = analysis.get("lecture_analysis") or {}
        root_cause = lecture.get("root_cause", "")
        impact = lecture.get("impact", "")
        conclusion = lecture.get("conclusion_strategy", "")
        if any([root_cause, impact, conclusion]):
            blocks.append(heading2("🎓 강의 핵심 논리 분석"))
            if root_cause:
                blocks.append(heading3("📌 현상의 원인"))
                blocks.append(paragraph(root_cause))
            if impact:
                blocks.append(heading3("🔗 파급 효과"))
                blocks.append(paragraph(impact))
            if conclusion:
                blocks.append(heading3("🎯 최종 결론 및 대응 전략"))
                blocks.append(paragraph(conclusion))

    return blocks


# ── 결과를 노션 페이지에 기록 ────────────────────────────
def write_notion_result(
    notion: NotionClient,
    page_id: str,
    video_title: str,
    analysis: dict,
    verified_name: str,
    verified_affiliation: str,
    role: str,
    channel: str = "",
) -> tuple[str, str]:
    """노션 페이지를 업데이트하고 (person_str, timestamp) 반환"""
    return adapter_write_notion_result(
        notion=notion,
        page_id=page_id,
        video_title=video_title,
        analysis=analysis,
        verified_name=verified_name,
        verified_affiliation=verified_affiliation,
        role=role,
        now_fn=lambda: datetime.now(KST),
        build_blocks=_build_notion_body_blocks,
        channel=channel,
        out=print,
    )


# ── 인물 데이터베이스 ─────────────────────────────────────

def _query_person_db_exact(notion: NotionClient, name: str) -> Optional[dict]:
    """노션 인물 DB에서 이름 완전 일치 검색"""
    try:
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        name_prop = fields.get("name") or "이름"
        ptype = schema.get(name_prop, "title")
        name_filter = {"title": {"equals": name}} if ptype == "title" else {"rich_text": {"equals": name}}
        resp = _person_db_query({"filter": {"property": name_prop, **name_filter}})
        results = resp.get("results", [])
        return results[0] if results else None
    except Exception as e:
        print(f"  ⚠️ 인물 DB 완전 일치 검색 실패: {e}")
        return None


def _get_all_persons_from_db(notion: NotionClient) -> list[dict]:
    """인물 DB 전체 페이지 조회 (페이지네이션 처리)"""
    now = time.time()
    cached = _PERSON_DB_LIST_CACHE.get("pages")
    cts = _PERSON_DB_LIST_CACHE.get("ts", 0.0)
    if cached is not None and (now - cts) <= PERSON_DB_LIST_CACHE_SEC:
        return list(cached)

    pages: list[dict] = []
    cursor = None
    last_exc = None
    while True:
        body: dict = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        try:
            resp = _person_db_query(body)
        except Exception as e:
            last_exc = e
            print(f"  ⚠️ 인물 DB 전체 조회 실패: {e}")
            break
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    if not pages and last_exc is not None:
        raise last_exc
    _PERSON_DB_LIST_CACHE["ts"] = time.time()
    _PERSON_DB_LIST_CACHE["pages"] = list(pages)
    return pages


def _invalidate_person_db_cache():
    _PERSON_DB_LIST_CACHE["ts"] = 0.0
    _PERSON_DB_LIST_CACHE["pages"] = None


def _normalize_identity_text(text: str) -> str:
    """인물 식별용 텍스트 정규화."""
    s = _clean(text or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" -_|/.,:;")
    return s


def _compact_identity_text(text: str) -> str:
    """한글/영문/숫자만 남긴 비교용 문자열."""
    return re.sub(r"[^0-9a-z가-힣]", "", _normalize_identity_text(text))


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _compact_identity_text(a), _compact_identity_text(b)).ratio()


def _person_lookup_deps() -> PersonLookupDeps:
    return PersonLookupDeps(
        person_name_aliases=_PERSON_NAME_ALIASES,
        recent_match_store=_PERSON_RECENT_MATCH,
        person_match_cooldown_sec=PERSON_MATCH_COOLDOWN_SEC,
        normalize_identity_text=_normalize_identity_text,
        compact_identity_text=_compact_identity_text,
        similarity=_similarity,
        normalize_opinion_text=_normalize_opinion_text,
        extract_prop_text=_extract_prop_text,
        sanitize_person_fields=_sanitize_person_fields,
        query_person_db_exact=_query_person_db_exact,
        get_all_persons_from_db=_get_all_persons_from_db,
        get_person_prop=_get_person_prop,
        gemini_verify_same_person=_gemini_verify_same_person,
        list_person_blocks=lambda notion, page_id: notion.blocks.children.list(block_id=page_id).get("results", []),
        parse_opinions_from_person_blocks=_parse_opinions_from_person_blocks,
        dedup_opinions=_dedup_opinions,
        now_ts=time.time,
    )


def _person_name_key(name: str) -> str:
    return service_person_name_key(name, _person_lookup_deps())


def _person_aff_key(affiliation: str) -> str:
    return service_person_aff_key(affiliation, _person_lookup_deps())


def _person_lookup_keys(name: str, affiliation: str) -> list[str]:
    return service_person_lookup_keys(name, affiliation, _person_lookup_deps())


def _remember_person_match(name: str, affiliation: str, page_id: str):
    service_remember_person_match(name, affiliation, page_id, _person_lookup_deps())


def _forget_person_match(page_id: str):
    service_forget_person_match(page_id, _person_lookup_deps())


def _find_recent_person_match(all_persons: list[dict], name: str, affiliation: str) -> Optional[dict]:
    return service_find_recent_person_match(all_persons, name, affiliation, _person_lookup_deps())


def _candidate_hint_score(cand_name: str, cand_aff: str, channel: str, opinion: str, cand_props: dict) -> float:
    return service_candidate_hint_score(cand_name, cand_aff, channel, opinion, cand_props, _person_lookup_deps())


def _sanitize_person_fields(name: str, affiliation: str, role: str) -> tuple[str, str, str]:
    """인물 이름/소속/직책 표기 흔들림 정리."""
    name = _normalize_identity_text(name)
    affiliation = _normalize_identity_text(affiliation)
    role = _normalize_identity_text(role)

    if "/" in name:
        # "대표) / 이영수 대표" 같은 케이스에서 실제 이름 파트 우선
        name = name.rsplit("/", 1)[-1].strip()

    # 다중 인물 병합 입력("김장열, 이권희", "A & B")은 대표 이름 1개만 우선 선택
    multi_parts = [p.strip() for p in re.split(r"[,;&]| and |\|", name) if p.strip()]
    if len(multi_parts) >= 2:
        # 한국식 이름에 가까운 짧은 토큰 우선
        cand = next((p for p in multi_parts if re.fullmatch(r"[가-힣]{2,4}", p)), None)
        name = cand or multi_parts[0]

    # 이름 필드에 소속/설명 문구가 섞인 경우 앞/뒤 잡음 제거
    name = re.sub(r"^(대표|이사|본부장|센터장|팀장|상무|운영자)\)?\s*", "", name).strip()
    name = re.sub(r"\s*(소속|채널|경제|전문가)\s*$", "", name).strip()

    # "김단태 애널리스트"처럼 이름+직책이 붙은 경우 분리
    role_tokens = [
        "애널리스트", "이코노미스트", "대표", "대표이사", "본부장", "센터장",
        "팀장", "상무", "이사", "운용역", "전문가", "교수", "박사", "연구원", "기자",
    ]
    if role in ("", "미상", "정보 없음"):
        for tok in role_tokens:
            if name.endswith(f" {tok}"):
                name = name[: -(len(tok) + 1)].strip()
                role = tok
                break
            if name.endswith(tok) and len(name) > len(tok) + 1:
                # 공백 없이 붙은 케이스(예: 김단태애널리스트)
                prefix = name[: -len(tok)].strip()
                if 2 <= len(prefix) <= 8:
                    name = prefix
                    role = tok
                    break

    if affiliation.count("(") > affiliation.count(")"):
        # "와이스트릿 (투자 전문가" 같이 잘린 표기 보정
        left, _, right = affiliation.partition("(")
        if right and role in ("", "미상", "정보 없음"):
            role = right.strip(" )")
        affiliation = left.strip()

    if name in ("채널 운영자", "운영자"):
        name = "미상"
    if role in ("", "none", "null"):
        role = "미상"
    if affiliation in ("", "none", "null"):
        affiliation = "정보 없음"

    # 이름 컬럼은 이름만 남기기: trailing 직책/소속 단어 제거
    name = re.sub(
        r"\s*(애널리스트|이코노미스트|대표|대표이사|본부장|센터장|팀장|상무|이사|운용역|전문가|교수|박사|연구원|기자)$",
        "",
        name,
    ).strip()
    if len(name) > 20:
        name = name[:20].strip()

    name = _PERSON_NAME_ALIASES.get(name, name)

    return name, affiliation, role


def _extract_alias_real_name_pair(name: str) -> tuple[str, str]:
    text = _normalize_identity_text(name)
    if not text:
        return "", ""
    match = re.match(r"(.+?)\s*\(([^)]+)\)", text)
    if not match:
        return "", ""
    outer = _normalize_identity_text(match.group(1))
    inner = _normalize_identity_text(match.group(2))
    if _is_likely_korean_fullname(inner):
        return inner, outer
    if _is_likely_korean_fullname(outer):
        return outer, inner
    return "", ""


def _resolve_canonical_name_from_search(
    name: str,
    affiliation: str,
    role: str,
    channel: str = "",
) -> tuple[str, str]:
    cache_key = _compact_identity_text(f"{name}|{affiliation}|{role}|{channel}")
    cached = _PERSON_ALIAS_SEARCH_CACHE.get(cache_key)
    if cached:
        return cached

    queries = [
        f"{name} {channel} 본명" if channel else "",
        f"{name} {affiliation} 실명",
        f"{name} 경제 유튜버 본명",
        f"{name} 투자 유튜버 본명",
    ]
    items: list[dict] = []
    for query in queries:
        if not query.strip():
            continue
        got = _google_search_items(query, num=5)
        if got:
            items = got
            break
    snippets = " ".join(i.get("snippet", "") for i in items)
    if not snippets.strip():
        _PERSON_ALIAS_SEARCH_CACHE[cache_key] = (name, "")
        return name, ""

    result = _gemini_json(f"""아래 검색 결과를 보고 인물 활동명/본명을 정리하세요.

입력 이름: {name}
입력 소속: {affiliation}
입력 직책: {role}
입력 채널: {channel}

검색 결과:
{snippets[:2000]}

규칙:
- 입력 이름이 활동명/닉네임이면 canonical_name에는 공개적으로 확인 가능한 실명을 적으세요.
- alias_name에는 활동명/닉네임을 적으세요.
- 실명을 확인할 수 없으면 canonical_name은 입력 이름 그대로 두고 alias_name은 빈 문자열로 두세요.

JSON으로만 응답:
{{"canonical_name":"실명 또는 입력 이름", "alias_name":"활동명 또는 빈 문자열", "confidence":0.0}}""")

    canonical_name = name
    alias_name = ""
    if isinstance(result, dict):
        cand = _normalize_identity_text(result.get("canonical_name", "") or name)
        alias = _normalize_identity_text(result.get("alias_name", "") or "")
        conf = float(result.get("confidence", 0.0) or 0.0)
        if cand and conf >= 0.72:
            canonical_name = cand
            alias_name = alias
    _PERSON_ALIAS_SEARCH_CACHE[cache_key] = (canonical_name, alias_name)
    return canonical_name, alias_name


def _resolve_canonical_person_identity(
    name: str,
    affiliation: str = "",
    role: str = "",
    channel: str = "",
) -> tuple[str, str]:
    canonical_name, affiliation, role = _sanitize_person_fields(name, affiliation, role)
    alias_name = ""

    if canonical_name in _PERSON_CANONICAL_OVERRIDES:
        return _PERSON_CANONICAL_OVERRIDES[canonical_name], canonical_name

    if _is_alias_only_persona_name(canonical_name, channel=channel):
        return canonical_name, ""

    pair_name, pair_alias = _extract_alias_real_name_pair(canonical_name)
    if pair_name:
        return pair_name, pair_alias

    if _is_unknown_person_name(canonical_name):
        return canonical_name, alias_name

    # 3글자 이상 한국식 실명은 그대로 유지한다.
    if re.fullmatch(r"[가-힣]{3,4}", canonical_name):
        return canonical_name, alias_name

    # 활동명/짧은 이름은 검색으로 실명 보강을 시도한다.
    if len(canonical_name) <= 2 or not _is_likely_korean_fullname(canonical_name):
        resolved_name, resolved_alias = _resolve_canonical_name_from_search(
            canonical_name,
            affiliation,
            role,
            channel=channel,
        )
        resolved_name, _, _ = _sanitize_person_fields(resolved_name, affiliation, role)
        if resolved_name and resolved_name != canonical_name:
            alias_name = resolved_alias or canonical_name
            return resolved_name, alias_name

    return canonical_name, alias_name


def _is_likely_korean_fullname(name: str) -> bool:
    n = _normalize_identity_text(name)
    # 전형적인 한국 이름: 한글 2~4자 (예: 홍춘욱, 김단태, 미키김은 False 처리)
    return bool(re.fullmatch(r"[가-힣]{2,4}", n))


def _nickname_value(name: str, alias_name: str = "") -> str:
    alias = _normalize_identity_text(alias_name)
    if alias:
        return alias
    n = _normalize_identity_text(name)
    return n if n and not _is_likely_korean_fullname(n) else ""


def _is_alias_only_persona_name(
    name: str,
    channel: str = "",
    source_url: str = "",
) -> bool:
    normalized = _normalize_identity_text(name)
    if not normalized:
        return False
    if normalized in _PERSON_ALIAS_ONLY_CANONICALS:
        return True
    persona_suffixes = ("팀장", "실장", "소장", "대표", "원장", "센터장")
    if not any(normalized.endswith(suffix) for suffix in persona_suffixes):
        return False
    if source_url and _is_youtube_url(source_url):
        return True
    compact_name = _compact_identity_text(normalized)
    compact_channel = _compact_identity_text(channel)
    return bool(compact_name and compact_channel and compact_name in compact_channel)


def _override_person_source_url(
    name: str,
    channel: str = "",
    source_url: str = "",
) -> str:
    normalized = _normalize_identity_text(name)
    override = _PERSON_CHANNEL_URL_OVERRIDES.get(normalized, "")
    if override:
        return override
    if _is_alias_only_persona_name(normalized, channel=channel, source_url=source_url) and source_url and _is_youtube_url(source_url):
        return source_url
    return source_url


def _person_identity_gate_reasons(
    name: str,
    alias_name: str,
    channel: str,
    source_url: str,
) -> list[str]:
    reasons: list[str] = []
    normalized = _normalize_identity_text(name)
    compact = _compact_identity_text(name)
    if _is_unknown_person_name(normalized):
        reasons.append("실명 미확정")
        return reasons
    if _is_alias_only_persona_name(normalized, channel=channel, source_url=source_url):
        if not source_url or not _is_youtube_url(source_url):
            reasons.append("활동명 기반 인물이라 유튜브 채널 근거 링크 필요")
        return reasons
    if len(compact) <= 1:
        reasons.append("한 글자 이름으로 보여 실명 검증 필요")
    if alias_name:
        return reasons
    if len(normalized) <= 2 and source_url and not _is_youtube_url(source_url):
        reasons.append("짧은 이름인데 비유튜브 근거만 있어 실명 검증 필요")
    if "(" in normalized or ")" in normalized:
        reasons.append("이름에 별칭/괄호 표기가 남아 있음")
    if not _is_likely_korean_fullname(normalized) and channel:
        reasons.append("활동명 또는 채널 파생 이름으로 보여 실명 검증 필요")
    return reasons


def _is_unknown_person_name(name: str) -> bool:
    n = _normalize_identity_text(name)
    return n in ("", "미상", "정보 없음", "unknown", "n/a")


def _is_yes_text(text: str) -> bool:
    t = _normalize_identity_text(text)
    return t in ("y", "yes", "예", "네", "ㅇ", "응", "맞아", "맞습니다")


def _is_no_text(text: str) -> bool:
    t = _normalize_identity_text(text)
    return t in ("n", "no", "아니오", "아니요", "아니", "ㄴ", "아님")


def _person_review_key(name: str, affiliation: str, role: str) -> str:
    return "|".join([
        _compact_identity_text(name),
        _compact_identity_text(affiliation),
        _compact_identity_text(role),
    ])


def _load_person_review_memory():
    global _PERSON_REVIEW_MEMORY_LOADED, _PERSON_REVIEW_MEMORY
    if _PERSON_REVIEW_MEMORY_LOADED:
        return
    try:
        _PERSON_REVIEW_MEMORY = service_load_person_review_rows(FAILED_URL_QUEUE_PATH)
        if not _PERSON_REVIEW_MEMORY and os.path.exists(_PERSON_REVIEW_MEMORY_PATH):
            with open(_PERSON_REVIEW_MEMORY_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _PERSON_REVIEW_MEMORY = data
                    for key, item in data.items():
                        try:
                            service_save_person_review_row(
                                FAILED_URL_QUEUE_PATH,
                                key,
                                name=item.get("name", ""),
                                affiliation=item.get("affiliation", ""),
                                role=item.get("role", ""),
                                status=item.get("status", ""),
                                note=item.get("note", ""),
                                updated_at=item.get("updated_at", ""),
                            )
                        except Exception:
                            pass
    except Exception as e:
        logger.warning(f"검토 이력 로드 실패: {e}")
        _PERSON_REVIEW_MEMORY = {}
    _PERSON_REVIEW_MEMORY_LOADED = True


def _save_person_review_memory():
    try:
        for key, item in _PERSON_REVIEW_MEMORY.items():
            service_save_person_review_row(
                FAILED_URL_QUEUE_PATH,
                key,
                name=item.get("name", ""),
                affiliation=item.get("affiliation", ""),
                role=item.get("role", ""),
                status=item.get("status", ""),
                note=item.get("note", ""),
                updated_at=item.get("updated_at", ""),
            )
    except Exception as e:
        logger.warning(f"검토 이력 저장 실패: {e}")


def _mark_person_review(name: str, affiliation: str, role: str, status: str, note: str = ""):
    _load_person_review_memory()
    key = _person_review_key(name, affiliation, role)
    _PERSON_REVIEW_MEMORY[key] = {
        "name": name,
        "affiliation": affiliation,
        "role": role,
        "status": status,  # approved|corrected|skipped
        "note": note[:500] if note else "",
        "updated_at": datetime.now(KST).isoformat(),
    }
    _save_person_review_memory()


def _is_person_review_approved(name: str, affiliation: str, role: str) -> bool:
    _load_person_review_memory()
    key = _person_review_key(name, affiliation, role)
    item = service_get_person_review_row(FAILED_URL_QUEUE_PATH, key) or _PERSON_REVIEW_MEMORY.get(key)
    if not item:
        return False
    status = (item.get("status") or "").lower()
    if status not in ("approved", "corrected"):
        return False
    updated = item.get("updated_at", "")
    try:
        dt = datetime.fromisoformat(updated)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        age_days = (datetime.now(KST) - dt.astimezone(KST)).days
        return age_days <= _PERSON_REVIEW_RETENTION_DAYS
    except Exception:
        return True


def _load_failed_url_queue():
    global _FAILED_URL_QUEUE_LOADED, _FAILED_URL_QUEUE
    if _FAILED_URL_QUEUE_LOADED:
        return
    _FAILED_URL_QUEUE = service_load_failed_url_queue(FAILED_URL_QUEUE_PATH, logger=logger)
    _FAILED_URL_QUEUE_LOADED = True


def _save_failed_url_queue():
    service_save_failed_url_queue(FAILED_URL_QUEUE_PATH, _FAILED_URL_QUEUE, logger=logger)


def _enqueue_failed_url(url: str, reason: str):
    global _FAILED_URL_QUEUE
    _load_failed_url_queue()
    _FAILED_URL_QUEUE = service_enqueue_failed_url(
        _FAILED_URL_QUEUE,
        url,
        reason,
        FailedQueueDeps(
            queue_path=FAILED_URL_QUEUE_PATH,
            retry_max=FAILED_URL_RETRY_MAX,
            retry_interval_min=FAILED_URL_RETRY_INTERVAL_MIN,
            kst=KST,
            get_notion_client=get_notion_client,
            extract_video_id=extract_video_id,
            find_duplicate_pages=find_duplicate_pages,
            is_incomplete=_is_incomplete,
            create_notion_page_from_url=create_notion_page_from_url,
            save_queue=lambda path, queue: service_save_failed_url_queue(path, queue, logger=logger),
            event_log_path=OPS_EVENT_LOG_PATH,
            logger=logger,
        ),
    )


def _dequeue_failed_url(url: str):
    global _FAILED_URL_QUEUE
    _load_failed_url_queue()
    _FAILED_URL_QUEUE = service_dequeue_failed_url(
        _FAILED_URL_QUEUE,
        url,
        FailedQueueDeps(
            queue_path=FAILED_URL_QUEUE_PATH,
            retry_max=FAILED_URL_RETRY_MAX,
            retry_interval_min=FAILED_URL_RETRY_INTERVAL_MIN,
            kst=KST,
            get_notion_client=get_notion_client,
            extract_video_id=extract_video_id,
            find_duplicate_pages=find_duplicate_pages,
            is_incomplete=_is_incomplete,
            create_notion_page_from_url=create_notion_page_from_url,
            save_queue=lambda path, queue: service_save_failed_url_queue(path, queue, logger=logger),
            event_log_path=OPS_EVENT_LOG_PATH,
            logger=logger,
        ),
    )


def _retry_failed_urls_once():
    """실패 URL 큐를 재주입해 notion_poller가 다시 처리하도록 함(저비용)."""
    global _FAILED_URL_QUEUE
    _load_failed_url_queue()
    _FAILED_URL_QUEUE, stats = service_retry_failed_urls_once(
        _FAILED_URL_QUEUE,
        FailedQueueDeps(
            queue_path=FAILED_URL_QUEUE_PATH,
            retry_max=FAILED_URL_RETRY_MAX,
            retry_interval_min=FAILED_URL_RETRY_INTERVAL_MIN,
            kst=KST,
            get_notion_client=get_notion_client,
            extract_video_id=extract_video_id,
            find_duplicate_pages=find_duplicate_pages,
            is_incomplete=_is_incomplete,
            create_notion_page_from_url=create_notion_page_from_url,
            save_queue=lambda path, queue: service_save_failed_url_queue(path, queue, logger=logger),
            event_log_path=OPS_EVENT_LOG_PATH,
            logger=logger,
        ),
    )
    return stats


def _was_briefing_sent(briefing_type: str, dispatch_key: str) -> bool:
    return service_was_briefing_sent(FAILED_URL_QUEUE_PATH, briefing_type, dispatch_key)


def _claim_briefing_dispatch(briefing_type: str, dispatch_key: str, sent_at: str) -> bool:
    return service_claim_briefing_dispatch(FAILED_URL_QUEUE_PATH, briefing_type, dispatch_key, sent_at)


def _update_briefing_dispatch_status(
    briefing_type: str,
    dispatch_key: str,
    status: str,
    sent_at: str,
) -> None:
    service_update_briefing_dispatch_status(
        FAILED_URL_QUEUE_PATH,
        briefing_type,
        dispatch_key,
        sent_at=sent_at,
        status=status,
    )


def _looks_like_person_name_input(text: str) -> bool:
    t = (text or "").strip()
    if not t or extract_youtube_url(t):
        return False
    # 한국어 이름/닉네임, 영문 이름 모두 허용
    if re.fullmatch(r"[가-힣A-Za-z][가-힣A-Za-z\s]{1,19}", t):
        return True
    return False


def _looks_like_keyword_input(text: str) -> bool:
    t = (text or "").strip()
    if not t or extract_youtube_url(t):
        return False
    # 키워드는 최소 2개 토큰 또는 길이 6자 이상인 설명형 텍스트를 유효로 본다.
    if len(t.split()) >= 2:
        return True
    return len(t) >= 6


def _find_conflicting_candidates(
    notion: NotionClient, name: str, affiliation: str, role: str, opinion: str
) -> list[str]:
    return service_find_conflicting_candidates(
        notion, name, affiliation, role, opinion, _person_lookup_deps()
    )


def _score_person_confidence(
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str,
    yt_conf: float,
    conflict_count: int,
    non_econ: bool,
) -> tuple[float, str]:
    return service_score_person_confidence(
        affiliation,
        role,
        career,
        expertise,
        source_url,
        yt_conf,
        conflict_count,
        non_econ,
        PERSON_CONFIDENCE_MIN,
        PERSON_CONFIDENCE_STRICT_MIN,
    )


def _is_uncertain_person(
    notion: NotionClient,
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str,
    opinion: str,
) -> tuple[bool, list[str], float, str]:
    return service_is_uncertain_person(
        notion,
        name,
        affiliation,
        role,
        career,
        expertise,
        source_url,
        opinion,
        UncertainPersonDeps(
            is_missing_person_value=_is_missing_person_value,
            is_non_economic_profile=_is_non_economic_profile,
            needs_person_category_review=_needs_person_category_review,
            is_likely_korean_fullname=_is_likely_korean_fullname,
            is_alias_only_persona_name=lambda person_name, source_url="": _is_alias_only_persona_name(
                person_name, source_url=source_url
            ),
            find_conflicting_candidates=_find_conflicting_candidates,
            is_youtube_url=_is_youtube_url,
            score_person_confidence=lambda aff, rl, cr, ex, src, yt, cnt, non: _score_person_confidence(
                name, aff, rl, cr, ex, src, yt, cnt, non
            ),
            person_confidence_min=PERSON_CONFIDENCE_MIN,
        ),
    )


def _non_economic_person_suspects(all_persons: list[dict]) -> list[dict]:
    return service_non_economic_person_suspects(
        all_persons,
        NonEconomicSuspectsDeps(
            get_person_prop=_get_person_prop,
            is_non_economic_profile=_is_non_economic_profile,
            needs_person_category_review=_needs_person_category_review,
        ),
    )


def _get_all_persons_from_db_retry(notion: NotionClient, attempts: int = 3, sleep_sec: float = 1.0) -> list[dict]:
    last_exc = None
    for idx in range(attempts):
        try:
            return _get_all_persons_from_db(notion)
        except Exception as exc:
            last_exc = exc
            if idx < attempts - 1:
                time.sleep(sleep_sec)
    raise last_exc


def _notify_uncertain_person(name: str, affiliation: str, role: str, reasons: list[str]):
    ticket = ""
    page_id = ""
    if isinstance(role, dict):
        # backward safety (unused)
        role = role.get("role", "미상")
    msg = (
        "⚠️ <b>인물 데이터 수동 수정 요청</b>\n\n"
        f"- 이름: {escape_html(name)}\n"
        f"- 소속: {escape_html(affiliation)}\n"
        f"- 직책: {escape_html(role)}\n"
        f"- 사유: {escape_html('; '.join(reasons))}\n\n"
        "이 항목은 자동 보류되었습니다. 직접 확인 후 수정해주세요."
    )
    try:
        send_telegram_review_message(msg)
    except Exception:
        pass


def _register_person_ticket(page_id: str, name: str, affiliation: str, role: str) -> str:
    review_key = _person_review_key(name, affiliation, role)
    # 동일 건이 이미 대기 중이면 기존 티켓 재사용
    for t, v in _pending_telegram.items():
        if v.get("review_key") == review_key:
            return t

    seed = _compact_identity_text(page_id)[:10] if page_id else _compact_identity_text(name)[:10]
    ticket = f"P{seed or int(time.time())}"
    _pending_telegram[ticket] = {
        "page_id": page_id,
        "name": name,
        "affiliation": affiliation,
        "role": role,
        "review_key": review_key,
        "created_at": datetime.now(KST).isoformat(),
    }
    return ticket


def _person_review_reply_markup(ticket: str) -> dict:
    return {
        "inline_keyboard": [[
            {"text": "✅ 예(저장)", "callback_data": f"person_decide:yes:{ticket}"},
            {"text": "❌ 아니오(보류)", "callback_data": f"person_decide:no:{ticket}"},
        ]]
    }


def _unknown_person_reply_markup() -> dict:
    return {
        "inline_keyboard": [[
            {"text": "✅ 예(미상 저장)", "callback_data": "unknown_decide:yes"},
            {"text": "❌ 아니오(이름 입력)", "callback_data": "unknown_decide:no"},
        ]]
    }


def _notion_page_url(page_id: str) -> str:
    pid = (page_id or "").replace("-", "")
    return f"https://www.notion.so/{pid}" if pid else ""


def _build_uncertain_person_evidence(page_id: str) -> str:
    """수동 검토용 근거 텍스트 생성(링크/요약)."""
    if not page_id:
        return "근거 자료: 페이지 링크 없음(신규 후보)"
    try:
        notion = get_notion_client()
        page = notion.pages.retrieve(page_id=page_id)
        name = _get_person_prop(page, "이름")
        source_url = _get_person_prop(page, "근거 링크")
        latest_channel = _get_person_prop(page, "최근 채널")
        latest_opinion = _get_person_prop(page, "최근 발언")
        notion_url = _notion_page_url(page_id)

        lines = ["근거 자료"]
        if notion_url:
            lines.append(f"- 노션 페이지: {notion_url}")
        if source_url:
            label = "원본 영상" if _is_youtube_url(source_url) else "근거 링크"
            lines.append(f"- {label}: {source_url}")

        if name:
            try:
                content_pages = _find_content_pages_for_person(notion, name)
            except Exception:
                content_pages = []
            if content_pages:
                def _page_sort_key(p: dict) -> str:
                    props = p.get("properties", {})
                    return _get_rich_text(props.get("처리일시", {})) or ""

                latest_pages = sorted(content_pages, key=_page_sort_key, reverse=True)[:2]
                for idx, content_page in enumerate(latest_pages, start=1):
                    props = content_page.get("properties", {})
                    video_url = props.get("URL", {}).get("url") or ""
                    title_items = (props.get("콘텐츠 제목", {}) or {}).get("title") or []
                    title = title_items[0]["text"]["content"] if title_items else ""
                    channel = _get_rich_text(props.get("채널명", {}))
                    if video_url:
                        suffix = []
                        if title:
                            suffix.append(f"제목: {title}")
                        if channel:
                            suffix.append(f"채널: {channel}")
                        detail = " / ".join(suffix)
                        if detail:
                            lines.append(f"- 관련 영상 {idx}: {video_url} ({detail})")
                        else:
                            lines.append(f"- 관련 영상 {idx}: {video_url}")
        if latest_channel:
            lines.append(f"- 최근 채널: {latest_channel}")
        if latest_opinion:
            lines.append(f"- 최근 발언: {latest_opinion[:180]}")
        if len(lines) == 1:
            lines.append("- 수집된 근거 링크/발언이 없습니다.")
        return "\n".join(lines)
    except Exception:
        notion_url = _notion_page_url(page_id)
        if notion_url:
            return f"근거 자료\n- 노션 페이지: {notion_url}"
        return "근거 자료 수집 실패"


def _notify_uncertain_person_with_ticket(
    page_id: str,
    name: str,
    affiliation: str,
    role: str,
    reasons: list[str],
):
    # 동일 인물 티켓이 이미 pending 상태면 재전송 생략
    review_key = _person_review_key(name, affiliation, role)
    for t, v in _pending_telegram.items():
        if v.get("review_key") == review_key:
            print(f"  [NOTIFY] 이미 대기 중인 티켓 있음({t}), 재전송 생략: {name}")
            return
    ticket = _register_person_ticket(page_id, name, affiliation, role)
    evidence = _build_uncertain_person_evidence(page_id)
    msg = (
        "⚠️ <b>인물 데이터 수동 수정 요청</b>\n\n"
        f"- 이름: {escape_html(name)}\n"
        f"- 소속: {escape_html(affiliation)}\n"
        f"- 직책: {escape_html(role)}\n"
        f"- 사유: {escape_html('; '.join(reasons))}\n\n"
        f"{escape_html(evidence)}\n\n"
        "현재 정보로 저장하시겠습니까?\n"
        "(아래 버튼에서 선택)"
    )
    try:
        send_telegram_review_message(msg, reply_markup=_person_review_reply_markup(ticket))
    except Exception:
        pass


def _gemini_verify_same_person(
    name: str,
    affiliation: str,
    role: str,
    candidates: list[dict],
) -> Optional[dict]:
    return service_gemini_verify_same_person(
        name,
        affiliation,
        role,
        candidates,
        GeminiSamePersonDeps(
            get_person_prop=_get_person_prop,
            gemini_json=_gemini_json,
            print_fn=print,
        ),
    )


def find_person_in_notion_db(
    notion: NotionClient,
    name: str,
    affiliation: str,
    role: str,
    channel: str = "",
    opinion: str = "",
) -> Optional[dict]:
    return service_find_person_in_notion_db(
        notion, name, affiliation, role, channel, opinion, _person_lookup_deps()
    )


def _get_person_prop(page: dict, prop_name: str) -> str:
    """인물 DB 페이지에서 속성값 텍스트 반환"""
    props = page.get("properties", {})
    prop = props.get(prop_name)
    if prop:
        return _extract_prop_text(prop)

    aliases = {
        "이름": ["Name", "성명"],
        "소속": ["Affiliation", "회사", "기관"],
        "직책": ["Role", "직위", "직무"],
        "주요 경력": ["경력", "Career"],
        "전문 분야": ["전문분야", "Expertise", "분야"],
        "등장 횟수": ["등장횟수", "Appearance Count", "Count", "횟수"],
        "근거 링크": ["출처 링크", "Reference URL", "URL", "링크"],
        "신뢰도 점수": ["Confidence Score", "confidence_score"],
        "신뢰도 상태": ["Confidence Status", "identity_status"],
        "닉네임": ["별칭", "Alias", "alias_name"],
        "인물 유형": ["person_types", "Person Types", "타입", "유형"],
        "TrustScore": ["trust_score_total", "Trust Score"],
        "TrustScore 밴드": ["trust_score_band", "Trust Score Band"],
        "TrustScore 신뢰도": ["trust_score_confidence", "Trust Score Confidence"],
        "해결 claim 수": ["resolved_claim_count", "Resolved Claim Count"],
        "대기 claim 수": ["pending_claim_count", "Pending Claim Count"],
        "방향 적중률": ["direction_accuracy", "Direction Accuracy"],
        "알파 점수": ["alpha_score", "Alpha Score"],
        "근거 제시 점수": ["source_transparency_score", "Source Transparency Score"],
        "입장 번복 플래그 수": ["contradiction_flag_count", "Contradiction Flag Count"],
        "마지막 TrustScore 갱신": ["last_trustscore_updated_at", "Last TrustScore Updated At"],
    }
    for alt in aliases.get(prop_name, []):
        if alt in props:
            return _extract_prop_text(props[alt])
    return ""


def _get_person_count(page: dict) -> int:
    props = page.get("properties", {})
    for key in ("등장 횟수", "등장횟수", "Appearance Count", "Count", "횟수"):
        if key not in props:
            continue
        prop = props.get(key, {})
        if prop.get("type") == "number":
            return prop.get("number") or 0
        raw = _extract_prop_text(prop)
        try:
            return int(float(raw.strip()))
        except Exception:
            continue
    return 0


def collect_person_info_from_search(name: str, affiliation: str, role: str, channel: str = "") -> dict:
    """검색으로 인물 정보 수집. {affiliation, role, career, expertise, source_url} 반환"""
    queries = [
        f"{name} {affiliation} {role} 경력 전문분야",
        f"{name} {role} 경제 전문가",
        f"{name} {affiliation} 소속 직책",
    ]
    if channel:
        queries.append(f"{name} {channel} 인터뷰")

    items: list[dict] = []
    for q in queries:
        got = _google_search_items(q, num=5)
        if got:
            items = got
            break

    snippets = " ".join(i.get("snippet", "") for i in items)
    source_url = _override_person_source_url(
        name,
        channel=channel,
        source_url=_pick_best_source_link(items, name, affiliation),
    )
    if not snippets:
        return {
            "affiliation": affiliation or "정보 없음",
            "role": role or "미상",
            "career": "정보 없음",
            "expertise": "정보 없음",
            "source_url": source_url,
        }
    result = _gemini_json(f"""검색 결과를 바탕으로 {name}({affiliation} {role})의 정보를 추출하세요.

검색 결과:
{snippets[:2000]}

JSON으로만 응답:
{{
  "affiliation": "가장 가능성 높은 소속. 모르면 '정보 없음'",
  "role": "가장 가능성 높은 직책. 모르면 '미상'",
  "career": "주요 경력 2~3문장. 모르면 '정보 없음'",
  "expertise": "전문 분야 키워드 나열. 모르면 '정보 없음'"
}}""")
    extracted = {
        "affiliation": affiliation or "정보 없음",
        "role": role or "미상",
        "career": "정보 없음",
        "expertise": "정보 없음",
        "source_url": source_url,
    }
    if result and isinstance(result, dict):
        extracted = {
            "affiliation": result.get("affiliation", affiliation or "정보 없음"),
            "role": result.get("role", role or "미상"),
            "career": result.get("career", "정보 없음"),
            "expertise": result.get("expertise", "정보 없음"),
            "source_url": source_url,
        }

    # 동명이인 필터: 비경제 직업 감지 시 경제 맥락으로 재검색
    if _is_non_economic_profile(
        extracted.get("affiliation", ""),
        extracted.get("role", ""),
        extracted.get("career", ""),
        extracted.get("expertise", ""),
        snippets,
    ):
        strict_queries = [
            f"{name} 경제 금융 투자 애널리스트",
            f"{name} {channel} 경제" if channel else f"{name} 경제 전문가",
            f"{name} 증권 리서치",
        ]
        strict_items: list[dict] = []
        for q in strict_queries:
            got = _google_search_items(q, num=5)
            if got:
                strict_items = got
                break
        strict_snippets = " ".join(i.get("snippet", "") for i in strict_items)
        strict_source = _override_person_source_url(
            name,
            channel=channel,
            source_url=_pick_best_source_link(strict_items, name, affiliation) or source_url,
        )

        strict_result = _gemini_json(f"""아래 검색 결과에서 '경제/금융/투자 분야 인물' 기준으로만 {name} 정보를 추출하세요.
경제와 무관한 직업 정보(연예/스포츠/의료/법조 등)라면 '미상/정보 없음'으로 반환하세요.

검색 결과:
{strict_snippets[:2000]}

JSON으로만 응답:
{{
  "affiliation": "경제 관련 소속. 불확실하면 '정보 없음'",
  "role": "경제 관련 직책. 불확실하면 '미상'",
  "career": "경제 관련 경력. 불확실하면 '정보 없음'",
  "expertise": "경제 관련 전문분야. 불확실하면 '정보 없음'"
}}""")
        if strict_result and isinstance(strict_result, dict):
            extracted = {
                "affiliation": strict_result.get("affiliation", "정보 없음"),
                "role": strict_result.get("role", "미상"),
                "career": strict_result.get("career", "정보 없음"),
                "expertise": strict_result.get("expertise", "정보 없음"),
                "source_url": strict_source,
            }
            if _is_non_economic_profile(
                extracted.get("affiliation", ""),
                extracted.get("role", ""),
                extracted.get("career", ""),
                extracted.get("expertise", ""),
                strict_snippets,
            ):
                # 재검색 후에도 비경제면 동명이인 의심으로 보류되도록 불확실값 반환
                extracted = {
                    "affiliation": "정보 없음",
                    "role": "미상",
                    "career": "정보 없음",
                    "expertise": "정보 없음",
                    "source_url": strict_source,
                    "homonym_suspected": True,
                }
        else:
            extracted = {
                "affiliation": "정보 없음",
                "role": "미상",
                "career": "정보 없음",
                "expertise": "정보 없음",
                "source_url": strict_source,
                "homonym_suspected": True,
            }

    return extracted


def _is_missing_person_value(value: str) -> bool:
    return not value or str(value).strip() in ("정보 없음", "미상", "")


def _needs_manual_person_input(affiliation: str, role: str, career: str, expertise: str) -> bool:
    """자동 수집 후에도 핵심 인물 정보가 부족한지 판단."""
    missing_core = _is_missing_person_value(affiliation) or _is_missing_person_value(role)
    missing_profile = _is_missing_person_value(career) and _is_missing_person_value(expertise)
    return missing_core or missing_profile


_ECON_POSITIVE_KEYWORDS = {
    "경제", "금융", "투자", "증권", "애널리스트", "이코노미스트", "리서치",
    "자산운용", "펀드", "매크로", "주식", "채권", "etf", "시장", "거시", "통화",
}
_ALLOWED_PERSON_CATEGORY_KEYWORDS = _ECON_POSITIVE_KEYWORDS | {
    # 정치/국제 정세
    "정치", "국제", "국제정세", "외교", "안보", "정책", "국회의원", "의원", "장관", "대통령", "총리",
    # 방송/미디어
    "방송", "방송인", "앵커", "기자", "아나운서", "mc", "pd", "진행자", "평론가",
    # 유튜브/크리에이터
    "유튜브", "youtube", "유튜버", "크리에이터", "채널", "채널운영자", "운영자",
}
_ECON_NEGATIVE_KEYWORDS = {
    "배우", "가수", "아이돌", "축구", "야구", "농구", "감독", "개그맨", "코미디언",
    "요리사", "셰프", "의사", "치과의사", "피부과", "변호사", "판사", "검사",
    "소설가", "작가", "모델", "댄서", "배우자", "연예인",
}


def _keyword_hits(text: str, keywords: set[str]) -> int:
    t = _normalize_identity_text(text)
    return sum(1 for k in keywords if k in t)


def _is_non_economic_profile(affiliation: str, role: str, career: str, expertise: str, snippets: str = "") -> bool:
    text = " ".join([affiliation or "", role or "", career or "", expertise or "", snippets or ""])
    # 허용 카테고리(경제/정치/국제정세/방송인/유튜버) 키워드가 있으면 비경제로 보지 않음
    if _keyword_hits(text, _ALLOWED_PERSON_CATEGORY_KEYWORDS) > 0:
        return False
    pos = _keyword_hits(text, _ECON_POSITIVE_KEYWORDS)
    neg = _keyword_hits(text, _ECON_NEGATIVE_KEYWORDS)
    # 경제 키워드가 거의 없고, 비경제 키워드가 많은 경우 동명이인 의심
    if pos == 0 and neg >= 1:
        return True
    if neg >= pos + 2:
        return True
    return False


def _needs_person_category_review(
    affiliation: str, role: str, career: str, expertise: str, snippets: str = ""
) -> bool:
    """허용 카테고리(경제/정치/국제정세/방송인/유튜버) 외 인물인지 점검."""
    text = " ".join([affiliation or "", role or "", career or "", expertise or "", snippets or ""])
    normalized = _normalize_identity_text(text)
    if not normalized:
        return False
    # 정보가 거의 없는 경우는 별도 불확실 로직에서 처리
    if _is_missing_person_value(affiliation) and _is_missing_person_value(role):
        return False
    return _keyword_hits(text, _ALLOWED_PERSON_CATEGORY_KEYWORDS) == 0


def _warn_manual_person_input_needed(name: str, affiliation: str, role: str, career: str, expertise: str):
    msg = (
        f"  [PERSON_DB] ⚠️ 수기 입력 필요: name={name!r}, affiliation={affiliation!r}, role={role!r}, "
        f"career={career[:40]!r}, expertise={expertise[:40]!r}"
    )
    print(msg)
    logger.warning(msg)


def _update_existing_person_record(
    notion: NotionClient,
    existing_page: dict,
    affiliation: str,
    role: str,
) -> tuple[str, str, str, str, str, str, int]:
    """기존 인물 페이지 기준으로 값/카운트 업데이트 후 반환."""
    page_id = existing_page["id"]
    saved_affiliation = _get_person_prop(existing_page, "소속")
    saved_role = _get_person_prop(existing_page, "직책")
    career = _get_person_prop(existing_page, "주요 경력")
    expertise = _get_person_prop(existing_page, "전문 분야")
    source_url = _get_person_prop(existing_page, "근거 링크")
    if saved_affiliation and saved_affiliation not in ("미상", "정보 없음"):
        affiliation = saved_affiliation
    if saved_role and saved_role not in ("미상", "정보 없음"):
        role = saved_role

    current_count = _get_person_count(existing_page)
    new_count = current_count + 1
    try:
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        count_prop = fields.get("count") or "등장 횟수"
        count_type = schema.get(count_prop, "number")
        if count_type == "number":
            count_payload = {"number": new_count}
        else:
            count_payload = _build_person_prop_value(
                count_type, str(new_count), lambda t: [{"text": {"content": str(t)[:2000]}}], lambda t: []
            )
        upd = notion.pages.update(
            page_id=page_id,
            properties={count_prop: count_payload},
        )
        saved_cnt = _get_person_count(upd)
        print(f"  [PERSON_DB] 기존 인물 발견 → 등장 횟수 {current_count} → {new_count} (응답값={saved_cnt})")
        if saved_cnt != new_count:
            print(f"  [PERSON_DB] ⚠️ 등장 횟수 불일치: 요청={new_count}, 저장={saved_cnt}")
    except Exception as e:
        logger.error(f"  [PERSON_DB] 등장 횟수 업데이트 실패: {e}", exc_info=True)
        new_count = current_count

    return page_id, affiliation, role, career, expertise, source_url, new_count


def _set_person_source_url_if_missing(notion: NotionClient, page_id: str, source_url: str):
    """인물 페이지에 근거 링크가 비어 있거나 더 좋은 유튜브 근거가 있으면 저장."""
    if not source_url:
        return
    try:
        page = notion.pages.retrieve(page_id=page_id)
        current = _get_person_prop(page, "근거 링크")
        person_name = _get_person_prop(page, "이름")
        channel = _get_person_prop(page, "대표 채널") or _get_person_prop(page, "최근 채널")
        preferred_url = _override_person_source_url(person_name, channel=channel, source_url=source_url)
        should_upgrade = bool(
            preferred_url
            and preferred_url != current
            and (not current or not _is_youtube_url(current))
        )
        if current and not should_upgrade:
            return
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        prop_name = fields.get("source_url")
        if not prop_name:
            return
        ptype = schema.get(prop_name, "url")
        payload = _build_person_prop_value(
            ptype, preferred_url or source_url,
            lambda t: [{"text": {"content": str(t)[:2000]}}],
            lambda t: [],
        )
        notion.pages.update(page_id=page_id, properties={prop_name: payload})
    except Exception:
        pass


def _backfill_person_profile(
    notion: NotionClient,
    page_id: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str,
):
    """인물 페이지의 비어 있는 핵심 프로필만 보수적으로 보강."""
    try:
        page = notion.pages.retrieve(page_id=page_id)
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        props = {}
        field_map = [
            ("affiliation", "소속", affiliation),
            ("role", "직책", role),
            ("career", "주요 경력", career),
            ("expertise", "전문 분야", expertise),
        ]
        for field_key, logical_name, value in field_map:
            if _is_missing_person_value(value):
                continue
            current = _get_person_prop(page, logical_name)
            if not _is_missing_person_value(current):
                continue
            prop_name = fields.get(field_key)
            if not prop_name:
                continue
            ptype = schema.get(prop_name, "rich_text")
            props[prop_name] = _build_person_prop_value(
                ptype,
                value,
                lambda t: [{"text": {"content": str(t)[:2000]}}],
                lambda t: [],
            )
        if props:
            notion.pages.update(page_id=page_id, properties=props)
        _set_person_source_url_if_missing(notion, page_id, source_url)
    except Exception:
        pass


def _update_person_review_status(
    notion: NotionClient,
    page_id: str,
    confidence_status: str,
) -> None:
    schema = _get_person_db_schema(notion)
    fields = _resolve_person_fields(schema)
    status_prop = fields.get("confidence_status")
    if not status_prop:
        return
    ptype = schema.get(status_prop, "rich_text")
    payload = _build_person_prop_value(
        ptype,
        confidence_status,
        lambda t: [{"text": {"content": str(t)[:2000]}}],
        lambda t: [],
    )
    notion.pages.update(page_id=page_id, properties={status_prop: payload})


def _weekly_person_identity_audit(notion: NotionClient) -> dict:
    all_persons = _get_all_persons_from_db(notion)
    suspicious = 0
    canonicalized = 0
    archived = 0
    samples: list[str] = []
    for person in all_persons:
        page_id = person.get("id", "")
        name = _get_person_prop(person, "이름")
        aff = _get_person_prop(person, "소속")
        role = _get_person_prop(person, "직책")
        source_url = _get_person_prop(person, "근거 링크")
        channel = _get_person_prop(person, "대표 채널") or _get_person_prop(person, "최근 채널")

        canonical_name, alias_name = _resolve_canonical_person_identity(name, aff, role, channel=channel)
        if canonical_name != name or (_nickname_value(canonical_name, alias_name) != _get_person_prop(person, "닉네임")):
            _sync_person_identity_props(notion, page_id, canonical_name, alias_name)
            canonicalized += 1

        preferred_source_url = _override_person_source_url(canonical_name, channel=channel, source_url=source_url)
        if preferred_source_url and preferred_source_url != source_url:
            _set_person_source_url_if_missing(notion, page_id, preferred_source_url)
            source_url = preferred_source_url

        reasons = _person_identity_gate_reasons(canonical_name, alias_name, channel, source_url)
        if reasons:
            suspicious += 1
            if len(samples) < 10:
                samples.append(f"{canonical_name}: {reasons[0]}")
            try:
                _update_person_review_status(notion, page_id, "검토 필요")
            except Exception:
                pass
            if _is_unknown_person_name(canonical_name) or len(_compact_identity_text(canonical_name)) <= 1:
                try:
                    notion.pages.update(page_id=page_id, archived=True)
                    archived += 1
                except Exception:
                    pass
        elif _is_alias_only_persona_name(canonical_name, channel=channel, source_url=source_url):
            try:
                _update_person_review_status(notion, page_id, "활동명 확정")
            except Exception:
                pass
    _invalidate_person_db_cache()
    return {
        "suspicious": suspicious,
        "canonicalized": canonicalized,
        "archived": archived,
        "samples": samples,
    }


def _sync_person_summary_props(
    notion: NotionClient,
    page_id: str,
    name: str,
    body_fields: dict[str, str],
    source_url: str = "",
    confidence_score: str = "",
    confidence_status: str = "",
    alias_name: str = "",
):
    """인물 페이지 본문 요약값을 Notion 속성(컬럼)에도 동기화."""
    try:
        _ensure_person_db_extra_columns(notion)
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)

        def rt(text: str) -> list:
            return [{"text": {"content": str(text)[:2000]}}]

        def ms(_: str) -> list:
            return []

        updates: dict = {}
        for logical, value in (
            ("latest_date", body_fields.get("latest_date", "")),
            ("latest_channel", body_fields.get("latest_channel", "")),
            ("latest_opinion", body_fields.get("latest_opinion", "")),
            ("dominant_channel", body_fields.get("dominant_channel", "")),
            ("top_channels", body_fields.get("top_channels", "")),
            ("consistency_summary", body_fields.get("consistency_summary", "")),
            ("confidence_score", confidence_score),
            ("confidence_status", confidence_status),
            ("nickname", _nickname_value(name, alias_name)),
        ):
            prop_name = fields.get(logical)
            if not prop_name:
                continue
            ptype = schema.get(prop_name, "rich_text")
            updates[prop_name] = _build_person_prop_value(ptype, value, rt, ms)

        src_prop = fields.get("source_url")
        if src_prop and source_url:
            ptype = schema.get(src_prop, "url")
            updates[src_prop] = _build_person_prop_value(ptype, source_url, rt, ms)

        if updates:
            notion.pages.update(page_id=page_id, properties=updates)
    except Exception as e:
        print(f"  ⚠️ 인물 요약 속성 동기화 실패: {e}")


def _sync_person_identity_props(
    notion: NotionClient,
    page_id: str,
    canonical_name: str,
    alias_name: str = "",
):
    try:
        _ensure_person_db_extra_columns(notion)
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)

        def rt(text: str) -> list:
            return [{"text": {"content": str(text)[:2000]}}]

        def ms(_: str) -> list:
            return []

        updates: dict = {}
        name_prop = fields.get("name")
        if name_prop and canonical_name:
            ptype = schema.get(name_prop, "title")
            updates[name_prop] = _build_person_prop_value(ptype, canonical_name, rt, ms)

        nickname_prop = fields.get("nickname")
        if nickname_prop:
            ptype = schema.get(nickname_prop, "rich_text")
            updates[nickname_prop] = _build_person_prop_value(
                ptype,
                _nickname_value(canonical_name, alias_name),
                rt,
                ms,
            )
        if updates:
            notion.pages.update(page_id=page_id, properties=updates)
    except Exception as e:
        print(f"  ⚠️ 인물 canonical/alias 속성 동기화 실패: {e}")


_NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

_PERSON_SCHEMA_ENSURED = False


def _ensure_person_db_extra_columns(notion: NotionClient):
    """인물 DB에 시트 대응 파생 컬럼이 없으면 자동 생성."""
    global _PERSON_SCHEMA_ENSURED
    if _PERSON_SCHEMA_ENSURED:
        return
    schema = _get_person_db_schema(notion)
    desired = {
        "근거 링크": "url",
        "최근 발언일": "rich_text",
        "최근 채널": "rich_text",
        "최근 발언": "rich_text",
        "대표 채널": "rich_text",
        "채널 TOP3": "rich_text",
        "일관성 요약": "rich_text",
        "신뢰도 점수": "rich_text",
        "신뢰도 상태": "rich_text",
        "닉네임": "rich_text",
    }
    to_add: dict = {}
    for col, typ in desired.items():
        if col in schema:
            continue
        if typ == "url":
            to_add[col] = {"url": {}}
        else:
            to_add[col] = {"rich_text": {}}

    if to_add:
        try:
            r = httpx.patch(
                f"https://api.notion.com/v1/databases/{PERSON_DB_ID}",
                headers=_NOTION_HEADERS,
                json={"properties": to_add},
                timeout=30,
            )
            r.raise_for_status()
            print(f"  [SCHEMA] 인물 DB 파생 컬럼 생성: {list(to_add.keys())}")
        except Exception as e:
            print(f"  [SCHEMA] ⚠️ 인물 DB 파생 컬럼 생성 실패: {e}")
    _PERSON_SCHEMA_ENSURED = True


def _person_db_query(body: dict) -> dict:
    """인물 DB 페이지 목록 조회 (raw httpx — notion-client 3.x는 databases.query 미지원)"""
    resp = httpx.post(
        f"https://api.notion.com/v1/databases/{PERSON_DB_ID}/query",
        headers=_NOTION_HEADERS,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_person_db_schema(notion: NotionClient) -> dict[str, str]:
    """인물 DB의 실제 속성명→타입 맵 반환 (디버그용).
    databases.retrieve 응답에 properties가 없으면 페이지 조회로 대체."""
    now = time.time()
    cached = _PERSON_DB_SCHEMA_CACHE.get("schema")
    cts = _PERSON_DB_SCHEMA_CACHE.get("ts", 0.0)
    if cached is not None and (now - cts) <= PERSON_DB_SCHEMA_CACHE_SEC:
        return dict(cached)

    try:
        db = notion.databases.retrieve(database_id=PERSON_DB_ID)
        props = db.get("properties", {})
        if props:
            schema = {k: v.get("type", "?") for k, v in props.items()}
            _PERSON_DB_SCHEMA_CACHE["ts"] = time.time()
            _PERSON_DB_SCHEMA_CACHE["schema"] = dict(schema)
            return schema
        # notion-client 3.x — databases.retrieve가 properties 미포함 → 페이지 조회로 대체
        print("  [SCHEMA] databases.retrieve에 properties 없음 → 페이지 조회로 속성명 확인")
        data = _person_db_query({"page_size": 1})
        results = data.get("results", [])
        if results:
            page_props = results[0].get("properties", {})
            schema = {k: v.get("type", "?") for k, v in page_props.items()}
            print(f"  [SCHEMA] 페이지 조회로 속성명 확인: {schema}")
            _PERSON_DB_SCHEMA_CACHE["ts"] = time.time()
            _PERSON_DB_SCHEMA_CACHE["schema"] = dict(schema)
            return schema
        print("  [SCHEMA] DB에 기존 페이지 없음 — 속성명 확인 불가 (빈 DB)")
        return {}
    except Exception as e:
        logger.error(f"[PERSON_DB] DB 스키마 조회 실패: {e}", exc_info=True)
        return {}


def _resolve_person_fields(schema: dict[str, str]) -> dict[str, Optional[str]]:
    """인물 DB의 실제 속성명에서 논리 필드명(name/count 등)을 유연하게 매핑."""
    if not schema:
        # 스키마 조회 실패 시 기본 속성명으로라도 생성 시도
        return {
            "name": "이름",
            "affiliation": "소속",
            "role": "직책",
            "career": "주요 경력",
            "expertise": "전문 분야",
            "count": "등장 횟수",
            "first_seen": "첫 등장일",
            "source_url": "근거 링크",
            "latest_date": "최근 발언일",
            "latest_channel": "최근 채널",
            "latest_opinion": "최근 발언",
            "dominant_channel": "대표 채널",
            "top_channels": "채널 TOP3",
            "consistency_summary": "일관성 요약",
            "confidence_score": "신뢰도 점수",
            "confidence_status": "신뢰도 상태",
            "nickname": "닉네임",
        }

    aliases = {
        "name": ["이름", "Name", "성명"],
        "affiliation": ["소속", "Affiliation", "회사", "기관"],
        "role": ["직책", "Role", "직위", "직무"],
        "career": ["주요 경력", "경력", "Career"],
        "expertise": ["전문 분야", "전문분야", "Expertise", "분야"],
        "count": ["등장 횟수", "등장횟수", "Appearance Count", "Count", "횟수"],
        "first_seen": ["첫 등장일", "First Seen", "등록일", "Date"],
        "source_url": ["근거 링크", "출처 링크", "Reference URL", "URL", "링크"],
        "latest_date": ["최근 발언일", "Latest Date"],
        "latest_channel": ["최근 채널", "Latest Channel"],
        "latest_opinion": ["최근 발언", "Latest Opinion"],
        "dominant_channel": ["대표 채널", "Dominant Channel"],
        "top_channels": ["채널 TOP3", "Top Channels"],
        "consistency_summary": ["일관성 요약", "Consistency Summary"],
        "confidence_score": ["신뢰도 점수", "Confidence Score"],
        "confidence_status": ["신뢰도 상태", "Confidence Status"],
        "nickname": ["닉네임", "Nickname"],
    }

    def _pick(logical: str, ptype: Optional[str] = None) -> Optional[str]:
        for cand in aliases[logical]:
            if cand in schema and (ptype is None or schema.get(cand) == ptype):
                return cand
        if ptype:
            for prop_name, typ in schema.items():
                if typ == ptype:
                    return prop_name
        return None

    return {
        "name": _pick("name", "title") or _pick("name"),
        "affiliation": _pick("affiliation"),
        "role": _pick("role"),
        "career": _pick("career"),
        "expertise": _pick("expertise"),
        "count": _pick("count", "number") or _pick("count"),
        "first_seen": _pick("first_seen", "date") or _pick("first_seen"),
        "source_url": _pick("source_url", "url") or _pick("source_url"),
        "latest_date": _pick("latest_date"),
        "latest_channel": _pick("latest_channel"),
        "latest_opinion": _pick("latest_opinion"),
        "dominant_channel": _pick("dominant_channel"),
        "top_channels": _pick("top_channels"),
        "consistency_summary": _pick("consistency_summary"),
        "confidence_score": _pick("confidence_score"),
        "confidence_status": _pick("confidence_status"),
        "nickname": _pick("nickname"),
    }


def _extract_prop_text(prop: dict) -> str:
    ptype = prop.get("type", "")
    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    if ptype == "multi_select":
        return ", ".join(opt.get("name", "") for opt in prop.get("multi_select", []))
    if ptype == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if ptype == "number":
        v = prop.get("number")
        return str(v) if v is not None else "0"
    if ptype == "url":
        return prop.get("url") or ""
    return ""


def _build_person_prop_value(ptype: str, value: str, rt_builder, ms_builder):
    """속성 타입에 맞는 Notion property payload 생성."""
    if ptype == "title":
        return {"title": rt_builder(value)}
    if ptype == "rich_text":
        return {"rich_text": rt_builder(value)}
    if ptype == "multi_select":
        return {"multi_select": ms_builder(value)}
    if ptype == "select":
        text = str(value).strip()
        return {"select": {"name": text[:100]}} if text else {"select": None}
    if ptype == "url":
        return {"url": (str(value).strip() or None)}
    return {"rich_text": rt_builder(value)}


def create_person_in_notion_db(
    notion: NotionClient,
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str = "",
    alias_name: str = "",
) -> Optional[str]:
    """노션 인물 DB에 새 인물 페이지 생성 후 page_id 반환"""
    def rt(text: str) -> list:
        return [{"text": {"content": str(text)[:2000]}}]

    _ensure_person_db_extra_columns(notion)

    # ── [DEBUG] 실제 DB 속성명 확인 ──────────────────────────
    schema = _get_person_db_schema(notion)
    print(f"  [NOTION] 인물 DB 실제 속성 목록: {schema}")

    def ms(text: str) -> list:
        """문자열을 multi_select 옵션 리스트로 변환 (쉼표 구분)"""
        return [{"name": t.strip()[:100]} for t in str(text).split(",") if t.strip()]

    today = datetime.now(KST).strftime("%Y-%m-%d")
    fields = _resolve_person_fields(schema)
    all_props = schema if schema else {
        "이름": "title",
        "소속": "rich_text",
        "직책": "rich_text",
        "주요 경력": "rich_text",
        "전문 분야": "rich_text",
        "등장 횟수": "number",
        "첫 등장일": "date",
        "근거 링크": "url",
    }

    properties: dict = {}
    if fields["name"]:
        prop_name = fields["name"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "title"), name, rt, ms
        )
    if fields["affiliation"]:
        prop_name = fields["affiliation"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "rich_text"), affiliation, rt, ms
        )
    if fields["role"]:
        prop_name = fields["role"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "rich_text"), role, rt, ms
        )
    if fields["career"]:
        prop_name = fields["career"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "rich_text"), career, rt, ms
        )
    if fields["expertise"]:
        prop_name = fields["expertise"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "rich_text"), expertise, rt, ms
        )
    if fields["count"]:
        prop_name = fields["count"]
        if all_props.get(prop_name) == "number":
            properties[prop_name] = {"number": 1}
        else:
            properties[prop_name] = _build_person_prop_value(
                all_props.get(prop_name, "rich_text"), "1", rt, ms
            )
    if fields["first_seen"]:
        prop_name = fields["first_seen"]
        if all_props.get(prop_name) == "date":
            properties[prop_name] = {"date": {"start": today}}
    if fields.get("source_url") and source_url:
        prop_name = fields["source_url"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "url"), source_url, rt, ms
        )
    if fields.get("nickname"):
        prop_name = fields["nickname"]
        properties[prop_name] = _build_person_prop_value(
            all_props.get(prop_name, "rich_text"),
            _nickname_value(name, alias_name),
            rt,
            ms,
        )

    # 코드 속성명이 실제 DB에 없으면 경고
    for prop_name in properties:
        if schema and prop_name not in schema:
            print(f"  [NOTION] ⚠️ 속성명 불일치: '{prop_name}' 가 DB에 없음 → 실제 속성: {list(schema.keys())}")
    if not properties:
        print("  [NOTION] ❌ 저장 가능한 속성을 찾지 못했습니다. 인물 DB 스키마를 확인하세요.")
        return None

    print(f"  [NOTION] pages.create 요청: parent_db={PERSON_DB_ID[:8]}… name={name!r}")

    try:
        resp = notion.pages.create(
            parent={"database_id": PERSON_DB_ID},
            properties=properties,
        )
        page_id = resp.get("id")
        print(f"  [NOTION] pages.create 응답: object={resp.get('object')}, id={page_id}")
        print(f"  [NOTION] 응답 properties keys: {list(resp.get('properties', {}).keys())}")

        if not page_id:
            print(f"  [NOTION] ❌ 응답에 id 없음. 전체 응답: {resp}")
            return None
        _invalidate_person_db_cache()

        # ── [DEBUG] 저장 직후 재조회로 실제 기록 값 확인 ────
        try:
            verify = notion.pages.retrieve(page_id=page_id)
            saved_name = _get_person_prop(verify, "이름")
            saved_aff  = _get_person_prop(verify, "소속")
            saved_role = _get_person_prop(verify, "직책")
            saved_cnt  = _get_person_count(verify)
            print(f"  [NOTION] 재조회 확인 → 이름={saved_name!r}, 소속={saved_aff!r}, 직책={saved_role!r}, 등장횟수={saved_cnt}")
            if not saved_name:
                print(f"  [NOTION] ⚠️ 이름이 비어 있음 — 속성명 '이름' 이 DB와 일치하는지 확인 필요")
                print(f"           전체 properties: { {k: v.get('type') for k, v in verify.get('properties', {}).items()} }")
        except Exception as ve:
            print(f"  [NOTION] 재조회 실패: {ve}")

        return page_id
    except Exception as e:
        logger.error(f"  [NOTION] pages.create 예외: {e}", exc_info=True)
        return None


def _ensure_sheet_tab(tab: str):
    """구글 시트에 탭이 없으면 생성"""
    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=EXPERT_SHEET_ID).execute()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    print(f"  [SHEETS] 현재 탭 목록: {sorted(existing)}")
    if tab not in existing:
        r = service.spreadsheets().batchUpdate(
            spreadsheetId=EXPERT_SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
        new_id = r.get("replies", [{}])[0].get("addSheet", {}).get("properties", {}).get("sheetId")
        print(f"  [SHEETS] '{tab}' 탭 생성 완료 (sheetId={new_id})")


def _get_sheet_tab_id(spreadsheet_id: str, tab: str) -> Optional[int]:
    try:
        service = _get_sheets_service()
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for s in meta.get("sheets", []):
            p = s.get("properties", {})
            if p.get("title") == tab:
                return p.get("sheetId")
    except Exception:
        return None
    return None


def _delete_sheet_rows(spreadsheet_id: str, tab: str, row_numbers: list[int]):
    """시트에서 행 번호(1-based) 기준으로 행 자체를 삭제. 헤더(1행)는 보호."""
    if not row_numbers:
        return
    sheet_id = _get_sheet_tab_id(spreadsheet_id, tab)
    if sheet_id is None:
        return
    service = _get_sheets_service()
    for row_num in sorted(set(row_numbers), reverse=True):
        if row_num <= 1:
            continue
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "ROWS",
                                    "startIndex": row_num - 1,
                                    "endIndex": row_num,
                                }
                            }
                        }
                    ]
                },
            ).execute()
        except Exception:
            pass


def _normalize_person_text(text: str) -> str:
    """중복 판별용 텍스트 정규화 (공백 축약 + 소문자)."""
    return _normalize_identity_text(text)


def _person_sheet_key(name: str, affiliation: str) -> tuple[str, str]:
    """Economic_Expert 중복 판별 키: (이름, 소속)."""
    return (_normalize_person_text(name), _normalize_person_text(affiliation))


def _resolve_content_person_relation_prop(page_props: dict) -> Optional[str]:
    """콘텐츠 DB에서 인물 relation 속성명을 탐지."""
    if CONTENT_PERSON_RELATION_PROP in page_props:
        if page_props[CONTENT_PERSON_RELATION_PROP].get("type") == "relation":
            return CONTENT_PERSON_RELATION_PROP

    relation_props = [k for k, v in page_props.items() if v.get("type") == "relation"]
    if not relation_props:
        return None

    for key in relation_props:
        lowered = key.lower()
        if ("인물" in key) or ("전문가" in key) or ("person" in lowered) or ("expert" in lowered):
            return key

    return relation_props[0]


def _normalize_person_name_column(notion: NotionClient):
    """인물 DB 이름 컬럼을 '이름만' 남기도록 정규화."""
    try:
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        name_prop = fields.get("name") or "이름"
        if not name_prop:
            return
        pages = _get_all_persons_from_db(notion)
        changed = 0
        for p in pages:
            raw_name = _get_person_prop(p, "이름")
            aff = _get_person_prop(p, "소속")
            role = _get_person_prop(p, "직책")
            clean_name, _, _ = _sanitize_person_fields(raw_name, aff, role)
            if not clean_name or clean_name == raw_name:
                continue
            try:
                ptype = schema.get(name_prop, "title")
                payload = _build_person_prop_value(
                    ptype,
                    clean_name,
                    lambda t: [{"text": {"content": str(t)[:2000]}}],
                    lambda t: [],
                )
                notion.pages.update(page_id=p["id"], properties={name_prop: payload})
                changed += 1
            except Exception:
                continue
        if changed:
            print(f"👤 이름 컬럼 정규화 완료: {changed}건")
    except Exception as e:
        logger.error(f"이름 컬럼 정규화 실패: {e}", exc_info=True)


def _enrich_missing_person_profiles(notion: NotionClient):
    """기존 인물 중 누락 필드(소속/직책/경력/링크) 보강."""
    pages = _get_all_persons_from_db(notion)
    patched = 0
    for p in pages:
        page_id = p["id"]
        name = _get_person_prop(p, "이름")
        affiliation = _get_person_prop(p, "소속")
        role = _get_person_prop(p, "직책")
        career = _get_person_prop(p, "주요 경력")
        expertise = _get_person_prop(p, "전문 분야")
        source_url = _get_person_prop(p, "근거 링크")
        if not name:
            continue

        need = (
            _is_missing_person_value(affiliation)
            or _is_missing_person_value(role)
            or _is_missing_person_value(career)
            or not source_url
        )
        if not need:
            continue

        info = collect_person_info_from_search(name, affiliation, role)
        new_aff = info.get("affiliation", affiliation)
        new_role = info.get("role", role)
        new_career = info.get("career", career)
        new_exp = info.get("expertise", expertise)
        new_url = info.get("source_url", source_url)

        try:
            schema = _get_person_db_schema(notion)
            fields = _resolve_person_fields(schema)
            payload = {}
            for logical, val in (
                ("affiliation", new_aff),
                ("role", new_role),
                ("career", new_career),
                ("expertise", new_exp),
                ("source_url", new_url),
            ):
                prop_name = fields.get(logical)
                if not prop_name:
                    continue
                ptype = schema.get(prop_name, "rich_text")
                payload[prop_name] = _build_person_prop_value(
                    ptype, val,
                    lambda t: [{"text": {"content": str(t)[:2000]}}],
                    lambda t: [{"name": s.strip()[:100]} for s in str(t).split(",") if s.strip()],
                )
            if payload:
                notion.pages.update(page_id=page_id, properties=payload)
                patched += 1
                time.sleep(PERSON_SYNC_SLEEP_SEC)
        except Exception:
            continue
    if patched:
        print(f"👤 누락 프로필 보강 완료: {patched}건")


def _run_people_maintenance_light():
    """저비용 주기 관리: 적립형 동기화 중심(기본), 필요 시 purge/rebuild 옵션 수행."""
    service_run_people_maintenance_light(
        PeopleMaintenanceDeps(
            logger=logger,
            print_fn=print,
            sync_people_from_notion=sync_people_from_notion,
            get_notion_client=get_notion_client,
            people_purge_on_maintenance=PEOPLE_PURGE_ON_MAINTENANCE,
            people_accumulate_mode=PEOPLE_ACCUMULATE_MODE,
            purge_people_without_youtube_source=_purge_people_without_youtube_source,
            normalize_person_name_column=_normalize_person_name_column,
            people_rebuild_on_maintenance=PEOPLE_REBUILD_ON_MAINTENANCE,
            rebuild_expert_sheet=_rebuild_expert_sheet,
            check_people_sync_status=check_people_sync_status,
            queue_non_economic_people_review=queue_non_economic_people_review,
            enrich_missing_person_profiles=_enrich_missing_person_profiles,
            auto_dedup_people_db=_auto_dedup_people_db,
            dedup_person_page_opinions=_dedup_person_page_opinions,
            weekly_identity_audit=_weekly_person_identity_audit,
        )
    )


def _run_people_maintenance_once():
    """정밀 배치(새벽): 경량 관리 + 프로필 보강 + 중복 정리(적립형 유지)."""
    service_run_people_maintenance_once(
        PeopleMaintenanceDeps(
            logger=logger,
            print_fn=print,
            sync_people_from_notion=sync_people_from_notion,
            get_notion_client=get_notion_client,
            people_purge_on_maintenance=PEOPLE_PURGE_ON_MAINTENANCE,
            people_accumulate_mode=PEOPLE_ACCUMULATE_MODE,
            purge_people_without_youtube_source=_purge_people_without_youtube_source,
            normalize_person_name_column=_normalize_person_name_column,
            people_rebuild_on_maintenance=PEOPLE_REBUILD_ON_MAINTENANCE,
            rebuild_expert_sheet=_rebuild_expert_sheet,
            check_people_sync_status=check_people_sync_status,
            queue_non_economic_people_review=queue_non_economic_people_review,
            enrich_missing_person_profiles=_enrich_missing_person_profiles,
            auto_dedup_people_db=_auto_dedup_people_db,
            dedup_person_page_opinions=_dedup_person_page_opinions,
            weekly_identity_audit=_weekly_person_identity_audit,
        )
    )


def save_person_to_expert_sheet(
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    appearance_count: int,
    notion: Optional[NotionClient] = None,
    person_page_id: Optional[str] = None,
    source_url: str = "",
    body_fields_override: Optional[dict] = None,
    confidence_score: str = "",
    confidence_status: str = "",
    alias_name: str = "",
):
    """구글 시트 Economic_Expert 탭에 인물 정보 저장/업데이트"""
    service_save_person_to_expert_sheet(
        name,
        affiliation,
        role,
        career,
        expertise,
        appearance_count,
        ExpertSheetSaveDeps(
            expert_sheet_tab=EXPERT_SHEET_TAB,
            expert_sheet_id=EXPERT_SHEET_ID,
            expert_sheet_headers=_EXPERT_SHEET_HEADERS,
            ensure_sheet_tab=_ensure_sheet_tab,
            get_sheets_service=_get_sheets_service,
            person_sheet_key=_person_sheet_key,
            extract_person_body_sheet_fields=_extract_person_body_sheet_fields,
            sync_person_summary_props=_sync_person_summary_props,
            get_person_prop=_get_person_prop,
            nickname_value=_nickname_value,
            delete_sheet_rows=_delete_sheet_rows,
            verify_write=EXPERT_SHEET_VERIFY_WRITE,
            logger=logger,
            print_fn=print,
        ),
        notion=notion,
        person_page_id=person_page_id,
        source_url=source_url,
        body_fields_override=body_fields_override,
        confidence_score=confidence_score,
        confidence_status=confidence_status,
        alias_name=alias_name,
    )


def _remove_person_from_expert_sheet(name: str, affiliation: str):
    tab = EXPERT_SHEET_TAB
    try:
        service = _get_sheets_service()
        sheet = service.spreadsheets()
        values = sheet.values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=EXPERT_SHEET_FULL_RANGE,
        ).execute().get("values", [])
        target = _person_sheet_key(name, affiliation)
        to_delete: list[int] = []
        for i, row in enumerate(values[1:], start=2):
            if not row:
                continue
            r_name = row[0] if len(row) > 0 else ""
            r_aff = row[1] if len(row) > 1 else ""
            if _person_sheet_key(r_name, r_aff) == target:
                to_delete.append(i)
        _delete_sheet_rows(EXPERT_SHEET_ID, tab, to_delete)
    except Exception:
        pass


def _is_youtube_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return ("youtube.com/" in u) or ("youtu.be/" in u)


def _purge_people_without_youtube_source(notion: NotionClient) -> dict:
    """근거 링크가 없거나 유튜브 링크가 아닌 인물 페이지를 삭제(archive)하고 시트에서도 제거."""
    pages = _get_all_persons_from_db(notion)
    purged = 0
    kept = 0
    samples: list[str] = []

    for p in pages:
        page_id = p.get("id", "")
        name = _get_person_prop(p, "이름")
        affiliation = _get_person_prop(p, "소속")
        source_url = _get_person_prop(p, "근거 링크")
        if source_url and _is_youtube_url(source_url):
            kept += 1
            continue

        try:
            notion.pages.update(page_id=page_id, archived=True)
            _remove_person_from_expert_sheet(name, affiliation)
            purged += 1
            if len(samples) < 10:
                samples.append(name or page_id[:8])
        except Exception as e:
            logger.error(f"근거 유튜브 링크 없는 인물 삭제 실패: name={name!r}, err={e}", exc_info=True)

    return {"total": len(pages), "kept": kept, "purged": purged, "samples": samples}


def _parse_opinions_from_person_blocks(blocks: list) -> list[dict]:
    """인물 페이지 블록에서 발언 목록 파싱. [{date, channel, text}] 반환"""
    opinions: list[dict] = []
    in_opinions = False
    pending: Optional[dict] = None

    for block in blocks:
        btype = block.get("type", "")

        if btype == "heading_2":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("heading_2", {}).get("rich_text", [])
            )
            in_opinions = "주요 발언 요약" in text
            pending = None
            continue

        if not in_opinions:
            continue

        if btype == "heading_3":
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("heading_3", {}).get("rich_text", [])
            )
            # 형식: "📺 채널명 | YYYY.MM.DD"
            text = text.replace("📺", "").strip()
            if "|" in text:
                parts = text.split("|", 1)
                channel = parts[0].strip()
                date = parts[1].strip()
            else:
                channel = text
                date = ""
            pending = {"date": date, "channel": channel, "text": ""}
        elif btype == "paragraph" and pending is not None:
            text = "".join(
                t.get("plain_text", "")
                for t in block.get("paragraph", {}).get("rich_text", [])
            )
            if text.strip():
                pending["text"] = text
                opinions.append(pending)
                pending = None

    return opinions


def _normalize_opinion_text(text: str) -> str:
    s = _clean(text or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^\w가-힣 ]", "", s)
    return s


def _normalize_opinion_date(date_text: str) -> str:
    return re.sub(r"[^0-9]", "", date_text or "")


def _is_near_duplicate_opinion(a: dict, b: dict) -> bool:
    """같은 발언의 표기 변형(띄어쓰기/문장부호/경미한 문구 차이)을 중복으로 판정."""
    a_text = _normalize_opinion_text(a.get("text", ""))
    b_text = _normalize_opinion_text(b.get("text", ""))
    if not a_text or not b_text:
        return False

    a_ch = _normalize_identity_text(a.get("channel", ""))
    b_ch = _normalize_identity_text(b.get("channel", ""))
    a_dt = _normalize_opinion_date(a.get("date", ""))
    b_dt = _normalize_opinion_date(b.get("date", ""))

    # 완전 동일 텍스트는 채널/날짜가 같으면 중복
    if a_text == b_text and (a_ch == b_ch or (a_dt and a_dt == b_dt)):
        return True

    # 같은 채널이거나 같은 날짜일 때는 유사 문장도 중복으로 간주
    if (a_ch and a_ch == b_ch) or (a_dt and a_dt == b_dt):
        sim = SequenceMatcher(None, a_text, b_text).ratio()
        if sim >= 0.93:
            return True
        # 부분 포함(요약 길이 차이) 처리
        short, long_ = (a_text, b_text) if len(a_text) <= len(b_text) else (b_text, a_text)
        if len(short) >= 40 and short in long_:
            return True
    return False


def _dedup_opinions(opinions: list[dict]) -> list[dict]:
    """같은 발언 중복 제거 (순서 유지). 완전일치+유사문장 중복 모두 제거."""
    seen_exact: set[tuple[str, str, str]] = set()
    deduped: list[dict] = []
    for op in opinions:
        text = (op.get("text") or "").strip()
        if not text:
            continue

        norm_channel = _normalize_identity_text(op.get("channel", ""))
        norm_date = _normalize_opinion_date(op.get("date", ""))
        norm_text = _normalize_opinion_text(text)
        key = (norm_channel, norm_date, norm_text)

        if key in seen_exact:
            continue

        # 경미한 표기 차이까지 제거
        dup_like = False
        for kept in deduped:
            if _is_near_duplicate_opinion(kept, op):
                dup_like = True
                break
        if dup_like:
            continue

        seen_exact.add(key)
        deduped.append(op)
    return deduped


def _extract_person_body_sheet_fields(notion: NotionClient, person_page_id: str) -> dict[str, str]:
    """인물 페이지 본문에서 시트용 요약 필드 추출."""
    try:
        blocks = notion.blocks.children.list(block_id=person_page_id).get("results", [])
    except Exception:
        return {
            "latest_date": "",
            "latest_channel": "",
            "latest_opinion": "",
            "dominant_channel": "",
            "top_channels": "",
            "consistency_summary": "",
        }

    opinions = _dedup_opinions(_parse_opinions_from_person_blocks(blocks))
    latest = opinions[-1] if opinions else {}

    channel_counts: dict[str, int] = {}
    for op in opinions:
        ch = op.get("channel", "")
        if ch:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1
    top3 = sorted(channel_counts.items(), key=lambda x: -x[1])[:3]
    top3_text = ", ".join(f"{ch}({cnt})" for ch, cnt in top3)
    dominant_channel = top3[0][0] if top3 else ""

    in_consistency = False
    summary = ""
    for block in blocks:
        btype = block.get("type", "")
        if btype == "heading_2":
            h = "".join(t.get("plain_text", "") for t in block.get("heading_2", {}).get("rich_text", []))
            in_consistency = "발언 일관성 검증" in h
            continue
        if in_consistency and btype == "paragraph":
            p = "".join(t.get("plain_text", "") for t in block.get("paragraph", {}).get("rich_text", []))
            if p.strip():
                summary = p.replace("요약:", "", 1).strip() if p.strip().startswith("요약:") else p.strip()
                break

    return {
        "latest_date": latest.get("date", "") if latest else "",
        "latest_channel": latest.get("channel", "") if latest else "",
        "latest_opinion": latest.get("text", "") if latest else "",
        "dominant_channel": dominant_channel,
        "top_channels": top3_text,
        "consistency_summary": summary,
    }


def _body_fields_from_opinions(opinions: list[dict], consistency: dict) -> dict[str, str]:
    latest = opinions[-1] if opinions else {}
    channel_counts: dict[str, int] = {}
    for op in opinions:
        ch = op.get("channel", "")
        if ch:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1
    top3 = sorted(channel_counts.items(), key=lambda x: -x[1])[:3]
    return {
        "latest_date": latest.get("date", "") if latest else "",
        "latest_channel": latest.get("channel", "") if latest else "",
        "latest_opinion": latest.get("text", "") if latest else "",
        "dominant_channel": top3[0][0] if top3 else "",
        "top_channels": ", ".join(f"{ch}({cnt})" for ch, cnt in top3),
        "consistency_summary": consistency.get("summary", "일관된 발언 유지 중"),
    }


def _analyze_opinion_consistency(name: str, opinions: list[dict]) -> dict:
    """Gemini로 발언 일관성 분석. opinions는 시간순 [{date, channel, text}]"""
    if len(opinions) < 2:
        return {"changes": [], "consistent": [], "summary": "발언 기록이 2개 미만으로 비교 불가"}

    opinions_text = "\n".join(
        f"[{o['date']} / {o['channel']}] {o['text']}"
        for o in opinions
        if o.get("text")
    )

    result = _gemini_json(f"""다음은 {name}의 과거 발언 목록입니다 (시간순):

{opinions_text}

이전 발언들과 가장 최근 발언을 비교하여 일관성을 분석하세요.

JSON으로만 응답:
{{
  "changes": [
    "예시: 2024.01 '금리 인하 시기상조' → 2024.06 '금리 인하 필요' (입장 변경)"
  ],
  "consistent": [
    "예시: 반도체 섹터 긍정 의견 일관되게 유지"
  ],
  "summary": "일관된 발언 유지 중"
}}

규칙:
- changes: 말이 실질적으로 바뀐 부분만 포함 (없으면 빈 배열)
- consistent: 일관성 있는 의견 (없으면 빈 배열)
- summary: 변경이 있으면 "입장 변경 발견", 없으면 "일관된 발언 유지 중"
""")

    if result and isinstance(result, dict):
        return {
            "changes": result.get("changes", []),
            "consistent": result.get("consistent", []),
            "summary": result.get("summary", "일관된 발언 유지 중"),
        }
    return {"changes": [], "consistent": [], "summary": "일관된 발언 유지 중"}


def _build_person_body_blocks(
    name: str,
    all_opinions: list[dict],
    channel_counts: dict[str, int],
    consistency: dict,
) -> list[dict]:
    """인물 페이지 본문 블록 리스트 생성 (발언 요약 + 채널 TOP3 + 일관성 검증)"""
    def rt(content: str) -> list:
        return [{"type": "text", "text": {"content": str(content)[:2000]}}]

    def h2(title: str) -> dict:
        return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": rt(title)}}

    def h3(title: str) -> dict:
        return {"object": "block", "type": "heading_3", "heading_3": {"rich_text": rt(title)}}

    def para(content: str) -> dict:
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": rt(content or "정보 없음")}}

    def bullet(content: str) -> dict:
        return {"object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": rt(content)}}

    blocks: list[dict] = []
    top3 = sorted(channel_counts.items(), key=lambda x: -x[1])[:3]

    # 📝 주요 발언 요약
    blocks.append(h2("📝 주요 발언 요약"))
    for op in all_opinions:
        op_channel = op.get("channel", "")
        op_date = op.get("date", "")
        op_text = op.get("text", "")
        if op_text:
            header = f"📺 {op_channel} | {op_date}" if op_date else f"📺 {op_channel}"
            blocks.append(h3(header))
            blocks.append(para(op_text))

    # 📺 자주 출연한 채널 TOP3
    blocks.append(h2("📺 자주 출연한 채널 TOP3"))
    if top3:
        for rank, (ch, cnt) in enumerate(top3, 1):
            blocks.append(bullet(f"{rank}위. {ch} ({cnt}회)"))
    else:
        blocks.append(para("집계 데이터 없음"))

    # 📊 발언 일관성 검증
    blocks.append(h2("📊 발언 일관성 검증"))
    changes = consistency.get("changes", [])
    consistent_list = consistency.get("consistent", [])
    summary = consistency.get("summary", "일관된 발언 유지 중")

    if changes:
        blocks.append(h3("🔄 입장 변경"))
        for change in changes:
            blocks.append(bullet(change))
    if consistent_list:
        blocks.append(h3("✅ 일관된 의견"))
        for item in consistent_list:
            blocks.append(bullet(item))
    blocks.append(para(f"종합: {summary}"))

    return blocks


def _clear_and_write_person_blocks(notion: NotionClient, page_id: str, name: str, blocks: list[dict]):
    """인물 페이지 기존 블록 전체 삭제 후 새 블록 추가"""
    try:
        existing = notion.blocks.children.list(block_id=page_id).get("results", [])
        for block in existing:
            try:
                notion.blocks.delete(block_id=block["id"])
            except Exception:
                pass
    except Exception as e:
        print(f"  ⚠️ 기존 블록 삭제 실패: {e}")

    try:
        for i in range(0, len(blocks), 100):
            notion.blocks.children.append(block_id=page_id, children=blocks[i:i + 100])
        print(f"  ✓ 인물 페이지 본문 업데이트 완료 ({name})")
    except Exception as e:
        print(f"  ⚠️ 인물 페이지 본문 업데이트 실패: {e}")


def update_person_page_body(
    notion: NotionClient,
    page_id: str,
    name: str,
    channel: str,
    opinion: str,
    date_str: str,
)-> dict[str, str]:
    """인물 DB 페이지 본문에 발언 추가, 채널 TOP3 업데이트, 일관성 검증 업데이트"""
    # 기존 블록 읽기 + 발언 파싱
    try:
        existing_blocks = notion.blocks.children.list(block_id=page_id).get("results", [])
    except Exception as e:
        print(f"  ⚠️ 인물 페이지 블록 읽기 실패: {e}")
        existing_blocks = []

    past_opinions = _parse_opinions_from_person_blocks(existing_blocks)
    all_opinions = _dedup_opinions(past_opinions + [{"date": date_str, "channel": channel, "text": opinion}])

    channel_counts: dict[str, int] = {}
    for op in all_opinions:
        ch = op.get("channel", "")
        if ch:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1

    consistency = _analyze_opinion_consistency(name, all_opinions)
    blocks = _build_person_body_blocks(name, all_opinions, channel_counts, consistency)
    _clear_and_write_person_blocks(notion, page_id, name, blocks)
    body_fields = _body_fields_from_opinions(all_opinions, consistency)
    _sync_person_summary_props(notion, page_id, name, body_fields)
    return body_fields


def process_person_db(
    notion: NotionClient,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    channel: str,
) -> Optional[str]:
    """인물 DB 처리 통합 함수: 검색 → 생성/업데이트 → 등장 횟수 → 본문 업데이트 → 시트 저장"""
    canonical_name, alias_name = _resolve_canonical_person_identity(
        name,
        affiliation,
        role,
        channel=channel,
    )
    return service_process_person_db(
        notion,
        canonical_name,
        affiliation,
        role,
        opinion,
        channel,
        alias_name,
        ProcessPersonDeps(
            kst=KST,
            sanitize_person_fields=_sanitize_person_fields,
            person_db_lock=_PERSON_DB_LOCK,
            find_person_in_notion_db=find_person_in_notion_db,
            update_existing_person_record=_update_existing_person_record,
            remember_person_match=_remember_person_match,
            forget_person_match=_forget_person_match,
            is_missing_person_value=_is_missing_person_value,
            collect_person_info_from_search=collect_person_info_from_search,
            google_search_items=_google_search_items,
            pick_best_source_link=_pick_best_source_link,
            notify_uncertain_person_with_ticket=_notify_uncertain_person_with_ticket,
            needs_manual_person_input=_needs_manual_person_input,
            warn_manual_person_input_needed=_warn_manual_person_input_needed,
            create_person_in_notion_db=create_person_in_notion_db,
            is_uncertain_person=_is_uncertain_person,
            is_person_review_approved=_is_person_review_approved,
            person_uncertain_action=PERSON_UNCERTAIN_ACTION,
            remove_person_from_expert_sheet=_remove_person_from_expert_sheet,
            update_person_page_body=update_person_page_body,
            backfill_person_profile=_backfill_person_profile,
            set_person_source_url_if_missing=_set_person_source_url_if_missing,
            save_person_to_expert_sheet=save_person_to_expert_sheet,
            sync_person_identity_props=_sync_person_identity_props,
            person_identity_gate_reasons=_person_identity_gate_reasons,
            logger=logger,
            print_fn=print,
        ),
    )


# ── 인물 DB 진단 (YouTube URL 없이 저장 기능만 단독 검사) ──────────
def check_person_db():
    """Notion 인물 DB 스키마·접근 및 Google Sheets 쓰기를 단독으로 검사"""
    SEP = "=" * 60
    STEP = "─" * 40
    TEST_NAME = "_테스트인물_진단용"

    print(f"\n{SEP}")
    print("🔍 인물 DB 진단 모드")
    print(SEP)

    notion = get_notion_client()

    # ── [1] Notion DB 스키마 조회 ───────────────────────────
    print(f"{STEP}\n[1] Notion 인물 DB 스키마 조회 (DB ID: {PERSON_DB_ID})")
    schema = _get_person_db_schema(notion)
    if not schema:
        print("  ⚠️ 속성 목록 조회 불가 (빈 DB 또는 Integration 권한 제한)")
        print("     → Notion DB → ... → Connections 에서 Integration에 '전체 접근' 권한 확인")
        print("     → 계속해서 페이지 직접 생성 시도...")
    else:
        print(f"  ✓ 속성 목록 ({len(schema)}개):")
        for prop_name, prop_type in schema.items():
            marker = "★" if prop_name in ("이름", "소속", "직책", "주요 경력", "전문 분야", "등장 횟수") else " "
            print(f"    {marker} '{prop_name}' ({prop_type})")

        expected = {"이름", "소속", "직책", "주요 경력", "전문 분야", "등장 횟수"}
        missing = expected - set(schema.keys())
        if missing:
            print(f"  ⚠️ 코드가 기대하는 속성 중 DB에 없는 것: {missing}")
            print(f"     → 위 ★ 표시 속성명과 DB 실제 속성명을 비교해 코드를 수정하세요")
        else:
            print(f"  ✓ 필수 속성 6개 모두 일치")

    # ── [2] Notion 인물 DB에 테스트 페이지 생성 ─────────────
    print(f"{STEP}\n[2] Notion 인물 DB 테스트 페이지 생성")
    test_page_id = create_person_in_notion_db(
        notion, TEST_NAME, "진단용소속", "진단용직책", "진단용경력", "진단용전문분야"
    )
    if test_page_id:
        print(f"  ✓ 테스트 페이지 생성 성공 (id={test_page_id[:8]}…)")
    else:
        print(f"  ❌ 테스트 페이지 생성 실패 — 위 로그에서 원인 확인")

    # ── [3] Google Sheets 테스트 행 저장 ────────────────────
    print(f"{STEP}\n[3] Google Sheets Economic_Expert 탭 쓰기 테스트")
    save_person_to_expert_sheet(
        TEST_NAME, "진단용소속", "진단용직책", "진단용경력", "진단용전문분야", 0,
        notion=notion, person_page_id=test_page_id if test_page_id else None,
    )

    # 저장 확인 후 삭제
    try:
        service = _get_sheets_service()
        tab = EXPERT_SHEET_TAB
        values = service.spreadsheets().values().get(
            spreadsheetId=EXPERT_SHEET_ID, range=f"'{tab}'!A:A"
        ).execute().get("values", [])
        row_idx = next((i + 1 for i, r in enumerate(values) if r and r[0] == TEST_NAME), None)
        if row_idx:
            service.spreadsheets().values().clear(
                spreadsheetId=EXPERT_SHEET_ID,
                range=f"'{tab}'!A{row_idx}:{EXPERT_SHEET_END_COL}{row_idx}",
            ).execute()
            print(f"  ✓ 테스트 행 삭제 완료 (row={row_idx})")
        else:
            print(f"  ⚠️ 테스트 행 삭제 건너뜀 (행 없음)")
    except Exception as e:
        print(f"  ⚠️ 테스트 행 삭제 실패: {e}")

    # ── [4] Notion 테스트 페이지 정리(아카이브) ───────────────
    if test_page_id:
        try:
            notion.pages.update(page_id=test_page_id, archived=True)
            print(f"  ✓ 테스트 페이지 아카이브(삭제) 완료")
        except Exception as e:
            print(f"  ⚠️ 테스트 페이지 삭제 실패 (수동 삭제 필요): {e}")

    print(f"\n{SEP}")
    print("🔍 진단 완료")
    print(SEP)


# ── 인물 DB 플로우 테스트 ────────────────────────────────────
def test_person_flow(url: str):
    """인물 DB 전체 플로우를 단계별로 실행하고 결과를 출력"""
    SEP = "=" * 60
    STEP = "─" * 40

    print(f"\n{SEP}")
    print("🧪 인물 DB 플로우 테스트")
    print(f"URL: {url}")
    print(SEP)

    # STEP 1: video_id
    video_id = extract_video_id(url)
    if not video_id:
        print("❌ [1] 유효하지 않은 유튜브 URL")
        return
    print(f"✓ [1] video_id: {video_id}")

    # STEP 2: YouTube 메타데이터
    print(f"{STEP}\n[2] YouTube 메타데이터 조회 중...")
    try:
        metadata = fetch_youtube_metadata(video_id)
        if not metadata:
            print("❌ [2] 메타데이터 없음")
            return
        print(f"✓ [2] 제목: {metadata.get('title', '')[:60]}")
        print(f"     채널: {metadata.get('channel', '')}")
    except Exception as e:
        print(f"❌ [2] 메타데이터 조회 실패: {e}")
        return

    # STEP 3: 자막
    print(f"{STEP}\n[3] 자막 추출 중...")
    transcript = fetch_transcript(video_id)
    print(f"✓ [3] 자막 {len(transcript)}자 {'(없음, 메타데이터로 분석)' if not transcript else ''}")

    # STEP 4: Gemini 분석
    print(f"{STEP}\n[4] Gemini 분석 중...")
    try:
        analysis = analyze_with_gemini(metadata, transcript)
        person = analysis.get("person", {})
        print(f"✓ [4] 분석 완료")
        print(f"     이름: {person.get('name')}")
        print(f"     소속: {person.get('affiliation')}")
        print(f"     직책: {person.get('role')}")
        print(f"     의견: {analysis.get('opinion', '')[:100]}")
    except Exception as e:
        print(f"❌ [4] Gemini 분석 실패: {e}")
        import traceback; traceback.print_exc()
        return

    # STEP 5: 인물 검증 (Google Search)
    print(f"{STEP}\n[5] 인물 검증 중 (Google Search)...")
    name = person.get("name", "미상")
    role = person.get("role", "미상")
    affiliation = person.get("affiliation", "미상")
    try:
        v_name, v_aff, v_role = verify_person(name, affiliation, role)
        print(f"✓ [5] 검증 결과: {v_name} / {v_aff} / {v_role}")
    except Exception as e:
        print(f"⚠️ [5] 검증 실패 (원본 사용): {e}")
        v_name, v_aff, v_role = name, affiliation, role

    # STEP 6: Notion 인물 DB 접근 확인
    print(f"{STEP}\n[6] Notion 인물 DB 접근 확인 (DB ID: {PERSON_DB_ID})...")
    notion = get_notion_client()
    try:
        resp = notion.databases.retrieve(database_id=PERSON_DB_ID)
        db_title = "".join(
            t.get("plain_text", "") for t in resp.get("title", [])
        )
        print(f"✓ [6] DB 접근 OK: '{db_title}'")
    except Exception as e:
        print(f"❌ [6] Notion 인물 DB 접근 실패: {e}")
        print("     → Notion 통합(Integration)이 해당 DB에 연결되어 있는지 확인하세요")

    # STEP 7: 인물 DB + 시트 저장
    print(f"{STEP}\n[7] 인물 DB 처리 시작: {v_name!r}")
    try:
        person_page_id = process_person_db(
            notion,
            name=v_name,
            affiliation=v_aff,
            role=v_role,
            opinion=analysis.get("opinion", ""),
            channel=metadata.get("channel", ""),
        )
    except Exception as e:
        print(f"❌ [7] process_person_db 예외: {e}")
        import traceback; traceback.print_exc()
        return

    # STEP 8: 결과 검증
    print(f"{STEP}\n[8] 결과 검증...")
    if person_page_id:
        print(f"✓ Notion 인물 DB page_id: {person_page_id}")
        # 저장된 속성 재조회
        try:
            saved = notion.pages.retrieve(page_id=person_page_id)
            print(f"  이름:     {_get_person_prop(saved, '이름')}")
            print(f"  소속:     {_get_person_prop(saved, '소속')}")
            print(f"  직책:     {_get_person_prop(saved, '직책')}")
            print(f"  등장 횟수: {_get_person_count(saved)}")
        except Exception as e:
            print(f"  ⚠️ 속성 재조회 실패: {e}")
    else:
        print("⚠️ Notion 인물 DB 저장 없음 (위 로그 확인)")

    # 시트 확인
    try:
        tab = EXPERT_SHEET_TAB
        service = _get_sheets_service()
        values = service.spreadsheets().values().get(
            spreadsheetId=EXPERT_SHEET_ID, range=EXPERT_SHEET_FULL_RANGE
        ).execute().get("values", [])
        names_in_sheet = [row[0] for row in values[1:] if row]
        if v_name in names_in_sheet:
            print(f"✓ Economic_Expert 시트에 '{v_name}' 기록 확인")
        else:
            print(f"⚠️ Economic_Expert 시트에 '{v_name}' 없음 (현재 {len(names_in_sheet)}명: {names_in_sheet})")
    except Exception as e:
        print(f"⚠️ 시트 확인 실패: {e}")

    print(f"\n{SEP}")
    print("🧪 테스트 완료")
    print(SEP)


# ── 인물 데이터 일괄 보강 ─────────────────────────────────

def _find_content_pages_for_person(notion: NotionClient, name: str) -> list[dict]:
    """콘텐츠 DB에서 특정 인물이 출연한 모든 페이지 조회 (페이지네이션)"""
    all_pages: list[dict] = []
    cursor = None
    while True:
        body: dict = {
            "filter": {"property": "출연자", "rich_text": {"contains": name}},
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor
        try:
            resp = httpx.post(
                f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                headers={
                    "Authorization": f"Bearer {NOTION_API_KEY}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            all_pages.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        except Exception as e:
            print(f"    ⚠️ 콘텐츠 DB 조회 실패 ({name}): {e}")
            break
    return all_pages


def _extract_content_opinions(
    content_pages: list[dict],
    channel_cache: dict,
) -> list[dict]:
    """콘텐츠 DB 페이지에서 발언 정보 추출. [{date, channel, text, title, url}]"""
    opinions: list[dict] = []
    for page in content_pages:
        props = page.get("properties", {})
        opinion_text = _get_rich_text(props.get("인물의견", {}))
        if not opinion_text or opinion_text.strip() in ("정보 없음", "미상", ""):
            continue

        timestamp = _get_rich_text(props.get("처리일시", {}))
        url = props.get("URL", {}).get("url") or ""
        title_raw = (props.get("콘텐츠 제목", {}) or {}).get("title") or []
        title = title_raw[0]["text"]["content"] if title_raw else ""
        date_str = timestamp[:10].replace("-", ".") if timestamp else ""

        # 채널명: 캐시 → YouTube API 순으로 조회
        channel = ""
        video_id = extract_video_id(url)
        if video_id:
            if video_id in channel_cache:
                channel = channel_cache[video_id]
            else:
                try:
                    meta = fetch_youtube_metadata(video_id)
                    channel = meta.get("channel", "")
                    channel_cache[video_id] = channel
                except Exception:
                    channel_cache[video_id] = ""

        opinions.append({
            "date": date_str,
            "channel": channel,
            "text": opinion_text,
            "title": title,
            "url": url,
        })

    opinions.sort(key=lambda o: o.get("date", ""))
    return opinions


def _gemini_enrich_person(
    name: str,
    affiliation: str,
    role: str,
    existing_career: str,
    existing_expertise: str,
    search_snippets: str,
    opinions_summary: str,
) -> dict:
    """Google Search + 발언 맥락으로 인물 프로필 종합 보강. {affiliation, role, career, expertise} 반환"""
    result = _gemini_json(f"""다음 인물의 프로필을 검색 결과와 발언 맥락을 종합해 최대한 정확하게 정리하세요.

인물: {name}
현재 소속: {affiliation}
현재 직책: {role}
현재 주요 경력: {existing_career}
현재 전문 분야: {existing_expertise}

━━ Google 검색 결과 ━━
{search_snippets[:2000]}

━━ 콘텐츠 발언 맥락 ━━
{opinions_summary[:1200]}

JSON으로만 응답:
{{
  "affiliation": "현재 소속 기관명 (공식 명칭, 모르면 기존 값 그대로)",
  "role": "현재 직책 (애널리스트/이코노미스트/교수 등, 모르면 기존 값 그대로)",
  "career": "주요 경력 3~5문장 (연도·기관명 포함, 시간순)",
  "expertise": "전문 분야 키워드 5개 이내 (쉼표 구분)"
}}

규칙:
- 정보가 불확실하면 기존 값 그대로 유지 (절대 임의 추측 금지)
- '정보 없음'·'미상'은 검색 결과로 실제 값 대체
- 경력은 구체적으로 (예: 2010~2015 OO증권 리서치센터 → 2015~ XX자산운용)
""")

    if result and isinstance(result, dict):
        return {
            "affiliation": result.get("affiliation") or affiliation,
            "role":        result.get("role") or role,
            "career":      result.get("career") or existing_career,
            "expertise":   result.get("expertise") or existing_expertise,
        }
    return {
        "affiliation": affiliation,
        "role": role,
        "career": existing_career,
        "expertise": existing_expertise,
    }


def enrich_all_people():
    """인물 DB 전체 프로필 보강: Google Search + 콘텐츠 DB 교차 참조 + Gemini 종합 (--enrich-people)"""
    notion = get_notion_client()

    print("\n📊 인물 DB 전체 조회 중...")
    all_persons = _get_all_persons_from_db(notion)
    total = len(all_persons)
    print(f"  → {total}명 발견\n")

    if total == 0:
        print("인물 DB가 비어있습니다.")
        return

    channel_cache: dict[str, str] = {}  # video_id → channel (YouTube API 중복 호출 방지)
    success = fail = 0

    for i, person in enumerate(all_persons, 1):
        name        = _get_person_prop(person, "이름")
        affiliation = _get_person_prop(person, "소속")
        role        = _get_person_prop(person, "직책")
        career      = _get_person_prop(person, "주요 경력")
        expertise   = _get_person_prop(person, "전문 분야")
        source_url  = _get_person_prop(person, "근거 링크")
        page_id     = person["id"]

        if not name:
            continue

        print(f"\n[{i}/{total}] ── {name} ({affiliation} / {role})")

        try:
            # ── [1] Google Search: 프로필 검증 ──────────────────
            print(f"  [1] Google Search 조회 중...")
            snippets = _google_search(f"{name} {affiliation} {role} 경제 경력 전문분야", num=5)
            if not snippets:
                # 소속·직책 없이 이름만으로 재시도
                snippets = _google_search(f"{name} 경제 애널리스트 전문가", num=5)
            snippet_preview = snippets[:80].replace("\n", " ") if snippets else "(없음)"
            print(f"       검색 결과: {snippet_preview}…")

            # ── [2] 콘텐츠 DB 교차 참조 ─────────────────────────
            print(f"  [2] 콘텐츠 DB 교차 참조 중...")
            content_pages = _find_content_pages_for_person(notion, name)
            print(f"       → {len(content_pages)}개 영상 발견")

            # [2-a] 기존 body block 발언 (채널 정보 보존)
            try:
                existing_blocks = notion.blocks.children.list(block_id=page_id).get("results", [])
                body_opinions = _parse_opinions_from_person_blocks(existing_blocks)
            except Exception:
                body_opinions = []

            # [2-b] 콘텐츠 DB에서 신규 발언 추출 (YouTube API 채널 조회 포함)
            content_opinions = _extract_content_opinions(content_pages, channel_cache)

            # [2-c] 두 소스 병합 (텍스트 앞 60자 기준 중복 제거, 날짜순 정렬)
            seen: set[str] = set()
            all_opinions: list[dict] = []
            for op in body_opinions + content_opinions:
                key = op.get("text", "")[:60].strip()
                if key and key not in seen:
                    seen.add(key)
                    all_opinions.append(op)
            all_opinions.sort(key=lambda o: o.get("date", ""))
            print(f"       → 총 {len(all_opinions)}개 발언 (body:{len(body_opinions)} + content:{len(content_opinions)} → 중복제거)")

            # ── [3] Gemini 종합 보강 ─────────────────────────────
            print(f"  [3] Gemini 프로필 보강 중...")
            opinions_summary = "\n".join(
                f"[{o.get('date','')}] {o.get('text','')[:150]}"
                for o in all_opinions[:10]
            )
            enriched = _gemini_enrich_person(
                name, affiliation, role, career, expertise,
                snippets, opinions_summary,
            )
            # 변경된 필드만 출력
            changes = []
            if enriched["affiliation"] != affiliation:
                changes.append(f"소속: {affiliation!r}→{enriched['affiliation']!r}")
            if enriched["role"] != role:
                changes.append(f"직책: {role!r}→{enriched['role']!r}")
            if enriched["career"] != career:
                changes.append("주요 경력: 업데이트됨")
            if enriched["expertise"] != expertise:
                changes.append(f"전문분야: {enriched['expertise']!r}")
            print(f"       변경: {', '.join(changes) if changes else '(변경 없음)'}")

            # ── [4] Notion 속성 업데이트 ─────────────────────────
            def rt(text: str) -> list:
                return [{"text": {"content": str(text)[:2000]}}]

            def ms_enrich(text: str) -> list:
                return [{"name": t.strip()[:100]} for t in str(text).split(",") if t.strip()]

            appearance_count = len(content_pages) if content_pages else _get_person_count(person)
            schema = _get_person_db_schema(notion)
            fields = _resolve_person_fields(schema)
            update_props = {}
            enriched_values = {
                "affiliation": enriched["affiliation"],
                "role": enriched["role"],
                "career": enriched["career"],
                "expertise": enriched["expertise"],
                "count": str(appearance_count),
            }
            for logical_key, raw_value in enriched_values.items():
                prop_name = fields.get(logical_key)
                if not prop_name:
                    continue
                ptype = schema.get(prop_name, "rich_text")
                if logical_key == "count":
                    if ptype == "number":
                        try:
                            update_props[prop_name] = {"number": int(appearance_count)}
                        except Exception:
                            update_props[prop_name] = {"number": 0}
                    else:
                        update_props[prop_name] = _build_person_prop_value(ptype, raw_value, rt, ms_enrich)
                    continue
                update_props[prop_name] = _build_person_prop_value(ptype, raw_value, rt, ms_enrich)
            notion.pages.update(
                page_id=page_id,
                properties=update_props,
            )
            print(f"  ✓ [4] Notion 속성 업데이트 (등장 횟수: {appearance_count})")

            # ── [5] 본문 재구성 ──────────────────────────────────
            channel_counts: dict[str, int] = {}
            for op in all_opinions:
                ch = op.get("channel", "")
                if ch:
                    channel_counts[ch] = channel_counts.get(ch, 0) + 1

            consistency = _analyze_opinion_consistency(name, all_opinions)
            blocks = _build_person_body_blocks(name, all_opinions, channel_counts, consistency)
            _clear_and_write_person_blocks(notion, page_id, name, blocks)
            print(f"  ✓ [5] 본문 재구성 ({len(all_opinions)}개 발언, 채널 {len(channel_counts)}종)")

            # ── [6] Economic_Expert 시트 업데이트 ───────────────
            save_person_to_expert_sheet(
                name,
                enriched["affiliation"],
                enriched["role"],
                enriched["career"],
                enriched["expertise"],
                appearance_count,
                notion=notion,
                person_page_id=page_id,
                source_url=source_url,
            )

            success += 1

        except Exception as e:
            logger.error(f"  ❌ [{i}/{total}] {name} 처리 실패: {e}", exc_info=True)
            fail += 1

        time.sleep(max(PERSON_SYNC_SLEEP_SEC, 0.2))  # API 레이트 리밋 방지

    print(f"\n{'='*50}")
    print(f"✅ 인물 DB 보강 완료 — 성공: {success} / 실패: {fail}")
    print(f"{'='*50}")


# ── 인물 DB 정리·중복 병합 ────────────────────────────────

def _best_value(*values: str) -> str:
    """비어있거나 '미상'/'정보 없음'이 아닌 첫 번째 값 반환"""
    for v in values:
        if v and v.strip() not in ("미상", "정보 없음", ""):
            return v
    return values[0] if values else ""


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _person_dedupe_deps() -> PersonDedupeDeps:
    return PersonDedupeDeps(
        get_person_prop=_get_person_prop,
        get_person_count=_get_person_count,
        get_all_persons_from_db=_get_all_persons_from_db,
        compact_identity_text=_compact_identity_text,
        normalize_identity_text=_normalize_identity_text,
        normalize_opinion_text=_normalize_opinion_text,
        dedup_opinions=_dedup_opinions,
        parse_opinions_from_person_blocks=_parse_opinions_from_person_blocks,
        list_person_blocks=lambda notion, page_id: notion.blocks.children.list(block_id=page_id).get("results", []),
        similarity=_similarity,
        jaccard=_jaccard,
        google_search=_google_search,
        gemini_json=_gemini_json,
        best_value=_best_value,
        analyze_opinion_consistency=_analyze_opinion_consistency,
        build_person_body_blocks=_build_person_body_blocks,
        clear_and_write_person_blocks=_clear_and_write_person_blocks,
        invalidate_person_db_cache=_invalidate_person_db_cache,
        person_name_aliases=_PERSON_NAME_ALIASES,
        person_sync_sleep_sec=PERSON_SYNC_SLEEP_SEC,
        logger=logger,
        print_fn=print,
    )


def _person_fingerprint(notion: NotionClient, page: dict) -> dict:
    return service_person_fingerprint(notion, page, _person_dedupe_deps())


def _duplicate_score(a: dict, b: dict) -> tuple[float, dict]:
    return service_duplicate_score(a, b, _person_dedupe_deps())


def _google_confirm_duplicate(a: dict, b: dict) -> tuple[bool, float, str]:
    return service_google_confirm_duplicate(a, b, _person_dedupe_deps())


def _cluster_groups_by_edges(nodes: list[dict], edges: list[tuple[int, int]]) -> list[list[dict]]:
    return service_cluster_groups_by_edges(nodes, edges)


def _find_duplicate_person_groups_hybrid(notion: NotionClient, all_persons: list[dict]) -> tuple[list[list[dict]], list[dict]]:
    return service_find_duplicate_person_groups_hybrid(notion, all_persons, _person_dedupe_deps())


def _auto_dedup_people_db(notion: NotionClient) -> dict:
    return service_auto_dedup_people_db(notion, _person_dedupe_deps())


def _find_duplicate_person_groups(all_persons: list[dict]) -> list[list[dict]]:
    """Gemini로 중복 인물 그룹 탐지. 각 그룹은 동일 인물의 페이지 목록."""
    if len(all_persons) < 2:
        return []

    persons_text = "\n".join(
        f"{i+1}. 이름={_get_person_prop(p, '이름')}, "
        f"소속={_get_person_prop(p, '소속')}, "
        f"직책={_get_person_prop(p, '직책')}"
        for i, p in enumerate(all_persons)
    )

    result = _gemini_json(f"""아래 인물 목록에서 동일 인물로 보이는 항목들을 그룹으로 묶어주세요.

인물 목록:
{persons_text}

규칙:
- 이름 표기가 약간 달라도 (오탈자, 띄어쓰기, 영문/한글 혼용) 동일 인물이면 같은 그룹으로
- 소속·직책이 유사하면 동일 인물로 판단
- 중복이 없는 단독 인물은 포함하지 마세요

JSON으로만 응답:
{{
  "duplicate_groups": [
    [1, 3],
    [2, 5, 7]
  ]
}}
중복이 전혀 없으면: {{"duplicate_groups": []}}""")

    groups: list[list[dict]] = []
    if result and isinstance(result, dict):
        for group_indices in result.get("duplicate_groups", []):
            group = [all_persons[idx - 1] for idx in group_indices
                     if isinstance(idx, int) and 1 <= idx <= len(all_persons)]
            if len(group) >= 2:
                groups.append(group)
    return groups


def _merge_person_group(notion: NotionClient, group: list[dict]):
    service_merge_person_group(notion, group, _person_dedupe_deps())


def _rebuild_expert_sheet(notion: NotionClient):
    """인물 DB 전체 기준으로 Economic_Expert 시트 재구성"""
    all_persons = _get_all_persons_from_db(notion)
    service = _get_sheets_service()
    sheet = service.spreadsheets()
    tab = EXPERT_SHEET_TAB

    # 기존 데이터는 먼저 읽기만 하고, 새 데이터 준비가 끝날 때까지 건드리지 않음
    try:
        existing = sheet.values().get(
            spreadsheetId=EXPERT_SHEET_ID, range=EXPERT_SHEET_FULL_RANGE
        ).execute().get("values", [])
    except Exception as e:
        print(f"  ⚠️ 시트 조회 실패: {e}")
        return

    # 전체 데이터 일괄 기록 (이름+소속 기준 중복 병합)
    merged: dict[tuple[str, str], dict] = {}
    for p in all_persons:
        name = _get_person_prop(p, "이름")
        if not name:
            continue
        affiliation = _get_person_prop(p, "소속")
        role = _get_person_prop(p, "직책")
        career = _get_person_prop(p, "주요 경력")
        expertise = _get_person_prop(p, "전문 분야")
        source_url = _get_person_prop(p, "근거 링크")
        confidence_score = _get_person_prop(p, "신뢰도 점수")
        confidence_status = _get_person_prop(p, "신뢰도 상태")
        if not source_url:
            info = collect_person_info_from_search(name, affiliation, role)
            source_url = info.get("source_url", "")
            if source_url:
                _set_person_source_url_if_missing(notion, p["id"], source_url)
        count = _get_person_count(p)
        body_fields = _extract_person_body_sheet_fields(notion, p["id"])
        _sync_person_summary_props(
            notion,
            p["id"],
            name,
            body_fields,
            source_url=source_url,
            confidence_score=confidence_score,
            confidence_status=confidence_status,
        )

        key = _person_sheet_key(name, affiliation)
        if key not in merged:
            merged[key] = {
                "name": name,
                "affiliation": affiliation,
                "role": role,
                "career": career,
                "expertise": expertise,
                "count": count,
                "latest_date": body_fields["latest_date"],
                "latest_channel": body_fields["latest_channel"],
                "latest_opinion": body_fields["latest_opinion"],
                "dominant_channel": body_fields["dominant_channel"],
                "top_channels": body_fields["top_channels"],
                "consistency_summary": body_fields["consistency_summary"],
                "source_url": source_url,
                "confidence_score": confidence_score,
                "confidence_status": confidence_status,
            }
            continue

        curr = merged[key]
        if len(role) > len(curr["role"]):
            curr["role"] = role
        if len(career) > len(curr["career"]):
            curr["career"] = career
        if len(expertise) > len(curr["expertise"]):
            curr["expertise"] = expertise
        curr["count"] += count
        if len(body_fields["latest_date"]) >= len(curr["latest_date"]):
            curr["latest_date"] = body_fields["latest_date"]
            curr["latest_channel"] = body_fields["latest_channel"]
            curr["latest_opinion"] = body_fields["latest_opinion"]
        if len(body_fields["top_channels"]) > len(curr["top_channels"]):
            curr["top_channels"] = body_fields["top_channels"]
        if len(body_fields["consistency_summary"]) > len(curr["consistency_summary"]):
            curr["consistency_summary"] = body_fields["consistency_summary"]
        if len(body_fields["dominant_channel"]) > len(curr["dominant_channel"]):
            curr["dominant_channel"] = body_fields["dominant_channel"]
        if len(source_url) > len(curr["source_url"]):
            curr["source_url"] = source_url
        if len(confidence_score) > len(curr["confidence_score"]):
            curr["confidence_score"] = confidence_score
        if len(confidence_status) > len(curr["confidence_status"]):
            curr["confidence_status"] = confidence_status

    rows = [
        [
            item["name"],
            item["affiliation"],
            item["role"],
            item["career"],
            item["expertise"],
            item["count"],
            item["latest_date"],
            item["latest_channel"],
            item["latest_opinion"],
            item["dominant_channel"],
            item["top_channels"],
            item["consistency_summary"],
            item["source_url"],
            item["confidence_score"],
            item["confidence_status"],
            _nickname_value(item["name"]),
        ]
        for item in merged.values()
    ]

    # 안전장치: 새 데이터가 비었는데 기존 데이터가 있으면 절대 비우지 않음
    if not rows and len(existing) > 1:
        print("  ⚠️ 재구성 중단: 새 데이터 0건(조회 실패 가능성). 기존 시트 데이터 유지.")
        return

    if rows:
        try:
            # 1) 헤더+데이터를 한 번에 덮어쓰기
            sheet.values().update(
                spreadsheetId=EXPERT_SHEET_ID,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                body={"values": [_EXPERT_SHEET_HEADERS] + rows},
            ).execute()
            # 2) 기존 행이 더 많았다면 남은 꼬리 행 정리
            old_len = len(existing)
            new_len = len(rows) + 1  # header 포함
            if old_len > new_len:
                sheet.values().clear(
                    spreadsheetId=EXPERT_SHEET_ID,
                    range=f"'{tab}'!A{new_len+1}:{EXPERT_SHEET_END_COL}{old_len}",
                ).execute()
            print(f"  → {len(rows)}명 기록 완료")
        except Exception as e:
            print(f"  ⚠️ 시트 기록 실패: {e}")
    else:
        # rows도 없고 기존도 없던 경우: 헤더만 보장
        try:
            sheet.values().update(
                spreadsheetId=EXPERT_SHEET_ID,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                body={"values": [_EXPERT_SHEET_HEADERS]},
            ).execute()
            print("  → 데이터 없음(헤더만 유지)")
        except Exception as e:
            print(f"  ⚠️ 헤더 설정 실패: {e}")


def _build_expert_snapshot_row(page: dict) -> list[str]:
    name = _get_person_prop(page, "이름")
    if not name:
        return []
    affiliation = _get_person_prop(page, "소속")
    role = _get_person_prop(page, "직책")
    dominant_channel = _get_person_prop(page, "대표 채널") or _get_person_prop(page, "최근 채널")
    person_types = _get_person_prop(page, "인물 유형")
    return [
        page.get("id", ""),
        name,
        _get_person_prop(page, "닉네임") or _nickname_value(name),
        _get_person_prop(page, "신뢰도 상태"),
        person_types,
        affiliation,
        role,
        dominant_channel,
        _get_person_prop(page, "TrustScore"),
        _get_person_prop(page, "TrustScore 밴드"),
        _get_person_prop(page, "TrustScore 신뢰도"),
        _get_person_prop(page, "해결 claim 수"),
        _get_person_prop(page, "대기 claim 수"),
        _get_person_prop(page, "방향 적중률"),
        _get_person_prop(page, "알파 점수"),
        _get_person_prop(page, "근거 제시 점수"),
        _get_person_prop(page, "입장 번복 플래그 수"),
        _get_person_prop(page, "마지막 TrustScore 갱신"),
    ]


def _snapshot_person_seed(page: dict) -> dict[str, str]:
    name = _get_person_prop(page, "이름")
    return {
        "person_id": page.get("id", ""),
        "person_name": name,
        "identity_status": _get_person_prop(page, "신뢰도 상태"),
        "person_types": _get_person_prop(page, "인물 유형"),
        "affiliation": _get_person_prop(page, "소속"),
        "role": _get_person_prop(page, "직책"),
        "dominant_channel": _get_person_prop(page, "대표 채널") or _get_person_prop(page, "최근 채널"),
    }


def _trust_value_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def _write_sheet_rows(
    *,
    spreadsheet_id: str,
    tab: str,
    headers: list[str],
    end_col: str,
    full_range: str,
    rows: list[list[object]],
    prevent_empty_overwrite: bool = False,
):
    service = _get_sheets_service()
    sheet = service.spreadsheets()
    try:
        existing = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=full_range,
        ).execute().get("values", [])
    except Exception as e:
        print(f"  ⚠️ 시트 조회 실패({tab}): {e}")
        return

    if prevent_empty_overwrite and not rows and len(existing) > 1:
        print(f"  ⚠️ 재구성 중단({tab}): 새 데이터 0건. 기존 시트 데이터 유지.")
        return

    try:
        payload = [headers] + rows if rows else [headers]
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": payload},
        ).execute()
        old_len = len(existing)
        new_len = len(rows) + 1
        if old_len > new_len:
            sheet.values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab}'!A{new_len+1}:{end_col}{old_len}",
            ).execute()
        if rows:
            print(f"  → {tab}: {len(rows)}행 기록 완료")
        else:
            print(f"  → {tab}: 데이터 없음(헤더만 유지)")
    except Exception as e:
        print(f"  ⚠️ 시트 기록 실패({tab}): {e}")


def _rebuild_expert_snapshot_sheet(notion: NotionClient):
    """LLM 문맥용 Expert_Snapshot 탭 재구성."""
    _ensure_tab_exists(EXPERT_SHEET_ID, EXPERT_SNAPSHOT_TAB)
    pages = _get_all_persons_from_db(notion)
    seeds = [_snapshot_person_seed(page) for page in pages if _get_person_prop(page, "이름")]
    bootstrap_stats = service_bootstrap_person_trust_rows(TRUST_DATA_DB_PATH, seeds)
    trust_rows = service_load_person_trust_rows(TRUST_DATA_DB_PATH)
    rows: list[list[str]] = []
    for page in pages:
        row = _build_expert_snapshot_row(page)
        if row:
            trust_row = trust_rows.get(page.get("id", ""), {})
            if trust_row:
                row[3] = trust_row.get("identity_status") or row[3]
                row[4] = trust_row.get("person_types") or row[4]
                row[5] = trust_row.get("affiliation") or row[5]
                row[6] = trust_row.get("role") or row[6]
                row[7] = trust_row.get("dominant_channel") or row[7]
                row[8] = _trust_value_text(trust_row.get("trust_score_total"))
                row[9] = trust_row.get("trust_score_band", "")
                row[10] = _trust_value_text(trust_row.get("trust_score_confidence"))
                row[11] = _trust_value_text(trust_row.get("resolved_claim_count"))
                row[12] = _trust_value_text(trust_row.get("pending_claim_count"))
                row[13] = _trust_value_text(trust_row.get("direction_accuracy"))
                row[14] = _trust_value_text(trust_row.get("alpha_score"))
                row[15] = _trust_value_text(trust_row.get("source_transparency_score"))
                row[16] = _trust_value_text(trust_row.get("contradiction_flag_count"))
                row[17] = trust_row.get("last_trustscore_updated_at", "")
            rows.append(row)
    rows.sort(key=lambda item: ((item[1] or "").strip(), (item[5] or "").strip()))
    print(
        f"  → Trust store bootstrap: inserted={bootstrap_stats['inserted']}, "
        f"updated={bootstrap_stats['updated']}"
    )
    _write_sheet_rows(
        spreadsheet_id=EXPERT_SHEET_ID,
        tab=EXPERT_SNAPSHOT_TAB,
        headers=_EXPERT_SNAPSHOT_HEADERS,
        end_col=EXPERT_SNAPSHOT_END_COL,
        full_range=EXPERT_SNAPSHOT_FULL_RANGE,
        rows=rows,
        prevent_empty_overwrite=True,
    )


def _init_review_queue_sheet():
    """LLM 검수 큐용 Review_Queue 탭 헤더 보장."""
    _ensure_tab_exists(EXPERT_SHEET_ID, REVIEW_QUEUE_TAB)
    service = _get_sheets_service()
    sheet = service.spreadsheets()
    try:
        existing = sheet.values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=REVIEW_QUEUE_FULL_RANGE,
        ).execute().get("values", [])
    except Exception as e:
        print(f"  ⚠️ 시트 조회 실패({REVIEW_QUEUE_TAB}): {e}")
        return

    if existing and any(existing[0]):
        print(f"  → {REVIEW_QUEUE_TAB}: 기존 헤더/데이터 유지")
        return

    try:
        sheet.values().update(
            spreadsheetId=EXPERT_SHEET_ID,
            range=f"'{REVIEW_QUEUE_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": [_REVIEW_QUEUE_HEADERS]},
        ).execute()
        print(f"  → {REVIEW_QUEUE_TAB}: 헤더 초기화 완료")
    except Exception as e:
        print(f"  ⚠️ 헤더 설정 실패({REVIEW_QUEUE_TAB}): {e}")


def _sync_review_queue_sheet():
    _ensure_tab_exists(EXPERT_SHEET_ID, REVIEW_QUEUE_TAB)
    service = _get_sheets_service()
    sheet = service.spreadsheets()
    try:
        existing = sheet.values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=REVIEW_QUEUE_FULL_RANGE,
        ).execute().get("values", [])
    except Exception as e:
        print(f"  ⚠️ 시트 조회 실패({REVIEW_QUEUE_TAB}): {e}")
        return

    preserved: dict[str, list[str]] = {}
    for row in existing[1:]:
        if not row:
            continue
        queue_id = row[0] if len(row) > 0 else ""
        if queue_id and queue_id not in preserved:
            preserved[queue_id] = row

    queue_rows = service_load_review_queue_rows(TRUST_DATA_DB_PATH)
    generated_ids: set[str] = set()
    rows: list[list[str]] = []
    for item in queue_rows:
        queue_id = item["queue_id"]
        generated_ids.add(queue_id)
        prev = preserved.get(queue_id, [])
        prev_status = prev[4] if len(prev) > 4 else ""
        merged_status = prev_status if prev_status == "in_progress" else item["status"]
        row = [
            queue_id,
            item["created_at"],
            item["queue_type"],
            item["priority"],
            merged_status,
            item["target_type"],
            item["target_id"],
            item["target_name"],
            item["source_url"],
            item["evidence_summary"],
            item["suggested_action"],
            prev[11] if len(prev) > 11 else item["owner"],
            prev[12] if len(prev) > 12 else item["last_reviewed_at"],
            prev[13] if len(prev) > 13 else item["notes"],
        ]
        rows.append(row)

    for queue_id, prev in preserved.items():
        if queue_id in generated_ids:
            continue
        if not prev:
            continue
        padded = prev + [""] * max(0, len(_REVIEW_QUEUE_HEADERS) - len(prev))
        if len(padded) > 9 and "[DB-resolved]" not in padded[9]:
            padded[9] = f"[DB-resolved] {padded[9]}".strip()
        rows.append(padded[:len(_REVIEW_QUEUE_HEADERS)])

    _write_sheet_rows(
        spreadsheet_id=EXPERT_SHEET_ID,
        tab=REVIEW_QUEUE_TAB,
        headers=_REVIEW_QUEUE_HEADERS,
        end_col=REVIEW_QUEUE_END_COL,
        full_range=REVIEW_QUEUE_FULL_RANGE,
        rows=rows,
        prevent_empty_overwrite=False,
    )


def _parse_resolution_notes(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in (raw or "").split(";"):
        chunk = part.strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        k = key.strip()
        v = value.strip()
        if k:
            out[k] = v
    return out


def apply_review_queue_resolutions():
    _ensure_tab_exists(EXPERT_SHEET_ID, REVIEW_QUEUE_TAB)
    service = _get_sheets_service()
    sheet = service.spreadsheets()
    values = sheet.values().get(
        spreadsheetId=EXPERT_SHEET_ID,
        range=REVIEW_QUEUE_FULL_RANGE,
    ).execute().get("values", [])
    registry = service_load_claim_override_registry(TRUST_CLAIM_OVERRIDE_PATH)
    claim_overrides = registry.get("claim_overrides", {}) or {}
    applied = 0
    skipped = 0
    for row in values[1:]:
        if len(row) < 14:
            skipped += 1
            continue
        queue_type = row[2].strip()
        status = row[4].strip().lower()
        target_type = row[5].strip()
        target_id = row[6].strip()
        notes = row[13].strip()
        if queue_type not in ("symbol_review", "outcome_review") or status != "resolved" or target_type != "claim" or not target_id:
            continue
        parsed = _parse_resolution_notes(notes)
        stooq_symbol = parsed.get("stooq_symbol", "")
        benchmark_symbol = parsed.get("benchmark_symbol", "")
        anchor_at = parsed.get("anchor_at", "")
        if anchor_at:
            try:
                datetime.fromisoformat(anchor_at)
            except Exception:
                print(f"  ⚠️ anchor_at 형식 오류 스킵: claim={target_id}, anchor_at={anchor_at}")
                skipped += 1
                continue
        if not any((stooq_symbol, benchmark_symbol, anchor_at)):
            skipped += 1
            continue
        existing_override = claim_overrides.get(target_id, {})
        updated_override = {
            "stooq_symbol": stooq_symbol or existing_override.get("stooq_symbol", ""),
            "benchmark_symbol": benchmark_symbol or existing_override.get("benchmark_symbol", ""),
            "anchor_at": anchor_at or existing_override.get("anchor_at", ""),
            "note": parsed.get("note", existing_override.get("note", "manual_review_queue_resolution")),
            "resolved_at": datetime.now().astimezone().isoformat(),
        }
        claim_overrides[target_id] = updated_override
        applied += 1
    registry["claim_overrides"] = claim_overrides
    service_save_claim_override_registry(TRUST_CLAIM_OVERRIDE_PATH, registry)
    print(f"  → review queue resolution 반영: applied={applied}, skipped={skipped}")


def init_trust_store():
    info = service_init_trust_data_db(TRUST_DATA_DB_PATH)
    print(f"  → trust store 준비 완료: {info['db_path']}")


def bootstrap_trust_store():
    notion = get_notion_client()
    pages = _get_all_persons_from_db(notion)
    seeds = [_snapshot_person_seed(page) for page in pages if _get_person_prop(page, "이름")]
    stats = service_bootstrap_person_trust_rows(TRUST_DATA_DB_PATH, seeds)
    print(
        f"  → trust store bootstrap 완료: inserted={stats['inserted']}, "
        f"updated={stats['updated']}"
    )


def ingest_claim_samples():
    notion = get_notion_client()
    pages = _get_all_persons_from_db(notion)
    person_name_to_id: dict[str, str] = {}
    for page in pages:
        name = _get_person_prop(page, "이름")
        if name:
            person_name_to_id[name] = page.get("id", "")
    docs = service_load_claim_sample_docs(CLAIM_SAMPLE_DIR)
    stats = service_ingest_claim_samples(TRUST_DATA_DB_PATH, docs, person_name_to_id)
    print(
        f"  → claim sample ingest 완료: inserted={stats['inserted']}, "
        f"updated={stats['updated']}, skipped={stats['skipped']}"
    )


def recompute_trust_scores():
    stats = service_recompute_person_trust_scores(TRUST_DATA_DB_PATH)
    print(f"  → trust score 재계산 완료: updated={stats['updated']}")


def refresh_claim_outcomes(force: bool = False):
    mapping_registry = service_load_symbol_mapping_registry(TRUST_SYMBOL_MAP_PATH)
    override_registry = service_load_claim_override_registry(TRUST_CLAIM_OVERRIDE_PATH)
    stats = service_refresh_claim_outcomes(
        TRUST_DATA_DB_PATH,
        force=force,
        mapping_registry=mapping_registry,
        override_registry=override_registry,
    )
    print(
        f"  → claim outcome 갱신 완료: updated={stats['updated']}, "
        f"unresolved={stats['unresolved']}, needs_review={stats['needs_review']}"
    )


def sync_llm_context_sheets():
    """LLM 문맥용 시트 탭 동기화."""
    notion = get_notion_client()
    service_init_trust_data_db(TRUST_DATA_DB_PATH)
    _rebuild_expert_snapshot_sheet(notion)
    _sync_review_queue_sheet()


def _dedup_person_page_opinions(notion: NotionClient):
    """인물 DB 전체 페이지의 본문 발언 중복을 정리."""
    pages = _get_all_persons_from_db(notion)
    changed = 0
    for p in pages:
        page_id = p["id"]
        name = _get_person_prop(p, "이름")
        if not name:
            continue
        try:
            blocks = notion.blocks.children.list(block_id=page_id).get("results", [])
        except Exception:
            continue

        raw = _parse_opinions_from_person_blocks(blocks)
        deduped = _dedup_opinions(raw)
        if len(deduped) >= len(raw):
            continue

        channel_counts: dict[str, int] = {}
        for op in deduped:
            ch = op.get("channel", "")
            if ch:
                channel_counts[ch] = channel_counts.get(ch, 0) + 1

        body_fields = _extract_person_body_sheet_fields(notion, page_id)
        consistency = {
            "changes": [],
            "consistent": [],
            "summary": body_fields.get("consistency_summary") or "일관된 발언 유지 중",
        }
        new_blocks = _build_person_body_blocks(name, deduped, channel_counts, consistency)
        _clear_and_write_person_blocks(notion, page_id, name, new_blocks)
        changed += 1
        time.sleep(PERSON_SYNC_SLEEP_SEC)

    print(f"  → 본문 발언 중복 정리 완료: {changed}명")


def check_people_sync_status():
    """노션 인물 DB와 Economic_Expert 시트의 불일치 점검."""
    notion = get_notion_client()
    all_persons = _get_all_persons_from_db(notion)
    schema = _get_person_db_schema(notion)
    fields = _resolve_person_fields(schema)
    source_prop_name = fields.get("source_url")
    source_compare_enabled = bool(source_prop_name)

    notion_map: dict[tuple[str, str], dict] = {}
    for p in all_persons:
        name = _get_person_prop(p, "이름")
        aff = _get_person_prop(p, "소속")
        if not name:
            continue
        notion_map[_person_sheet_key(name, aff)] = {
            "name": name,
            "aff": aff,
            "role": _get_person_prop(p, "직책"),
            "source_url": _get_person_prop(p, source_prop_name) if source_prop_name else "",
            "confidence_score": _get_person_prop(p, "신뢰도 점수"),
            "confidence_status": _get_person_prop(p, "신뢰도 상태"),
        }

    service = _get_sheets_service()
    tab = EXPERT_SHEET_TAB
    values = service.spreadsheets().values().get(
        spreadsheetId=EXPERT_SHEET_ID,
        range=EXPERT_SHEET_FULL_RANGE,
    ).execute().get("values", [])

    sheet_map: dict[tuple[str, str], dict] = {}
    for row in values[1:]:
        if not row:
            continue
        name = row[0] if len(row) > 0 else ""
        aff = row[1] if len(row) > 1 else ""
        role = row[2] if len(row) > 2 else ""
        src = row[12] if len(row) > 12 else ""
        dom_ch = row[9] if len(row) > 9 else ""
        top3 = row[10] if len(row) > 10 else ""
        sheet_map[_person_sheet_key(name, aff)] = {
            "name": name,
            "aff": aff,
            "role": role,
            "source_url": src,
            "confidence_score": row[13] if len(row) > 13 else "",
            "confidence_status": row[14] if len(row) > 14 else "",
            "dominant_channel": dom_ch,
            "top_channels": top3,
        }

    notion_keys = set(notion_map.keys())
    sheet_keys = set(sheet_map.keys())
    only_notion = notion_keys - sheet_keys
    only_sheet = sheet_keys - notion_keys
    missing_source = [v for v in sheet_map.values() if not (v.get("source_url") or "").strip()]
    mismatch_rows = []
    for k in (notion_keys & sheet_keys):
        n = notion_map[k]
        s = sheet_map[k]
        role_mis = _normalize_identity_text(n.get("role", "")) != _normalize_identity_text(s.get("role", ""))
        src_mis = False
        conf_score_mis = _normalize_identity_text(n.get("confidence_score", "")) != _normalize_identity_text(s.get("confidence_score", ""))
        conf_status_mis = _normalize_identity_text(n.get("confidence_status", "")) != _normalize_identity_text(s.get("confidence_status", ""))
        if source_compare_enabled:
            src_mis = bool(n.get("source_url", "")) != bool(s.get("source_url", ""))
        if role_mis or src_mis or conf_score_mis or conf_status_mis:
            mismatch_rows.append(n.get("name", ""))

    # 이름+직책 붙은 패턴 의심치
    notion_glued = []
    glued_pattern = re.compile(r"(애널리스트|이코노미스트|대표|본부장|센터장|팀장|상무|이사|전문가|교수|박사|연구원|기자)$")
    glued = []
    for v in notion_map.values():
        nm = _normalize_identity_text(v.get("name", ""))
        rl = _normalize_identity_text(v.get("role", ""))
        if rl in ("", "미상", "정보 없음") and glued_pattern.search(nm):
            notion_glued.append(v)

    for v in sheet_map.values():
        nm = _normalize_identity_text(v.get("name", ""))
        rl = _normalize_identity_text(v.get("role", ""))
        if rl in ("", "미상", "정보 없음") and glued_pattern.search(nm):
            glued.append(v)

    print("\n📋 인물 데이터 동기화 점검")
    print(f"  - Notion 키 수: {len(notion_keys)}")
    print(f"  - Sheet  키 수: {len(sheet_keys)}")
    print(f"  - Notion에만 있음: {len(only_notion)}")
    print(f"  - Sheet에만 있음: {len(only_sheet)}")
    print(f"  - 출연자 검증 링크 누락: {len(missing_source)}")
    print(f"  - 노션/시트 컬럼 불일치: {len(mismatch_rows)}")
    if not source_compare_enabled:
        print("  - 참고: Notion 근거 링크(URL) 컬럼이 없어 링크 컬럼 비교는 제외됨")
    print(f"  - 이름/직책 붙음 의심(Notion): {len(notion_glued)}")
    print(f"  - 이름/직책 붙음 의심(Sheet): {len(glued)}")

    if only_notion:
        sample = [notion_map[k]["name"] for k in list(only_notion)[:10]]
        print(f"    · Notion only 샘플: {sample}")
    if only_sheet:
        sample = [sheet_map[k]["name"] for k in list(only_sheet)[:10]]
        print(f"    · Sheet only 샘플: {sample}")
    if missing_source:
        sample = [x["name"] for x in missing_source[:10]]
        print(f"    · 링크 누락 샘플: {sample}")
    if mismatch_rows:
        print(f"    · 컬럼 불일치 샘플: {mismatch_rows[:10]}")
    if glued:
        sample = [x["name"] for x in glued[:10]]
        print(f"    · Sheet 붙음 의심 샘플: {sample}")
    if notion_glued:
        sample = [x["name"] for x in notion_glued[:10]]
        print(f"    · Notion 붙음 의심 샘플: {sample}")

    suspects = _non_economic_person_suspects(all_persons)
    print(f"  - 비경제/카테고리 검토 필요: {len(suspects)}")
    if suspects:
        sample = [s["name"] for s in suspects[:10]]
        print(f"    · 검토 필요 샘플: {sample}")


def reconcile_people_sync():
    """Notion 인물 DB 기준으로 시트 누락 항목 보강 + 이름 표기 정규화."""
    with _PERSON_DB_LOCK:
        notion = get_notion_client()
        all_persons = _get_all_persons_from_db(notion)
        schema = _get_person_db_schema(notion)
        fields = _resolve_person_fields(schema)
        rt = lambda t: [{"text": {"content": str(t)[:2000]}}]
        ms = lambda t: [{"name": s.strip()[:100]} for s in str(t).split(",") if s.strip()]

        service = _get_sheets_service()
        tab = EXPERT_SHEET_TAB
        values = service.spreadsheets().values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=EXPERT_SHEET_FULL_RANGE,
        ).execute().get("values", [])
        sheet_keys: set[tuple[str, str]] = set()
        for row in values[1:]:
            if not row:
                continue
            n = row[0] if len(row) > 0 else ""
            a = row[1] if len(row) > 1 else ""
            sheet_keys.add(_person_sheet_key(n, a))

        notion_keys: set[tuple[str, str]] = set()

        renamed = 0
        upserted = 0
        skipped = 0

        for p in all_persons:
            page_id = p.get("id", "")
            raw_name = _get_person_prop(p, "이름")
            raw_aff = _get_person_prop(p, "소속")
            raw_role = _get_person_prop(p, "직책")
            if not raw_name:
                skipped += 1
                continue
            name, affiliation, role = _sanitize_person_fields(raw_name, raw_aff, raw_role)
            notion_keys.add(_person_sheet_key(name, affiliation))

            # Notion 이름/소속/직책 정규화 반영
            if (name != raw_name) or (affiliation != raw_aff) or (role != raw_role):
                try:
                    payload = {}
                    if fields.get("name"):
                        ptype = schema.get(fields["name"], "title")
                        payload[fields["name"]] = _build_person_prop_value(ptype, name, rt, ms)
                    if fields.get("affiliation"):
                        ptype = schema.get(fields["affiliation"], "rich_text")
                        payload[fields["affiliation"]] = _build_person_prop_value(ptype, affiliation, rt, ms)
                    if fields.get("role"):
                        ptype = schema.get(fields["role"], "rich_text")
                        payload[fields["role"]] = _build_person_prop_value(ptype, role, rt, ms)
                    if payload:
                        notion.pages.update(page_id=page_id, properties=payload)
                        renamed += 1
                except Exception as e:
                    logger.error(f"인물 정규화 업데이트 실패: {raw_name!r} ({e})")

            key = _person_sheet_key(name, affiliation)
            if key in sheet_keys:
                skipped += 1
                continue

            career = _get_person_prop(p, "주요 경력")
            expertise = _get_person_prop(p, "전문 분야")
            source_url = _get_person_prop(p, "근거 링크")
            count = _get_person_count(p)
            body_fields = _extract_person_body_sheet_fields(notion, page_id)

            save_person_to_expert_sheet(
                name, affiliation, role, career, expertise, count,
                notion=notion, person_page_id=page_id, source_url=source_url, body_fields_override=body_fields,
            )
            sheet_keys.add(key)
            upserted += 1
            time.sleep(max(PERSON_SYNC_SLEEP_SEC, 0.1))

        # 시트에만 남은 stale 행 정리: Notion을 기준으로 모든 orphan row 제거
        values_after = service.spreadsheets().values().get(
            spreadsheetId=EXPERT_SHEET_ID,
            range=EXPERT_SHEET_FULL_RANGE,
        ).execute().get("values", [])
        stale_rows: list[int] = []
        for i, row in enumerate(values_after[1:], start=2):
            if not row:
                continue
            row_name = row[0] if len(row) > 0 else ""
            row_aff = row[1] if len(row) > 1 else ""
            row_role = row[2] if len(row) > 2 else ""
            key = _person_sheet_key(row_name, row_aff)
            if key not in notion_keys:
                s_name, s_aff, _ = _sanitize_person_fields(row_name, row_aff, row_role)
                if _person_sheet_key(s_name, s_aff) not in notion_keys:
                    stale_rows.append(i)
        _delete_sheet_rows(EXPERT_SHEET_ID, tab, stale_rows)

    print(
        f"🔁 인물 동기화 보정 완료: renamed={renamed}, sheet_upsert={upserted}, stale_removed={len(stale_rows)}, skipped={skipped}, total={len(all_persons)}"
    )
    check_people_sync_status()


def check_non_economic_people():
    """비경제/허용 카테고리 외 인물 의심 항목 상세 출력."""
    notion = get_notion_client()
    try:
        all_persons = _get_all_persons_from_db_retry(notion)
    except Exception as exc:
        print("\n📋 비경제 인물 의심 점검")
        print(f"  ❌ 조회 실패: {exc}")
        return
    suspects = _non_economic_person_suspects(all_persons)
    print("\n📋 비경제 인물 의심 점검")
    print(f"  - 총 인물: {len(all_persons)}")
    print(f"  - 의심 항목: {len(suspects)}")
    for i, s in enumerate(suspects, 1):
        print(
            f"  [{i}] 사유={s.get('reason', '')}, 이름={s['name']!r}, 소속={s['affiliation']!r}, "
            f"직책={s['role']!r}, 전문분야={s['expertise']!r}, page={s['page_id'][:8]}…"
        )


def queue_non_economic_people_review():
    """비경제/허용 카테고리 외 인물 의심 항목을 텔레그램 수동 검토 큐에 적재."""
    notion = get_notion_client()
    try:
        all_persons = _get_all_persons_from_db_retry(notion)
    except Exception as exc:
        print("\n🧾 비경제/카테고리 검토 큐 적재")
        print(f"  ❌ 조회 실패: {exc}")
        return
    suspects = _non_economic_person_suspects(all_persons)
    queued = 0

    print("\n🧾 비경제/카테고리 검토 큐 적재")
    print(f"  - 총 인물: {len(all_persons)}")
    print(f"  - 검토 대상: {len(suspects)}")

    for suspect in suspects:
        reasons = [suspect.get("reason", "검토 필요")]
        latest_opinion = _get_person_prop(
            next((p for p in all_persons if p.get("id", "") == suspect.get("page_id", "")), {}),
            "최근 발언",
        )
        if latest_opinion:
            reasons.append(f"최근 발언: {latest_opinion[:80]}")
        _notify_uncertain_person_with_ticket(
            suspect.get("page_id", ""),
            suspect.get("name", "미상"),
            suspect.get("affiliation", "정보 없음"),
            suspect.get("role", "미상"),
            reasons,
        )
        queued += 1
        print(
            f"  - 큐 적재: {suspect.get('name','미상')} / {suspect.get('affiliation','정보 없음')} "
            f"/ {suspect.get('reason','검토 필요')}"
        )

    print(f"✅ 검토 큐 적재 완료: {queued}건")


def run_healthcheck_once() -> bool:
    """운영 점검 1회 실행(키 유효성 + 인물 동기화 정합성)."""
    print("\n🩺 Healthcheck 시작")
    ok = True
    try:
        keys_ok = check_runtime_keys()
        ok = ok and keys_ok
        print(f"  - 키 점검: {'OK' if keys_ok else 'FAIL'}")
    except Exception as e:
        print(f"  - 키 점검 예외: {e}")
        ok = False

    try:
        check_people_sync_status()
        print("  - 인물 동기화 점검: OK")
    except Exception as e:
        print(f"  - 인물 동기화 점검 예외: {e}")
        ok = False

    _load_failed_url_queue()
    print(f"🩺 Healthcheck 완료: {'정상' if ok else '이상 있음'}")
    append_ops_event(
        OPS_EVENT_LOG_PATH,
        "healthcheck",
        {"ok": ok, "queue_size": len(_FAILED_URL_QUEUE)},
        logger=logger,
    )
    return ok


def clean_people_full():
    """인물 DB 전체 클렌징 1회 실행: 동기화→중복정리→이름정리→시트재구성."""
    notion = get_notion_client()
    print("\n🧹 인물 DB 전체 클렌징 시작")
    sync_people_from_notion()
    purge_stats = _purge_people_without_youtube_source(notion)
    print(
        f"  → 근거 유튜브 링크 기준 정리: total={purge_stats['total']}, "
        f"kept={purge_stats['kept']}, purged={purge_stats['purged']}"
    )
    if purge_stats["samples"]:
        print(f"    · 삭제 샘플: {purge_stats['samples']}")
    _enrich_missing_person_profiles(notion)
    stats = _auto_dedup_people_db(notion)
    print(
        f"  → 중복정리: groups={stats['groups']}, forced_groups={stats.get('forced_groups', 0)}, "
        f"merged_pages={stats['merged_pages']}, review_pending={stats['review_cases']}"
    )
    _normalize_person_name_column(notion)
    _dedup_person_page_opinions(notion)
    _rebuild_expert_sheet(notion)
    check_people_sync_status()
    print("✅ 인물 DB 전체 클렌징 완료\n")


def backfill_person_source_links():
    """인물 DB/시트의 근거 링크를 강제로 백필."""
    notion = get_notion_client()
    pages = _get_all_persons_from_db(notion)
    done = 0

    for p in pages:
        page_id = p["id"]
        name = _get_person_prop(p, "이름")
        if not name:
            continue
        affiliation = _get_person_prop(p, "소속")
        role = _get_person_prop(p, "직책")
        career = _get_person_prop(p, "주요 경력")
        expertise = _get_person_prop(p, "전문 분야")
        count = _get_person_count(p)
        source_url = _get_person_prop(p, "근거 링크")

        if not source_url:
            info = collect_person_info_from_search(name, affiliation, role)
            source_url = info.get("source_url", "")
            if not source_url:
                items = _google_search_items(f"{name} {affiliation} {role} 경제", num=3)
                source_url = _pick_best_source_link(items, name, affiliation)
            if source_url:
                _set_person_source_url_if_missing(notion, page_id, source_url)

        save_person_to_expert_sheet(
            name, affiliation, role, career, expertise, count,
            notion=notion, person_page_id=page_id, source_url=source_url,
        )
        if source_url:
            done += 1
        time.sleep(PERSON_SYNC_SLEEP_SEC)

    print(f"🔗 근거 링크 백필 완료: {done}건")


def rebuild_people_db():
    """인물 DB 중복 제거·정리 후 Economic_Expert 시트 재구성 (--rebuild-people)"""
    with _PERSON_DB_LOCK:
        service_rebuild_people_db(
            RebuildPeopleDeps(
                get_notion_client=get_notion_client,
                get_all_persons_from_db=_get_all_persons_from_db,
                find_duplicate_person_groups_hybrid=_find_duplicate_person_groups_hybrid,
                get_person_prop=_get_person_prop,
                person_fingerprint=_person_fingerprint,
                cluster_groups_by_edges=_cluster_groups_by_edges,
                merge_person_group=_merge_person_group,
                rebuild_expert_sheet=_rebuild_expert_sheet,
                person_sync_sleep_sec=PERSON_SYNC_SLEEP_SEC,
                sys_module=sys,
                print_fn=print,
            )
        )


# ── 공통 콘텐츠 파이프라인 ───────────────────────────────
def _pipeline_invalid_url(notion: NotionClient, page: PageContext, job: VideoJob) -> PipelineResult:
    print("  ✗ 유효하지 않은 유튜브 URL")
    notion.pages.update(
        page_id=page.page_id,
        properties={"주제": {"rich_text": [{"text": {"content": "⚠️ 유효하지 않은 URL"}}]}},
    )
    return PipelineResult(status="perm_fail", page_id=page.page_id, message="invalid_url")


def _pipeline_missing_metadata(page: PageContext, job: VideoJob) -> PipelineResult:
    print("  ✗ 영상 정보 없음")
    return PipelineResult(status="temp_fail", page_id=page.page_id, message="metadata_missing")


def _build_unknown_person_pipeline_handler(
    notion: NotionClient,
    session_key: str,
):
    def _handler(
        page: PageContext,
        job: VideoJob,
        evidence,
        analysis: dict,
    ) -> Optional[PipelineResult]:
        if not job.allow_interactive_review:
            return None
        person = analysis.get("person", {}) if isinstance(analysis.get("person"), dict) else {}
        pname = person.get("name", "미상")
        if not _is_unknown_person_name(pname):
            return None
        _unknown_person_sessions[session_key] = {
            "stage": "confirm",
            "notion": notion,
            "page_id": page.page_id,
            "props": page.props,
            "url": job.url,
            "metadata": evidence.metadata,
            "analysis": analysis,
        }
        return PipelineResult(
            status="needs_review",
            page_id=page.page_id,
            message="인물이 검색되지 않았습니다. 미상으로 남길까요?",
            analysis=analysis,
        )

    return _handler


def _build_content_pipeline_deps(
    notion: NotionClient,
    unknown_person_handler=None,
) -> ContentPipelineDeps:
    def _finalize(page: PageContext, job: VideoJob, evidence, analysis: dict) -> Optional[dict]:
        return _finalize_page_with_analysis(
            notion,
            page.page_id,
            page.props,
            job.url,
            evidence.metadata,
            analysis,
        )

    def _on_exception(page: PageContext, job: VideoJob, exc: Exception) -> PipelineResult:
        print(f"  ❌ 파이프라인 처리 오류: {exc}")
        if job.url:
            _enqueue_failed_url(job.url, f"{job.source}_pipeline_error:{exc}")
        try:
            notion.pages.update(page_id=page.page_id, properties={"주제": {"rich_text": []}})
        except Exception:
            pass
        return PipelineResult(status="temp_fail", page_id=page.page_id, message=str(exc))

    return ContentPipelineDeps(
        extract_video_id=extract_video_id,
        fetch_metadata=fetch_youtube_metadata,
        fetch_transcript=fetch_transcript,
        fetch_comments=fetch_youtube_comments,
        fetch_channel_about=fetch_channel_about,
        fetch_channel_titles=fetch_channel_recent_video_titles,
        extract_speaker_hint=_extract_speaker_from_transcript,
        detect_recurring_person=_detect_recurring_person_from_titles,
        analyze=analyze_with_gemini,
        finalize=_finalize,
        on_invalid_url=lambda page, job: _pipeline_invalid_url(notion, page, job),
        on_missing_metadata=_pipeline_missing_metadata,
        on_unknown_person=unknown_person_handler,
        on_exception=_on_exception,
    )


# ── 단일 페이지 처리 ─────────────────────────────────────
def _finalize_page_with_analysis(
    notion: NotionClient,
    page_id: str,
    props: dict,
    url: str,
    metadata: dict,
    analysis: dict,
) -> Optional[dict]:
    """분석 완료된 결과를 노션/인물DB/시트에 반영."""
    person = analysis.get("person", {})
    name = person.get("name", "미상")
    role = person.get("role", "미상")
    affiliation = person.get("affiliation", "미상")
    channel = metadata.get("channel", "")

    # 1순위 보조: 설명글·제목 패턴 매칭으로 이름/직책 보정
    desc_name, desc_aff, desc_role = _extract_person_from_description(
        metadata.get("description", ""), metadata.get("title", "")
    )
    if desc_name and (not name or name == "미상"):
        name = desc_name
        print(f"  [PERSON] 설명글 패턴으로 이름 보정: {name}")
    if desc_role and (not role or role == "미상"):
        role = desc_role

    print(f"  → 인물 검증 중: {name} ({role}) / {affiliation}")
    # verify_person에 channel 전달 — 2순위(전문가 DB) → 3순위(Google Search) 순으로 검증
    verified_name, verified_affiliation, verified_role = verify_person(name, affiliation, role, channel=channel)
    verified_name, verified_affiliation, verified_role = _sanitize_person_fields(
        verified_name, verified_affiliation, verified_role
    )
    canonical_name, alias_name = _resolve_canonical_person_identity(
        verified_name,
        verified_affiliation,
        verified_role,
        channel=channel,
    )
    if alias_name:
        print(f"  [PERSON] canonical/alias 적용: {canonical_name} (alias: {alias_name})")
    verified_name = canonical_name

    products = analysis.get("mentioned_products", [])
    if products:
        print(f"  → 투자상품 검증 중: {len(products)}개")
        analysis["mentioned_products"] = verify_products(products)

    person_str, timestamp = write_notion_result(
        notion, page_id,
        video_title=metadata.get("title", ""),
        analysis=analysis,
        verified_name=verified_name,
        verified_affiliation=verified_affiliation,
        role=verified_role,
        channel=channel,
    )
    print(f"  ✓ 노션 기록 완료")

    # 인물 DB 처리 (검색 → 생성/업데이트 → 발언 누적 → 일관성 검증)
    person = analysis.get("person", {})
    person_page_id = process_person_db(
        notion,
        name=verified_name,
        affiliation=verified_affiliation,
        role=verified_role,
        opinion=analysis.get("opinion", ""),
        channel=metadata.get("channel", ""),
    )
    if person_page_id and alias_name:
        _sync_person_identity_props(notion, person_page_id, verified_name, alias_name)

    # 콘텐츠 DB ↔ 인물 DB relation 연결
    if person_page_id:
        try:
            relation_prop = _resolve_content_person_relation_prop(props)
            # 페이지 스냅샷이 오래됐을 수 있어 1회 재조회 후 다시 탐지
            if not relation_prop:
                fresh_page = notion.pages.retrieve(page_id=page_id)
                fresh_props = fresh_page.get("properties", {})
                relation_prop = _resolve_content_person_relation_prop(fresh_props)
            candidates: list[str] = []
            if relation_prop:
                candidates.append(relation_prop)
            if CONTENT_PERSON_RELATION_PROP and CONTENT_PERSON_RELATION_PROP not in candidates:
                candidates.append(CONTENT_PERSON_RELATION_PROP)

            if not candidates:
                print("  ℹ️ 콘텐츠 DB에 인물 relation 컬럼이 없어 연결을 건너뜁니다.")
                candidates = []

            last_err = None
            linked = False
            for cand in candidates:
                try:
                    notion.pages.update(
                        page_id=page_id,
                        properties={cand: {"relation": [{"id": person_page_id}]}},
                    )
                    print(f"  ✓ 콘텐츠 DB ↔ 인물 DB 연결 완료 ({cand})")
                    linked = True
                    break
                except Exception as e:
                    last_err = e
                    continue

            if candidates and not linked:
                available_types = {k: v.get("type") for k, v in (props or {}).items()}
                raise ValueError(
                    f"relation 연결 실패. candidates={candidates}, properties={available_types}, last_err={last_err}"
                )
        except Exception as e:
            print(f"  ⚠️ Relation 연결 실패: {e}")

    # 노션 기록 성공 후 구글 시트에 기록
    write_to_sheet(
        url=url,
        video_title=metadata.get("title", ""),
        channel=metadata.get("channel", ""),
        analysis=analysis,
        person_str=person_str,
        timestamp=timestamp,
    )
    print(f"  ✓ 완료: {analysis.get('summary', '')}")

    return {
        "title": metadata.get("title", ""),
        "hashtags": " ".join(analysis.get("hashtags", [])),
        "summary": analysis.get("summary", ""),
        "person": f"{verified_affiliation} ({verified_role}) / {verified_name}" if verified_role != "미상" else f"{verified_affiliation} / {verified_name}",
        "opinion": analysis.get("opinion", ""),
    }


def process_page(notion: NotionClient, page: dict) -> Optional[dict]:
    """페이지 분석 후 결과 dict 반환. 실패 시 None."""
    page_id = page["id"]
    props = page.get("properties", {})

    url_prop = props.get("URL", {})
    url = url_prop.get("url", "") or ""
    print(f"\n▶ 페이지 {page_id[:8]}… 처리 중: {url}")
    job = VideoJob(url=url, source="notion", page_id=page_id)
    page_ctx = PageContext(page_id=page_id, props=props)
    pipeline_result = run_content_pipeline(job, page_ctx, _build_content_pipeline_deps(notion))
    return pipeline_result.payload if pipeline_result.status == "done" else None


# ── 인물 str 파싱 헬퍼 ────────────────────────────────────
def _parse_person_str(person_str: str) -> tuple[str, str, str]:
    """'소속 (직책) / 이름' 또는 '소속 / 이름' 형식에서 (name, affiliation, role) 추출"""
    if not person_str:
        return "", "", ""
    parts = person_str.rsplit("/", 1)
    if len(parts) < 2:
        return person_str.strip(), "", ""
    name = parts[1].strip()
    left = parts[0].strip()
    role_match = re.search(r'\(([^)]+)\)', left)
    if role_match:
        role = role_match.group(1).strip()
        affiliation = left[:role_match.start()].strip()
    else:
        role = ""
        affiliation = left
    return _sanitize_person_fields(name, affiliation, role)


# ── 기존 콘텐츠 DB → 인물 DB 일괄 동기화 ─────────────────
def sync_people_from_notion():
    """콘텐츠 DB의 완성된 영상들을 순회하며 인물 DB 등록·연결 (--sync-people)"""
    with _PERSON_DB_LOCK:
        service_sync_people_from_notion(
            SyncPeopleDeps(
                get_notion_client=get_notion_client,
                notion_database_id=NOTION_DATABASE_ID,
                notion_api_key=NOTION_API_KEY,
                httpx_module=httpx,
                get_rich_text=_get_rich_text,
                parse_person_str=_parse_person_str,
                resolve_content_person_relation_prop=_resolve_content_person_relation_prop,
                get_person_prop=_get_person_prop,
                get_person_count=_get_person_count,
                google_search_items=_google_search_items,
                pick_best_source_link=_pick_best_source_link,
                set_person_source_url_if_missing=_set_person_source_url_if_missing,
                save_person_to_expert_sheet=save_person_to_expert_sheet,
                extract_video_id=extract_video_id,
                fetch_youtube_metadata=fetch_youtube_metadata,
                sync_person_and_link=_sync_person_and_link,
                person_sync_sleep_sec=PERSON_SYNC_SLEEP_SEC,
                kst=KST,
                datetime_module=datetime,
                time_module=time,
                print_fn=print,
            )
        )


def _sync_person_and_link(
    notion: NotionClient,
    content_page_id: str,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    channel: str,
    date_str: str,
) -> Optional[str]:
    """인물 DB에 등록/업데이트 후 콘텐츠 DB 페이지와 relation 연결. person_page_id 반환."""
    canonical_name, alias_name = _resolve_canonical_person_identity(
        name,
        affiliation,
        role,
        channel=channel,
    )
    return service_sync_person_and_link(
        notion,
        content_page_id,
        canonical_name,
        affiliation,
        role,
        opinion,
        channel,
        date_str,
        alias_name,
        SyncPersonAndLinkDeps(
            sanitize_person_fields=_sanitize_person_fields,
            person_db_lock=_PERSON_DB_LOCK,
            find_person_in_notion_db=find_person_in_notion_db,
            update_existing_person_record=_update_existing_person_record,
            remember_person_match=_remember_person_match,
            forget_person_match=_forget_person_match,
            google_search_items=_google_search_items,
            pick_best_source_link=_pick_best_source_link,
            collect_person_info_from_search=collect_person_info_from_search,
            is_missing_person_value=_is_missing_person_value,
            notify_uncertain_person_with_ticket=_notify_uncertain_person_with_ticket,
            needs_manual_person_input=_needs_manual_person_input,
            warn_manual_person_input_needed=_warn_manual_person_input_needed,
            create_person_in_notion_db=create_person_in_notion_db,
            is_uncertain_person=_is_uncertain_person,
            is_person_review_approved=_is_person_review_approved,
            person_uncertain_action=PERSON_UNCERTAIN_ACTION,
            remove_person_from_expert_sheet=_remove_person_from_expert_sheet,
            update_person_page_body=update_person_page_body,
            backfill_person_profile=_backfill_person_profile,
            set_person_source_url_if_missing=_set_person_source_url_if_missing,
            resolve_content_person_relation_prop=_resolve_content_person_relation_prop,
            content_person_relation_prop=CONTENT_PERSON_RELATION_PROP,
            save_person_to_expert_sheet=save_person_to_expert_sheet,
            sync_person_identity_props=_sync_person_identity_props,
            person_identity_gate_reasons=_person_identity_gate_reasons,
            logger=logger,
            print_fn=print,
        ),
    )


# ── 텔레그램 전송용 텍스트 정제 ──────────────────────────
def sanitize_for_telegram(text: str) -> str:
    """ASCII 인코딩 오류를 유발하는 유니코드 문자를 치환"""
    return _clean(text)


def escape_html(text: str) -> str:
    """Telegram HTML 모드용 특수문자 이스케이프"""
    if not text:
        return text
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── 텔레그램 결과 메시지 포맷 ────────────────────────────
def format_telegram_result(result: dict, url: str) -> str:
    title    = escape_html(sanitize_for_telegram(result['title']))
    hashtags = escape_html(sanitize_for_telegram(result['hashtags']))
    summary  = escape_html(sanitize_for_telegram(result['summary']))
    person   = escape_html(sanitize_for_telegram(result['person']))
    opinion  = escape_html(sanitize_for_telegram(result['opinion']))
    return (
        f"✅ <b>분석 완료</b>\n\n"
        f"🎬 <b>{title}</b>\n\n"
        f"🏷️ {hashtags}\n\n"
        f"📝 <b>한줄 요약</b>\n{summary}\n\n"
        f"👤 <b>출연자</b>\n{person}\n\n"
        f"💡 <b>전문가 의견</b>\n{opinion}\n\n"
        f"🔗 <a href=\"{url}\">영상 링크</a>"
    )


def _is_admin_chat(chat_id: int) -> bool:
    if not TELEGRAM_ADMIN_CHAT_IDS:
        return True
    return str(chat_id) in TELEGRAM_ADMIN_CHAT_IDS


def _parse_fix_kv(tokens: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for tok in tokens:
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        k = _normalize_identity_text(k)
        if k in ("이름", "name"):
            out["name"] = v.strip()
        elif k in ("소속", "affiliation"):
            out["affiliation"] = v.strip()
        elif k in ("직책", "role"):
            out["role"] = v.strip()
        elif k in ("경력", "career"):
            out["career"] = v.strip()
        elif k in ("전문분야", "전문 분야", "expertise"):
            out["expertise"] = v.strip()
        elif k in ("근거링크", "근거 링크", "source", "url"):
            out["source_url"] = v.strip()
    return out


def _update_person_by_ticket(ticket: str, kv: dict[str, str]) -> tuple[bool, str]:
    item = _pending_telegram.get(ticket)
    if not item:
        return False, "티켓을 찾을 수 없습니다."
    page_id = item.get("page_id")
    if not page_id:
        return False, "티켓에 page_id가 없습니다."

    notion = get_notion_client()
    _ensure_person_db_extra_columns(notion)
    schema = _get_person_db_schema(notion)
    fields = _resolve_person_fields(schema)

    def rt(text: str) -> list:
        return [{"text": {"content": str(text)[:2000]}}]

    def ms(_: str) -> list:
        return []

    props: dict = {}
    for logical in ("name", "affiliation", "role", "career", "expertise", "source_url"):
        if logical not in kv:
            continue
        prop_name = fields.get(logical)
        if not prop_name:
            continue
        ptype = schema.get(prop_name, "rich_text")
        props[prop_name] = _build_person_prop_value(ptype, kv[logical], rt, ms)

    if not props:
        return False, "수정할 값이 없습니다. 예: 이름=홍춘욱 소속=... 직책=..."

    notion.pages.update(page_id=page_id, properties=props)
    saved = notion.pages.retrieve(page_id=page_id)
    name = _get_person_prop(saved, "이름")
    aff = _get_person_prop(saved, "소속")
    role = _get_person_prop(saved, "직책")
    career = _get_person_prop(saved, "주요 경력")
    expertise = _get_person_prop(saved, "전문 분야")
    count = _get_person_count(saved)
    source_url = _get_person_prop(saved, "근거 링크")
    save_person_to_expert_sheet(
        name, aff, role, career, expertise, count,
        notion=notion, person_page_id=page_id, source_url=source_url
    )
    _remember_person_match(name, aff, page_id)
    return True, f"수정 완료: {name} / {aff} / {role}"


def _merge_person_by_ticket(ticket: str, target_name: str) -> tuple[bool, str]:
    item = _pending_telegram.get(ticket)
    if not item:
        return False, "티켓을 찾을 수 없습니다."
    source_id = item.get("page_id")
    if not source_id:
        return False, "티켓에 page_id가 없습니다."

    notion = get_notion_client()
    source = notion.pages.retrieve(page_id=source_id)
    s_aff = _get_person_prop(source, "소속")
    s_role = _get_person_prop(source, "직책")
    target = find_person_in_notion_db(notion, target_name, s_aff, s_role)
    if not target:
        return False, f"병합 대상 인물을 찾지 못했습니다: {target_name}"

    target_id = target.get("id")
    if target_id == source_id:
        return False, "같은 페이지입니다."

    _merge_person_group(notion, [target, source])
    # 병합 결과는 등장횟수 큰 쪽이 primary가 될 수 있으므로 전체 재구성으로 정합 보장
    _rebuild_expert_sheet(notion)
    return True, f"병합 완료: '{_get_person_prop(source, '이름')}' -> '{_get_person_prop(target, '이름')}'"


def _approve_person_ticket_save(ticket: str) -> tuple[bool, str]:
    item = _pending_telegram.get(ticket)
    if not item:
        return False, "티켓을 찾을 수 없습니다."
    page_id = item.get("page_id", "")
    if not page_id:
        return False, "저장 대상 page_id가 없어 수동 입력이 필요합니다. /person_form <티켓>을 사용하세요."

    notion = get_notion_client()
    page = notion.pages.retrieve(page_id=page_id)
    name = _get_person_prop(page, "이름")
    aff = _get_person_prop(page, "소속")
    role = _get_person_prop(page, "직책")
    career = _get_person_prop(page, "주요 경력")
    expertise = _get_person_prop(page, "전문 분야")
    count = _get_person_count(page)
    source_url = _get_person_prop(page, "근거 링크")

    save_person_to_expert_sheet(
        name, aff, role, career, expertise, count,
        notion=notion, person_page_id=page_id, source_url=source_url
    )
    _mark_person_review(name, aff, role, "approved", "inline_yes_save")
    _pending_telegram.pop(ticket, None)
    return True, f"저장 완료: {name} / {aff} / {role}"


def _strip_korean_tail_particles(text: str) -> str:
    """'홍춘욱이라고/홍춘욱은' 같은 조사/어미를 제거."""
    t = (text or "").strip()
    tails = [
        "이라고요", "라고요", "입니다", "이었습니다", "였습니다",
        "이라고", "라고", "님", "씨", "은", "는", "이", "가", "을", "를", "와", "과",
    ]
    for tail in tails:
        if t.endswith(tail) and len(t) > len(tail) + 1:
            t = t[: -len(tail)].strip()
            break
    return t


def _guess_person_name_from_keywords(base_name: str, keywords: str) -> str:
    """대체 정보 문장에서 이름 후보 추출. 없으면 base_name 유지."""
    text = (keywords or "").strip()
    if not text:
        return base_name

    # 공백 토큰 우선 검사
    tokens = [tok for tok in re.split(r"\s+", text) if tok]
    candidates: list[str] = []
    for tok in tokens:
        tok2 = _strip_korean_tail_particles(tok)
        if re.fullmatch(r"[가-힣]{2,6}", tok2 or ""):
            candidates.append(tok2)

    # 문장 전체에서도 한글 이름 패턴 추출
    if not candidates:
        for m in re.findall(r"[가-힣]{2,6}", text):
            m2 = _strip_korean_tail_particles(m)
            if re.fullmatch(r"[가-힣]{2,6}", m2 or ""):
                candidates.append(m2)

    # 일반 키워드 제거
    blacklist = {
        "경제", "금융", "투자", "증권", "전문가", "애널리스트", "이코노미스트",
        "소속", "직책", "기자", "박사", "채널", "유튜브", "인터뷰",
    }
    candidates = [c for c in candidates if c not in blacklist]
    if not candidates:
        return base_name

    # base_name의 확장형(예: 홍 -> 홍춘욱) 우선
    for c in candidates:
        if base_name and c.startswith(base_name) and len(c) > len(base_name):
            return c

    # 가장 그럴듯한(길이 긴) 후보 선택
    return sorted(candidates, key=len, reverse=True)[0]


def _research_and_fill_person_by_ticket(ticket: str, keywords: str) -> tuple[bool, str]:
    """티켓 대상 인물을 키워드로 재검색/재검증 후 DB 반영."""
    item = _pending_telegram.get(ticket)
    if not item:
        return False, "티켓을 찾을 수 없습니다."

    base_name = (item.get("name") or "").strip() or "미상"
    base_aff = (item.get("affiliation") or "").strip() or "정보 없음"
    base_role = (item.get("role") or "").strip() or "미상"
    query = (keywords or "").strip() or f"{base_name} {base_aff} {base_role}"
    query_name = _guess_person_name_from_keywords(base_name, query)

    # 1차 검색
    info1 = collect_person_info_from_search(query_name, query, base_role, channel="")
    # 2차 재검색(1차 결과 기반 재검증)
    q2_aff = info1.get("affiliation") or base_aff
    q2_role = info1.get("role") or base_role
    info2 = collect_person_info_from_search(query_name, q2_aff, q2_role, channel="")

    new_name, new_aff, new_role = _sanitize_person_fields(
        query_name,
        info2.get("affiliation") or info1.get("affiliation") or base_aff,
        info2.get("role") or info1.get("role") or base_role,
    )
    new_career = info2.get("career") or info1.get("career") or "정보 없음"
    new_expertise = info2.get("expertise") or info1.get("expertise") or "정보 없음"
    new_source = info2.get("source_url") or info1.get("source_url") or ""

    kv = {
        "name": new_name,
        "affiliation": new_aff,
        "role": new_role,
        "career": new_career,
        "expertise": new_expertise,
    }
    if new_source:
        kv["source_url"] = new_source

    page_id = (item.get("page_id") or "").strip()
    if page_id:
        ok, msg = _update_person_by_ticket(ticket, kv)
        if ok:
            _mark_person_review(new_name, new_aff, new_role, "corrected", f"keywords:{query}")
            _pending_telegram.pop(ticket, None)
            return True, f"{msg}\n- 검색 키워드: {query}\n- 근거 링크: {new_source or '없음'}"
        return False, msg

    # page_id가 없던 티켓은 새 인물 페이지 생성 후 저장
    notion = get_notion_client()
    new_page_id = create_person_in_notion_db(
        notion, new_name, new_aff, new_role, new_career, new_expertise, source_url=new_source
    )
    if not new_page_id:
        return False, "재검색은 완료했지만 Notion 인물 페이지 생성에 실패했습니다."

    save_person_to_expert_sheet(
        new_name, new_aff, new_role, new_career, new_expertise, 1,
        notion=notion, person_page_id=new_page_id, source_url=new_source
    )
    _remember_person_match(new_name, new_aff, new_page_id)
    _mark_person_review(new_name, new_aff, new_role, "corrected", f"keywords:{query}")
    _pending_telegram.pop(ticket, None)
    return True, (
        f"재검색 후 신규 저장 완료: {new_name} / {new_aff} / {new_role}\n"
        f"- 검색 키워드: {query}\n"
        f"- 근거 링크: {new_source or '없음'}"
    )


async def person_decision_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    if not data.startswith("person_decide:"):
        return

    parts = data.split(":", 2)
    if len(parts) != 3:
        await query.answer("잘못된 요청입니다.", show_alert=True)
        return
    _, action, ticket = parts

    chat_id = query.message.chat_id if query.message else 0
    if not _is_admin_chat(chat_id):
        await query.answer("권한이 없습니다.", show_alert=True)
        return

    if action == "yes":
        loop = asyncio.get_event_loop()
        ok, text = await loop.run_in_executor(None, _approve_person_ticket_save, ticket)
        await query.answer("처리 완료" if ok else "처리 실패", show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        if query.message:
            await query.message.reply_text(("✅ " if ok else "❌ ") + text)
        return

    if action == "no":
        await query.answer("보류 처리", show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        user_id = query.from_user.id if query.from_user else 0
        skey = _person_form_session_key(chat_id, user_id)
        _ticket_review_sessions[skey] = {"ticket": ticket, "stage": "keywords"}
        if query.message:
            await query.message.reply_text(
                "📝 보류 처리되었습니다.\n"
                "대체 정보를 입력해 주세요. (검색 키워드로 사용)\n"
                "예: 홍춘욱 경제강의노트 전 하나금융투자 이코노미스트"
            )
        return

    await query.answer("알 수 없는 동작입니다.", show_alert=True)


async def unknown_decision_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """미상 인물 확인(예/아니오) 인라인 버튼 처리."""
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    if not data.startswith("unknown_decide:"):
        return

    action = data.split(":", 1)[1] if ":" in data else ""
    chat_id = query.message.chat_id if query.message else 0
    user_id = query.from_user.id if query.from_user else 0
    skey = _person_form_session_key(chat_id, user_id)
    up = _unknown_person_sessions.get(skey)
    if not up or up.get("stage") != "confirm":
        await query.answer("처리 가능한 대기 건이 없습니다.", show_alert=True)
        return

    if action == "yes":
        await query.answer("미상 저장 처리", show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        if up.get("testbed"):
            _unknown_person_sessions.pop(skey, None)
            if query.message:
                await query.message.reply_text("🧪 테스트베드 결과: '미상'으로 저장하는 흐름이 실행됩니다. (실저장 없음)")
            return
        try:
            result = _finalize_page_with_analysis(
                up["notion"], up["page_id"], up["props"], up["url"], up["metadata"], up["analysis"]
            )
            _unknown_person_sessions.pop(skey, None)
            if query.message:
                if result:
                    await query.message.reply_text(format_telegram_result(result, up["url"]), parse_mode="HTML")
                else:
                    await query.message.reply_text("⚠️ 분석에 실패했습니다. 노션에서 직접 확인해주세요.")
        except Exception as e:
            _unknown_person_sessions.pop(skey, None)
            if query.message:
                await query.message.reply_text(f"❌ 오류가 발생했습니다: {e}")
        return

    if action == "no":
        await query.answer("이름 입력으로 전환", show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        up["stage"] = "name"
        _unknown_person_sessions[skey] = up
        if query.message:
            await query.message.reply_text("이름을 입력해 주세요.")
        return

    await query.answer("알 수 없는 동작입니다.", show_alert=True)


async def person_fix_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if not _is_admin_chat(msg.chat_id):
        await msg.reply_text("권한이 없습니다.")
        return
    if not context.args or len(context.args) < 2:
        await msg.reply_text("사용법: /person_fix <티켓> 이름=... 소속=... 직책=...")
        return
    ticket = context.args[0].strip()
    kv = _parse_fix_kv(context.args[1:])
    loop = asyncio.get_event_loop()
    ok, text = await loop.run_in_executor(None, _update_person_by_ticket, ticket, kv)
    await msg.reply_text(("✅ " if ok else "❌ ") + text)


async def person_merge_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if not _is_admin_chat(msg.chat_id):
        await msg.reply_text("권한이 없습니다.")
        return
    if not context.args or len(context.args) < 2:
        await msg.reply_text("사용법: /person_merge <티켓> <기존인물명>")
        return
    ticket = context.args[0].strip()
    target_name = " ".join(context.args[1:]).strip()
    loop = asyncio.get_event_loop()
    ok, text = await loop.run_in_executor(None, _merge_person_by_ticket, ticket, target_name)
    await msg.reply_text(("✅ " if ok else "❌ ") + text)


async def person_skip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if not _is_admin_chat(msg.chat_id):
        await msg.reply_text("권한이 없습니다.")
        return
    if not context.args:
        await msg.reply_text("사용법: /person_skip <티켓>")
        return
    ticket = context.args[0].strip()
    if ticket in _pending_telegram:
        item = _pending_telegram.get(ticket, {})
        _mark_person_review(
            item.get("name", ""),
            item.get("affiliation", ""),
            item.get("role", ""),
            "skipped",
            "manual_skip",
        )
        _pending_telegram.pop(ticket, None)
        await msg.reply_text(f"✅ 보류 처리 완료: {ticket}")
    else:
        await msg.reply_text("❌ 티켓을 찾을 수 없습니다.")


def _person_form_session_key(chat_id: int, user_id: int) -> str:
    return f"{chat_id}:{user_id}"


def _person_form_prompt(session: dict) -> str:
    idx = session.get("step", 0)
    field_key, field_label = _PERSON_FORM_FIELDS[idx]
    return (
        f"📝 인물 수정 폼 ({idx+1}/{len(_PERSON_FORM_FIELDS)})\n"
        f"- 티켓: {session['ticket']}\n"
        f"- 항목: {field_label}\n\n"
        "값을 입력하세요.\n"
        "- 건너뛰기: `-`\n"
        "- 취소: `/person_skip <티켓>`"
    )


async def person_form_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if not _is_admin_chat(msg.chat_id):
        await msg.reply_text("권한이 없습니다.")
        return
    if not context.args:
        await msg.reply_text("사용법: /person_form <티켓>")
        return
    ticket = context.args[0].strip()
    item = _pending_telegram.get(ticket)
    if not item:
        await msg.reply_text("❌ 티켓을 찾을 수 없습니다.")
        return

    user_id = msg.from_user.id if msg.from_user else 0
    skey = _person_form_session_key(msg.chat_id, user_id)
    _person_form_sessions[skey] = {
        "ticket": ticket,
        "step": 0,
        "data": {},
    }
    await msg.reply_text(
        "✅ 수정 폼을 시작합니다.\n"
        f"- 현재 이름: {item.get('name','')}\n"
        f"- 현재 소속: {item.get('affiliation','')}\n"
        f"- 현재 직책: {item.get('role','')}\n\n"
        + _person_form_prompt(_person_form_sessions[skey]),
        parse_mode="Markdown",
    )


async def person_testbed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """실데이터 반영 없이 '미상 인물 보강' 대화 흐름 테스트."""
    msg = update.message
    if not msg:
        return
    if not _is_admin_chat(msg.chat_id):
        await msg.reply_text("권한이 없습니다.")
        return
    user_id = msg.from_user.id if msg.from_user else 0
    ukey = _person_form_session_key(msg.chat_id, user_id)
    _unknown_person_sessions[ukey] = {
        "stage": "confirm",
        "testbed": True,
        "url": "https://example.com/testbed",
        "metadata": {"title": "테스트베드 영상", "channel": "테스트채널"},
        "analysis": {
            "person": {"name": "미상", "affiliation": "정보 없음", "role": "미상", "background": "정보 없음"},
            "summary": "테스트베드 요약",
            "opinion": "테스트베드 의견",
            "hashtags": ["#테스트"],
            "mentioned_products": [],
            "sectors": [],
            "economic_outlook": "중립",
        },
    }
    await msg.reply_text(
        "🧪 테스트베드 시작 (실데이터 반영 없음)\n"
        "인물이 검색되지 않았습니다. 미상으로 남길까요?\n"
        "아래 버튼에서 선택해 주세요.",
        reply_markup=_unknown_person_reply_markup(),
    )


# ── 텔레그램 핸들러 ──────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    payload = (context.args[0] if context and context.args else "").strip().lower()
    uid = str(msg.from_user.id) if msg.from_user else ""
    uname = msg.from_user.username if msg.from_user else ""
    if payload:
        loop = asyncio.get_event_loop()
        payload_result = await loop.run_in_executor(
            None,
            lambda: handle_start_payload(payload, uid, uname or "", save_daily_feedback),
        )
        if payload_result and payload_result.handled:
            await msg.reply_text(payload_result.reply_text)
            return
    await msg.reply_text(
        "👋 경제 콘텐츠 분석 봇입니다.\n"
        "유튜브 링크를 보내주시면 자동으로 분석하고 노션 DB에 저장합니다."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"메시지 수신: update_id={update.update_id}, chat_id={update.effective_chat.id}, text={repr(update.message.text if update.message else None)}")
    message = update.message
    if not message:
        logger.warning("update.message가 None — 무시")
        return
    text = message.text or ""
    chat_id = message.chat_id
    user_id = message.from_user.id if message.from_user else 0

    # 인물 수정 폼 진행 중이면 URL 처리보다 우선
    skey = _person_form_session_key(chat_id, user_id)
    session = _person_form_sessions.get(skey)
    if session:
        ticket = session.get("ticket", "")
        step = session.get("step", 0)
        data = session.get("data", {})
        if 0 <= step < len(_PERSON_FORM_FIELDS):
            field_key, _ = _PERSON_FORM_FIELDS[step]
            value = text.strip()
            if value and value != "-":
                data[field_key] = value
            step += 1
            session["step"] = step
            session["data"] = data
            _person_form_sessions[skey] = session

            if step >= len(_PERSON_FORM_FIELDS):
                _person_form_sessions.pop(skey, None)
                if session.get("testbed"):
                    await message.reply_text(
                        "🧪 테스트베드 완료\n"
                        f"- 입력값: {json.dumps(data, ensure_ascii=False)}\n"
                        "실데이터는 변경되지 않았습니다."
                    )
                    return
                loop = asyncio.get_event_loop()
                ok, result_text = await loop.run_in_executor(None, _update_person_by_ticket, ticket, data)
                await message.reply_text(("✅ " if ok else "❌ ") + result_text)
                return

            await message.reply_text(_person_form_prompt(session), parse_mode="Markdown")
            return

    # 의심 인물 티켓 보류 후 키워드 입력 세션
    t_session = _ticket_review_sessions.get(skey)
    if t_session:
        ticket = t_session.get("ticket", "")
        keywords = text.strip()
        if not _looks_like_keyword_input(keywords):
            _ticket_review_sessions.pop(skey, None)
            await message.reply_text(
                "입력 내용이 검색 키워드로 보이지 않습니다. 버튼 선택 단계로 돌아갑니다.",
                reply_markup=_person_review_reply_markup(ticket),
            )
            return
        _ticket_review_sessions.pop(skey, None)
        loop = asyncio.get_event_loop()
        ok, result_text = await loop.run_in_executor(None, _research_and_fill_person_by_ticket, ticket, keywords)
        await message.reply_text(("✅ " if ok else "❌ ") + result_text)
        return

    # 미상 인물 보정 세션 진행 중
    ukey = _person_form_session_key(chat_id, user_id)
    up = _unknown_person_sessions.get(ukey)
    if up:
        stage = up.get("stage", "confirm")
        if stage == "confirm":
            # confirm 단계는 인라인 버튼 전용.
            # URL이 아닌 텍스트 입력 시 버튼 리마인더를 보내고 종료.
            if not extract_youtube_url(text):
                await message.reply_text(
                    "아래 버튼에서 선택해 주세요.",
                    reply_markup=_unknown_person_reply_markup(),
                )
                return
            # URL이면 새 영상 처리로 넘김 (기존 session 덮어씀)

        if stage == "name":
            if not _looks_like_person_name_input(text):
                up["stage"] = "confirm"
                _unknown_person_sessions[ukey] = up
                await message.reply_text(
                    "입력 내용이 이름으로 확인되지 않아 버튼 선택 단계로 돌아갑니다.",
                    reply_markup=_unknown_person_reply_markup(),
                )
                return
            up["manual_name"] = text.strip()
            up["stage"] = "context"
            _unknown_person_sessions[ukey] = up
            await message.reply_text("관련 정보를 알려주세요. (예: 소속/직책/채널/키워드)")
            return

        if stage == "context":
            manual_name = (up.get("manual_name") or "").strip()
            context_text = text.strip()
            if not _looks_like_keyword_input(context_text):
                up["stage"] = "confirm"
                _unknown_person_sessions[ukey] = up
                await message.reply_text(
                    "입력 내용이 관련 정보로 확인되지 않아 버튼 선택 단계로 돌아갑니다.",
                    reply_markup=_unknown_person_reply_markup(),
                )
                return
            metadata = up["metadata"]
            analysis = up["analysis"]
            p = analysis.get("person", {}) if isinstance(analysis.get("person"), dict) else {}

            info = collect_person_info_from_search(
                manual_name or p.get("name", "미상"),
                context_text or p.get("affiliation", "정보 없음"),
                p.get("role", "미상"),
                channel=metadata.get("channel", ""),
            )
            new_name = manual_name or p.get("name", "미상")
            new_aff = info.get("affiliation") or context_text or p.get("affiliation", "정보 없음")
            new_role = info.get("role") or p.get("role", "미상")
            new_name, new_aff, new_role = _sanitize_person_fields(new_name, new_aff, new_role)

            analysis["person"] = {
                "name": new_name,
                "affiliation": new_aff,
                "role": new_role,
                "background": info.get("career", p.get("background", "정보 없음")),
            }

            if up.get("testbed"):
                _unknown_person_sessions.pop(ukey, None)
                await message.reply_text(
                    "🧪 테스트베드 결과 (검색 보강 후 저장 전 미리보기)\n"
                    f"- 이름: {new_name}\n"
                    f"- 소속: {new_aff}\n"
                    f"- 직책: {new_role}\n"
                    f"- 근거링크: {info.get('source_url','') or '없음'}\n"
                    "실데이터는 변경되지 않았습니다."
                )
                return

            try:
                result = _finalize_page_with_analysis(
                    up["notion"], up["page_id"], up["props"], up["url"], metadata, analysis
                )
                _unknown_person_sessions.pop(ukey, None)
                if result:
                    await message.reply_text(format_telegram_result(result, up["url"]), parse_mode="HTML")
                else:
                    await message.reply_text("⚠️ 분석에 실패했습니다. 노션에서 직접 확인해주세요.")
            except Exception as e:
                _unknown_person_sessions.pop(ukey, None)
                await message.reply_text(f"❌ 오류가 발생했습니다: {e}")
            return

    url = extract_youtube_url(text)
    if not url:
        await message.reply_text("유튜브 링크를 보내주세요.\n예: https://youtu.be/xxxxx")
        return

    notion = get_notion_client()

    video_id = extract_video_id(url)
    page_id = None
    if video_id:
        existing = find_duplicate_pages(notion, video_id)
        if existing:
            completed = [p for p in existing if not _is_incomplete(p)]
            incomplete = [p for p in existing if _is_incomplete(p)]

            if completed:
                await message.reply_text("⚠️ 이미 분석 완료된 영상입니다.")
                return

            if incomplete:
                # 이전 처리 중 오류로 미완료 상태 — 기존 페이지를 재처리
                page_id = incomplete[0]["id"]
                await message.reply_text("🔄 이전에 오류가 발생한 영상입니다. 재처리를 시작합니다...")

    try:
        if page_id is None:
            # 신규 등록
            await message.reply_text("🔍 링크 수신\n노션 DB에 저장 중...")
            page_id = create_notion_page_from_url(notion, url)
            await message.reply_text("📋 노션 저장 완료. 분석 시작...")

        # 폴러가 같은 페이지를 중복 처리하지 않도록 즉시 '처리 중...' 표시
        notion.pages.update(
            page_id=page_id,
            properties={"주제": {"rich_text": [{"text": {"content": "처리 중..."}}]}}
        )

        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {})
        job = VideoJob(
            url=url,
            source="telegram",
            page_id=page_id,
            chat_id=chat_id,
            allow_interactive_review=True,
        )
        page_ctx = PageContext(page_id=page_id, props=props)
        deps = _build_content_pipeline_deps(
            notion,
            unknown_person_handler=_build_unknown_person_pipeline_handler(notion, ukey),
        )
        loop = asyncio.get_event_loop()
        pipeline_result = await loop.run_in_executor(
            None,
            lambda: run_content_pipeline(job, page_ctx, deps),
        )

        if pipeline_result.status == "needs_review":
            await message.reply_text(
                pipeline_result.message + "\n아래 버튼에서 선택해 주세요.",
                reply_markup=_unknown_person_reply_markup(),
            )
            return
        if pipeline_result.status == "done" and pipeline_result.payload:
            _dequeue_failed_url(url)
            await message.reply_text(
                format_telegram_result(pipeline_result.payload, url),
                parse_mode="HTML",
            )
            return
        if pipeline_result.message == "invalid_url":
            await message.reply_text("⚠️ 유효하지 않은 유튜브 URL")
            return
        if pipeline_result.message == "metadata_missing":
            await message.reply_text("⚠️ 영상 메타데이터를 가져오지 못했습니다.")
            return

        _enqueue_failed_url(url, pipeline_result.message or "pipeline_failed")
        await message.reply_text("⚠️ 분석에 실패했습니다. 노션에서 직접 확인해주세요.")

    except Exception as e:
        print(f"  ❌ 텔레그램 처리 오류: {e}")
        if url:
            _enqueue_failed_url(url, f"telegram_handle_error:{e}")
        # 오류 시 주제를 초기화해 다음 재전송 때 재처리 가능하도록
        if page_id:
            try:
                notion.pages.update(page_id=page_id, properties={"주제": {"rich_text": []}})
            except Exception:
                pass
        await message.reply_text(f"❌ 오류가 발생했습니다: {e}")


# ── 전체 페이지 조회 (재처리용) ──────────────────────────
def get_all_pages_with_url(notion: NotionClient) -> list[dict]:
    """URL이 있는 노션 DB 페이지 전체 반환 (처리 여부 무관, 페이지네이션 처리)"""
    pages = []
    cursor = None
    while True:
        body: dict = {
            "filter": {"property": "URL", "url": {"is_not_empty": True}},
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor
        resp = httpx.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers={
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


# ── URL 중복 체크 ────────────────────────────────────────
def find_duplicate_pages(notion: NotionClient, video_id: str) -> list[dict]:
    """동일 video_id를 가진 노션 DB 페이지 목록 반환"""
    all_pages = get_all_pages_with_url(notion)
    return [
        p for p in all_pages
        if extract_video_id(
            (p.get("properties", {}).get("URL", {}).get("url") or "")
        ) == video_id
    ]


def _get_page_topic(page: dict) -> str:
    """노션 페이지의 주제 컬럼 텍스트 반환"""
    rich_texts = page.get("properties", {}).get("주제", {}).get("rich_text", [])
    return rich_texts[0].get("text", {}).get("content", "") if rich_texts else ""


def _is_incomplete(page: dict) -> bool:
    """주제가 비어있거나 '처리 중...'인 미완료 페이지 여부"""
    topic = _get_page_topic(page)
    return not topic or topic == "처리 중..."


def dedup_all(notion: NotionClient):
    """노션 DB에서 같은 video_id를 가진 중복 페이지 삭제 (오래된 것 1개 유지)"""
    all_pages = get_all_pages_with_url(notion)

    by_video_id: dict[str, list[dict]] = {}
    for page in all_pages:
        url = page.get("properties", {}).get("URL", {}).get("url") or ""
        vid = extract_video_id(url)
        if vid:
            by_video_id.setdefault(vid, []).append(page)

    deleted = 0
    for vid, pages in by_video_id.items():
        if len(pages) <= 1:
            continue
        pages_sorted = sorted(pages, key=lambda p: p.get("created_time", ""))
        for page in pages_sorted[1:]:
            page_id = page["id"]
            url = page.get("properties", {}).get("URL", {}).get("url") or ""
            notion.pages.update(page_id=page_id, archived=True)
            print(f"  🗑️  삭제: {page_id[:8]}… | {url}")
            deleted += 1

    print(f"\n✅ 중복 제거 완료 — {deleted}개 삭제")


# ── 페이지 본문 블록 전체 삭제 ────────────────────────────
def clear_page_blocks(notion: NotionClient, page_id: str):
    """페이지의 기존 본문 블록을 모두 삭제 (재처리 전 초기화)"""
    response = notion.blocks.children.list(block_id=page_id)
    for block in response.get("results", []):
        try:
            notion.blocks.delete(block_id=block["id"])
        except Exception as e:
            print(f"    블록 삭제 실패 ({block['id'][:8]}…): {e}")


# ── 강제 재처리 ───────────────────────────────────────────
def reprocess_all():
    """노션 DB의 모든 URL 항목을 처음부터 재분석"""
    notion = get_notion_client()
    pages = get_all_pages_with_url(notion)
    total = len(pages)

    if total == 0:
        print("재처리할 페이지가 없습니다.")
        return

    print(f"\n📋 재처리 대상: {total}개 페이지\n")

    success = 0
    fail = 0
    for i, page in enumerate(pages, 1):
        props = page.get("properties", {})
        title_items = (props.get("콘텐츠 제목", {}) or {}).get("title") or []
        existing_title = title_items[0]["text"]["content"] if title_items else "(제목 없음)"
        print(f"[{i}/{total}] {existing_title[:50]}")

        # 주제 필드 초기화 (process_page가 조건 없이 실행되므로 직접 호출)
        try:
            notion.pages.update(
                page_id=page["id"],
                properties={"주제": {"rich_text": [{"text": {"content": ""}}]}},
            )
        except Exception as e:
            print(f"  ⚠️ 주제 초기화 실패: {e}")

        # 기존 본문 블록 삭제
        print("  → 기존 블록 삭제 중...")
        clear_page_blocks(notion, page["id"])

        # 재분석 실행
        try:
            result = process_page(notion, page)
            if result:
                success += 1
            else:
                fail += 1
        except Exception as e:
            print(f"  ❌ 페이지 처리 오류 (건너뜀): {e}")
            fail += 1

        if i < total:
            time.sleep(max(PERSON_SYNC_SLEEP_SEC, 0.2))  # API 레이트 리밋 방지

    print(f"\n{'='*50}")
    print(f"✅ 재처리 완료 — 성공: {success}개 / 실패: {fail}개")
    print(f"{'='*50}")

    _validate_and_cleanse_sheets()


# ── 텔레그램 채널 메시지 발송 ────────────────────────────
def _strip_html_tags(text: str) -> str:
    """Telegram HTML 파싱 실패 시 fallback용 plain text 변환."""
    if not text:
        return text
    # 링크는 텍스트만 남김
    text = re.sub(r"<a\s+[^>]*>(.*?)</a>", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
    # 나머지 태그 제거
    text = re.sub(r"</?[^>]+>", "", text)
    # html entity 최소 복원
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    return text

def _split_html_message(text: str, max_len: int = 4000) -> list[str]:
    """HTML 텍스트를 단락(\n\n) 기준으로 분할. 단락이 너무 길면 줄 단위로 분할."""
    chunks: list[str] = []
    current = ""
    for para in text.split("\n\n"):
        segment = para + "\n\n"
        if len(current) + len(segment) > max_len:
            if current:
                chunks.append(current.rstrip())
            if len(segment) > max_len:
                line_buf = ""
                for line in segment.split("\n"):
                    line_seg = line + "\n"
                    if len(line_buf) + len(line_seg) > max_len:
                        if line_buf:
                            chunks.append(line_buf.rstrip())
                        line_buf = line_seg
                    else:
                        line_buf += line_seg
                current = line_buf
            else:
                current = segment
        else:
            current += segment
    if current.strip():
        chunks.append(current.rstrip())
    return chunks or [text[:max_len]]


def _send_telegram_message_to_chat(chat_id: str, text: str, reply_markup: Optional[dict] = None) -> bool:
    """지정 chat_id로 텔레그램 메시지 발송.
    1차: HTML 모드
    2차: HTML 400 실패 시 plain text fallback
    """
    return adapter_send_message_to_chat(
        TELEGRAM_BOT_TOKEN or "",
        chat_id,
        text,
        reply_markup=reply_markup,
        logger=logger,
    )


def send_telegram_channel_message(text: str, reply_markup: Optional[dict] = None) -> bool:
    """텔레그램 데일리 채널 발송."""
    if not TELEGRAM_CHANNEL_ID:
        logger.warning("TELEGRAM_CHANNEL_ID 미설정 — 채널 발송 스킵")
        return False
    return _send_telegram_message_to_chat(TELEGRAM_CHANNEL_ID, text, reply_markup=reply_markup)


def send_telegram_review_message(text: str, reply_markup: Optional[dict] = None) -> bool:
    """인물 수동 수정요청 전용 발송 (리뷰/운영 채팅)."""
    if not REVIEW_ALERTS_ENABLED:
        logger.info("REVIEW_ALERTS_ENABLED=0 — 수동 수정요청 알림 발송 스킵")
        return False
    review_chat = TELEGRAM_REVIEW_CHAT_ID or (next(iter(TELEGRAM_ADMIN_CHAT_IDS)) if TELEGRAM_ADMIN_CHAT_IDS else "")
    if not review_chat:
        logger.warning("TELEGRAM_REVIEW_CHAT_ID/TELEGRAM_ADMIN_CHAT_IDS 미설정 — 리뷰 메시지 발송 스킵")
        return False
    return _send_telegram_message_to_chat(review_chat, text, reply_markup=reply_markup)



# ── 구글 시트 전체 데이터 조회 ───────────────────────────
def read_sheet_rows() -> list[dict]:
    """구글 시트 전체 데이터를 헤더 기준 dict 리스트로 반환"""
    try:
        service = _get_sheets_service()
        return adapter_read_rows(
            service=service,
            spreadsheet_id=GOOGLE_SHEET_ID,
            data_range="A:K",
            logger=logger,
        )
    except Exception as e:
        logger.error(f"시트 데이터 조회 실패: {e}")
        return []


# ── 경제 점검지표 시트 조회 ──────────────────────────────
def read_indicator_sheet() -> dict:
    """경제 점검지표 시트를 읽어 {master_signal, indicators} 반환.
    헤더 행(3행)을 기준으로 컬럼을 동적으로 매핑하므로 열 순서 변경에 강인.
    지표명/수치에 #REF! 오류가 있는 행은 자동으로 제외."""
    try:
        service = _get_sheets_service()
        return adapter_read_indicator_rows(
            service=service,
            spreadsheet_id=INDICATOR_SHEET_ID,
            data_range="점검 지표!A1:K12",
            logger=logger,
        )
    except Exception as e:
        logger.error(f"경제 지표 시트 조회 실패: {e}")
        return {}


def _calc_diff(current: str, prev: str) -> str:
    """현재 수치와 전일 수치의 차이를 '전일 X 대비 +Y ▲' 형식으로 반환. 계산 불가 시 빈 문자열."""
    try:
        # 숫자만 추출 (쉼표·%, 원 등 단위 제거)
        def _parse(s: str) -> float:
            return float(re.sub(r"[^\d.\-]", "", s.replace(",", "")))
        c = _parse(current)
        p = _parse(prev)
        diff = round(c - p, 4)
        arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "―")
        sign  = "+" if diff > 0 else ""
        # 정수처럼 딱 떨어지면 소수점 생략
        diff_str = str(int(diff)) if diff == int(diff) else str(diff)
        return f"전일 {prev} 대비 {sign}{diff_str} {arrow}"
    except Exception:
        return ""


def _indicators_to_text(indicator_data: dict) -> str:
    """경제 지표 데이터를 Gemini 프롬프트용 텍스트로 변환"""
    if not indicator_data:
        return "(경제 지표 데이터 없음)"

    lines = []
    master = indicator_data.get("master_signal", "")
    if master:
        lines.append(f"[마스터 신호등] {master}\n")

    for ind in indicator_data.get("indicators", []):
        name   = ind.get("지표명", "")
        value  = ind.get("현재 수치", "")
        prev   = ind.get("전일 수치", "")
        signal = ind.get("신호등", "")
        note   = ind.get("비고", "")

        line = f"- {name}: {value}"
        if prev:
            diff_str = _calc_diff(value, prev)
            if diff_str:
                line += f" ({diff_str})"
        if signal:
            line += f" {signal}"
        if note:
            line += f"\n  └ {note[:120]}"
        lines.append(line)

    return "\n".join(lines)


def _daily_briefing_reply_markup() -> Optional[dict]:
    """데일리 브리핑 하단 인라인 버튼. 채널에서도 표시 가능."""
    if not TELEGRAM_BOT_USERNAME:
        return None
    uname = TELEGRAM_BOT_USERNAME.lstrip("@")
    return {
        "inline_keyboard": [
            [
                {"text": "👍 좋아요", "url": f"https://t.me/{uname}?start=daily_feedback_good"},
                {"text": "👎 아쉬워요", "url": f"https://t.me/{uname}?start=daily_feedback_bad"},
            ],
            [
                {"text": "⚙️ 설정", "url": f"https://t.me/{uname}?start=settings"},
            ],
        ]
    }


def _parse_row_datetime(row: dict) -> Optional[datetime]:
    """처리일시 필드를 KST datetime으로 파싱 (서버 로컬 타임 = KST 가정)"""
    raw = row.get("처리일시", "")
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=KST)
        except ValueError:
            continue
    return None


def filter_rows_by_date(
    rows: list[dict], start_kst: datetime, end_kst: datetime
) -> list[dict]:
    """KST 기준 start_kst ~ end_kst 범위의 행만 반환"""
    result = []
    for row in rows:
        dt = _parse_row_datetime(row)
        if dt and start_kst <= dt <= end_kst:
            result.append(row)
    return result


# ── Gemini 기반 리포트 생성 ──────────────────────────────
def _rows_to_text(rows: list[dict]) -> str:
    """시트 행을 Gemini 프롬프트용 텍스트로 변환"""
    parts = []
    for i, r in enumerate(rows, 1):
        parts.append(
            f"[영상 {i}]\n"
            f"제목: {r.get('영상 제목', '')}\n"
            f"출연자: {r.get('출연자(소속/직책/이름)', '')}\n"
            f"한줄 요약: {r.get('한줄 요약', '')}\n"
            f"인물 의견: {r.get('인물 의견', '')}\n"
            f"주요 섹터: {r.get('주요 섹터', '')}\n"
            f"언급 상품: {r.get('언급 상품', '')}\n"
            f"경기 전망: {r.get('경기 전망', '')}"
        )
    return "\n\n".join(parts)


_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]


def generate_daily_briefing(rows: list[dict], indicator_data: dict, highlights_text: str = "") -> str:
    return service_generate_daily_briefing(
        rows,
        indicator_data,
        highlights_text,
        ReportGenerationDeps(
            kst=KST,
            api_key=GEMINI_API_KEY,
            genai_module=genai,
            logger=logger,
            rows_to_text=_rows_to_text,
            indicators_to_text=_indicators_to_text,
        ),
    )


def generate_weekly_report(rows: list[dict], indicator_data: dict) -> str:
    return service_generate_weekly_report(
        rows,
        indicator_data,
        ReportGenerationDeps(
            kst=KST,
            api_key=GEMINI_API_KEY,
            genai_module=genai,
            logger=logger,
            rows_to_text=_rows_to_text,
            indicators_to_text=_indicators_to_text,
        ),
    )


# ── 데일리 브리핑 발송 ────────────────────────────────────
async def send_daily_briefing():
    await service_send_daily_briefing(
        DailySendDeps(
            logger=logger,
            kst=KST,
            check_runtime_keys=check_runtime_keys,
            read_sheet_rows=read_sheet_rows,
            read_indicator_sheet=read_indicator_sheet,
            filter_rows_by_date=filter_rows_by_date,
            build_daily_highlights=_build_daily_highlights,
            generate_daily_briefing=generate_daily_briefing,
            daily_briefing_reply_markup=_daily_briefing_reply_markup,
            send_telegram_channel_message=send_telegram_channel_message,
            save_daily_briefing_log=save_daily_briefing_log,
            save_daily_highlight_log=save_daily_highlight_log,
        )
    )


# ── 주간 리포트 발송 ──────────────────────────────────────
async def send_weekly_report():
    await service_send_weekly_report(
        WeeklySendDeps(
            logger=logger,
            kst=KST,
            read_sheet_rows=read_sheet_rows,
            read_indicator_sheet=read_indicator_sheet,
            filter_rows_by_date=filter_rows_by_date,
            generate_weekly_report=generate_weekly_report,
            send_telegram_channel_message=send_telegram_channel_message,
        )
    )


# ── 리포트 스케줄러 ───────────────────────────────────────
async def report_scheduler():
    return await entrypoint_report_scheduler(
        ReportLoopDeps(
            logger=logger,
            kst=KST,
            send_daily_briefing=send_daily_briefing,
            send_weekly_report=send_weekly_report,
            was_briefing_sent=_was_briefing_sent,
            claim_briefing_dispatch=_claim_briefing_dispatch,
            update_briefing_dispatch_status=_update_briefing_dispatch_status,
        )
    )


async def people_maintenance_scheduler():
    return await entrypoint_people_maintenance_scheduler(
        PeopleLoopDeps(
            logger=logger,
            kst=KST,
            people_light_interval_min=PEOPLE_LIGHT_INTERVAL_MIN,
            people_sync_on_start=PEOPLE_SYNC_ON_START,
            people_dawn_enabled=PEOPLE_DAWN_ENABLED,
            people_dawn_hour=PEOPLE_DAWN_HOUR,
            people_dawn_minute=PEOPLE_DAWN_MINUTE,
            people_full_clean_weekday=PEOPLE_FULL_CLEAN_WEEKDAY,
            run_light=_run_people_maintenance_light,
            run_full=_run_people_maintenance_once,
        )
    )


def _build_ops_insight_text() -> str:
    return service_build_ops_insight_text(
        ReportOpsInsightDeps(
            kst=KST,
            read_sheet_rows=read_sheet_rows,
            parse_row_datetime=_parse_row_datetime,
            load_failed_url_queue=_load_failed_url_queue,
            failed_url_queue=_FAILED_URL_QUEUE,
        )
    )


async def failed_retry_scheduler():
    return await entrypoint_failed_retry_scheduler(
        FailedRetryLoopDeps(
            logger=logger,
            enabled=FAILED_RETRY_SCHEDULER_ENABLED,
            failed_url_retry_interval_min=FAILED_URL_RETRY_INTERVAL_MIN,
            retry_once=_retry_failed_urls_once,
        )
    )


def run_retry_worker():
    entrypoint_run_retry_worker(
        RetryWorkerDeps(
            logger=logger,
            interval_min=FAILED_URL_RETRY_INTERVAL_MIN,
            retry_once=_retry_failed_urls_once,
        )
    )


async def ops_insight_scheduler():
    return await entrypoint_ops_insight_scheduler(
        OpsLoopDeps(
            logger=logger,
            enabled=OPS_INSIGHT_ENABLED,
            interval_min=OPS_INSIGHT_INTERVAL_MIN,
            chat_id=OPS_INSIGHT_CHAT_ID,
            build_text=_build_ops_insight_text,
            send_message_to_chat=_send_telegram_message_to_chat,
            send_review_message=send_telegram_review_message,
        )
    )


# ── 노션 폴러 (asyncio) ──────────────────────────────────
async def notion_poller():
    return await entrypoint_notion_poller(
        PollerDeps(
            notion_client_factory=get_notion_client,
            get_unprocessed_pages=get_unprocessed_pages,
            process_page=process_page,
            dequeue_failed_url=_dequeue_failed_url,
            enqueue_failed_url=_enqueue_failed_url,
            poll_interval=POLL_INTERVAL,
        )
    )


# ── 메인 ─────────────────────────────────────────────────
async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN 환경변수가 설정되지 않았습니다.")

    print("🤖 경제 콘텐츠 에이전트 시작")
    print(f"   DB: {APP_RUNTIME_CONFIG.notion_database_id}")
    print(f"   인물DB: {APP_RUNTIME_CONFIG.person_db_id}")
    print(f"   적립형 운영: {APP_RUNTIME_CONFIG.people_accumulate_mode}")
    print(f"   노션 폴링 간격: {POLL_INTERVAL}초")
    print("   텔레그램 봇 연결 중...\n")

    app = build_bot_application(
        token=TELEGRAM_BOT_TOKEN,
        start_command=start_command,
        person_decision_callback=person_decision_callback,
        unknown_decision_callback=unknown_decision_callback,
        person_fix_command=person_fix_command,
        person_form_command=person_form_command,
        person_testbed_command=person_testbed_command,
        person_merge_command=person_merge_command,
        person_skip_command=person_skip_command,
        handle_message=handle_message,
    )
    print("✅ 텔레그램 봇 런타임 시작\n")

    await run_bot_runtime(
        app,
        notion_poller(),
        report_scheduler(),
        people_maintenance_scheduler(),
        failed_retry_scheduler(),
        ops_insight_scheduler(),
    )


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    handled = run_cli(
        args,
        CliActions(
            reprocess_all=reprocess_all,
            dedup_all=dedup_all,
            get_notion_client=get_notion_client,
            sync_sheets_from_notion=sync_sheets_from_notion,
            check_content_sync_status=check_content_sync_status,
            reconcile_content_sync=reconcile_content_sync,
            sync_expert_snapshot_sheet=lambda: _rebuild_expert_snapshot_sheet(get_notion_client()),
            init_review_queue_sheet=_init_review_queue_sheet,
            sync_review_queue_sheet=_sync_review_queue_sheet,
            apply_review_queue_resolutions=apply_review_queue_resolutions,
            sync_llm_context_sheets=sync_llm_context_sheets,
            init_trust_store=init_trust_store,
            bootstrap_trust_store=bootstrap_trust_store,
            ingest_claim_samples=ingest_claim_samples,
            recompute_trust_scores=recompute_trust_scores,
            refresh_claim_outcomes=refresh_claim_outcomes,
            send_daily_briefing=send_daily_briefing,
            send_weekly_report=send_weekly_report,
            sync_people_from_notion=sync_people_from_notion,
            rebuild_people_db=rebuild_people_db,
            check_person_db=check_person_db,
            check_people_sync_status=check_people_sync_status,
            reconcile_people_sync=reconcile_people_sync,
            check_non_economic_people=check_non_economic_people,
            queue_non_economic_people_review=queue_non_economic_people_review,
            clean_people_full=clean_people_full,
            backfill_person_source_links=backfill_person_source_links,
            purge_people_without_youtube_source=_purge_people_without_youtube_source,
            test_person_flow=test_person_flow,
            enrich_all_people=enrich_all_people,
            check_runtime_keys=check_runtime_keys,
            run_healthcheck_once=run_healthcheck_once,
            check_notion_access=check_notion_access,
            run_retry_worker=run_retry_worker,
            send_telegram_channel_message=send_telegram_channel_message,
        ),
        kst=KST,
        weekday_ko=_WEEKDAY_KO,
    )
    if not handled:
        asyncio.run(main())
