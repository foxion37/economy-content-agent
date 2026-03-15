import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


_FAILED_QUEUE_DB_READY: set[str] = set()


def append_ops_event(log_path: str, event_type: str, payload: dict, logger=None) -> None:
    try:
        dir_name = os.path.dirname(log_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        row = {
            "ts": datetime.now().astimezone().isoformat(),
            "event": event_type,
            "payload": payload,
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        if logger:
            logger.warning(f"ops event 저장 실패: {exc}")


def _failed_queue_db_path(path: str) -> str:
    if path.endswith(".json"):
        return path[:-5] + ".sqlite3"
    return path


def _ensure_failed_queue_db(db_path: str) -> None:
    if db_path in _FAILED_QUEUE_DB_READY and os.path.exists(db_path):
        return
    _FAILED_QUEUE_DB_READY.discard(db_path)
    dir_name = os.path.dirname(db_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS failed_url_queue (
                url TEXT PRIMARY KEY,
                retries INTEGER NOT NULL DEFAULT 0,
                next_ts REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_until REAL NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS briefing_dispatch_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                briefing_type TEXT NOT NULL,
                dispatch_key TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'sent',
                UNIQUE(briefing_type, dispatch_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS person_review_log (
                review_key TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                affiliation TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cols = {row[1] for row in conn.execute("PRAGMA table_info(failed_url_queue)").fetchall()}
        if "lease_owner" not in cols:
            conn.execute("ALTER TABLE failed_url_queue ADD COLUMN lease_owner TEXT NOT NULL DEFAULT ''")
        if "lease_until" not in cols:
            conn.execute("ALTER TABLE failed_url_queue ADD COLUMN lease_until REAL NOT NULL DEFAULT 0")
    _FAILED_QUEUE_DB_READY.add(db_path)


def _read_failed_queue_rows(db_path: str) -> dict[str, dict]:
    _ensure_failed_queue_db(db_path)
    out: dict[str, dict] = {}
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        rows = conn.execute(
            "SELECT url, retries, next_ts, last_error, updated_at FROM failed_url_queue"
        ).fetchall()
    for url, retries, next_ts, last_error, updated_at in rows:
        out[url] = {
            "retries": int(retries or 0),
            "next_ts": float(next_ts or 0),
            "last_error": last_error or "",
            "updated_at": updated_at or "",
        }
    return out


def _sync_failed_queue_rows(db_path: str, queue: dict[str, dict]) -> None:
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = {row[0] for row in conn.execute("SELECT url FROM failed_url_queue").fetchall()}
        target = set(queue.keys())
        stale = existing - target
        if stale:
            conn.executemany("DELETE FROM failed_url_queue WHERE url = ?", [(url,) for url in stale])
        conn.executemany(
            """
            INSERT INTO failed_url_queue (url, retries, next_ts, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                retries=excluded.retries,
                next_ts=excluded.next_ts,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at
            """,
            [
                (
                    url,
                    int(item.get("retries", 0) or 0),
                    float(item.get("next_ts", 0) or 0),
                    str(item.get("last_error", "") or ""),
                    str(item.get("updated_at", "") or ""),
                )
                for url, item in queue.items()
            ],
        )
        conn.commit()


def _claim_due_failed_urls(db_path: str, worker_id: str, lease_sec: int, limit: int = 20) -> list[str]:
    _ensure_failed_queue_db(db_path)
    now_ts = time.time()
    lease_until = now_ts + max(lease_sec, 60)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT url
            FROM failed_url_queue
            WHERE next_ts <= ?
              AND (lease_until <= ? OR lease_owner = '')
            ORDER BY next_ts ASC
            LIMIT ?
            """,
            (now_ts, now_ts, limit),
        ).fetchall()
        urls = [row[0] for row in rows]
        if urls:
            conn.executemany(
                """
                UPDATE failed_url_queue
                SET lease_owner = ?, lease_until = ?
                WHERE url = ?
                """,
                [(worker_id, lease_until, url) for url in urls],
            )
        conn.commit()
    return urls


def _release_failed_url_claim(db_path: str, url: str) -> None:
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute(
            "UPDATE failed_url_queue SET lease_owner = '', lease_until = 0 WHERE url = ?",
            (url,),
        )
        conn.commit()


def _delete_failed_url_row(db_path: str, url: str) -> None:
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("DELETE FROM failed_url_queue WHERE url = ?", (url,))
        conn.commit()


def _failed_url_row_exists(db_path: str, url: str) -> bool:
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        row = conn.execute("SELECT 1 FROM failed_url_queue WHERE url = ? LIMIT 1", (url,)).fetchone()
    return bool(row)


def was_briefing_sent(path: str, briefing_type: str, dispatch_key: str) -> bool:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM briefing_dispatch_log
            WHERE briefing_type = ? AND dispatch_key = ? AND status = 'sent'
            LIMIT 1
            """,
            (briefing_type, dispatch_key),
        ).fetchone()
    return bool(row)


def claim_briefing_dispatch(
    path: str,
    briefing_type: str,
    dispatch_key: str,
    claimed_at: str,
) -> bool:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT status, sent_at
            FROM briefing_dispatch_log
            WHERE briefing_type = ? AND dispatch_key = ?
            LIMIT 1
            """,
            (briefing_type, dispatch_key),
        ).fetchone()
        claimed = False
        if not row:
            conn.execute(
                """
                INSERT INTO briefing_dispatch_log (briefing_type, dispatch_key, sent_at, status)
                VALUES (?, ?, ?, 'pending')
                """,
                (briefing_type, dispatch_key, claimed_at),
            )
            claimed = True
        else:
            status = row[0] or ""
            last_ts = row[1] or ""
            stale_pending = False
            if status == "pending" and last_ts:
                try:
                    stale_pending = (datetime.now().astimezone() - datetime.fromisoformat(last_ts)).total_seconds() >= 600
                except Exception:
                    stale_pending = False
            if status == "failed" or stale_pending:
                conn.execute(
                    """
                    UPDATE briefing_dispatch_log
                    SET sent_at = ?, status = 'pending'
                    WHERE briefing_type = ? AND dispatch_key = ?
                    """,
                    (claimed_at, briefing_type, dispatch_key),
                )
                claimed = True
        conn.commit()
    return claimed


def update_briefing_dispatch_status(
    path: str,
    briefing_type: str,
    dispatch_key: str,
    *,
    sent_at: str,
    status: str,
) -> None:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute(
            """
            INSERT INTO briefing_dispatch_log (briefing_type, dispatch_key, sent_at, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(briefing_type, dispatch_key) DO UPDATE SET
                sent_at=excluded.sent_at,
                status=excluded.status
            """,
            (briefing_type, dispatch_key, sent_at, status),
        )
        conn.commit()


def load_person_review_rows(path: str) -> dict[str, dict]:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    out: dict[str, dict] = {}
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        rows = conn.execute(
            """
            SELECT review_key, name, affiliation, role, status, note, updated_at
            FROM person_review_log
            """
        ).fetchall()
    for review_key, name, affiliation, role, status, note, updated_at in rows:
        out[review_key] = {
            "name": name or "",
            "affiliation": affiliation or "",
            "role": role or "",
            "status": status or "",
            "note": note or "",
            "updated_at": updated_at or "",
        }
    return out


def save_person_review_row(
    path: str,
    review_key: str,
    *,
    name: str,
    affiliation: str,
    role: str,
    status: str,
    note: str,
    updated_at: str,
) -> None:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute(
            """
            INSERT INTO person_review_log (review_key, name, affiliation, role, status, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(review_key) DO UPDATE SET
                name=excluded.name,
                affiliation=excluded.affiliation,
                role=excluded.role,
                status=excluded.status,
                note=excluded.note,
                updated_at=excluded.updated_at
            """,
            (review_key, name, affiliation, role, status, note, updated_at),
        )
        conn.commit()


def get_person_review_row(path: str, review_key: str) -> dict | None:
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        row = conn.execute(
            """
            SELECT name, affiliation, role, status, note, updated_at
            FROM person_review_log
            WHERE review_key = ?
            LIMIT 1
            """,
            (review_key,),
        ).fetchone()
    if not row:
        return None
    name, affiliation, role, status, note, updated_at = row
    return {
        "name": name or "",
        "affiliation": affiliation or "",
        "role": role or "",
        "status": status or "",
        "note": note or "",
        "updated_at": updated_at or "",
    }


def _update_failed_url_row(
    db_path: str,
    url: str,
    *,
    retries: int,
    next_ts: float,
    last_error: str,
    updated_at: str,
) -> None:
    _ensure_failed_queue_db(db_path)
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute(
            """
            INSERT INTO failed_url_queue (url, retries, next_ts, last_error, updated_at, lease_owner, lease_until)
            VALUES (?, ?, ?, ?, ?, '', 0)
            ON CONFLICT(url) DO UPDATE SET
                retries=excluded.retries,
                next_ts=excluded.next_ts,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at,
                lease_owner='',
                lease_until=0
            """,
            (url, retries, next_ts, last_error, updated_at),
        )
        conn.commit()


def _enqueue_failed_url_row(
    db_path: str,
    url: str,
    *,
    reason: str,
    retry_max: int,
    retry_interval_min: int,
    updated_at: str,
) -> dict:
    _ensure_failed_queue_db(db_path)
    now_ts = time.time()
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT retries FROM failed_url_queue WHERE url = ?",
            (url,),
        ).fetchone()
        retries = int((row[0] if row else 0) or 0) + 1
        next_ts = now_ts + (86400 if retries > retry_max else max(retry_interval_min, 1) * 60)
        last_error = (
            f"max_retries_exceeded: {reason[:200]}"
            if retries > retry_max
            else reason[:200]
        )
        conn.execute(
            """
            INSERT INTO failed_url_queue (url, retries, next_ts, last_error, updated_at, lease_owner, lease_until)
            VALUES (?, ?, ?, ?, ?, '', 0)
            ON CONFLICT(url) DO UPDATE SET
                retries=excluded.retries,
                next_ts=excluded.next_ts,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at,
                lease_owner='',
                lease_until=0
            """,
            (url, retries, next_ts, last_error, updated_at),
        )
        conn.commit()
    return {
        "retries": retries,
        "next_ts": next_ts,
        "last_error": last_error,
        "updated_at": updated_at,
    }


def _migrate_failed_queue_json(path: str, logger=None) -> None:
    if not path.endswith(".json") or not os.path.exists(path):
        return
    db_path = _failed_queue_db_path(path)
    _ensure_failed_queue_db(db_path)
    try:
        with sqlite3.connect(db_path, isolation_level=None) as conn:
            existing_count = conn.execute("SELECT COUNT(*) FROM failed_url_queue").fetchone()[0]
        if existing_count:
            return
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or not data:
            return
        _sync_failed_queue_rows(db_path, data)
    except Exception as exc:
        if logger:
            logger.warning(f"실패 URL 큐 JSON→SQLite 마이그레이션 실패: {exc}")


def load_failed_url_queue(path: str, logger=None) -> dict[str, dict]:
    try:
        _migrate_failed_queue_json(path, logger=logger)
        return _read_failed_queue_rows(_failed_queue_db_path(path))
    except Exception as exc:
        if logger:
            logger.warning(f"실패 URL 큐 로드 실패: {exc}")
    return {}


def save_failed_url_queue(path: str, queue: dict[str, dict], logger=None) -> None:
    try:
        if logger:
            logger.warning("save_failed_url_queue는 레거시 전체 동기화 경로입니다. 가능하면 enqueue/dequeue atomic 경로를 사용하세요.")
        _sync_failed_queue_rows(_failed_queue_db_path(path), queue)
    except Exception as exc:
        if logger:
            logger.warning(f"실패 URL 큐 저장 실패: {exc}")


@dataclass(slots=True)
class FailedQueueDeps:
    queue_path: str
    retry_max: int
    retry_interval_min: int
    kst: object
    get_notion_client: Callable[[], object]
    extract_video_id: Callable[[str], str]
    find_duplicate_pages: Callable[[object, str], list[dict]]
    is_incomplete: Callable[[dict], bool]
    create_notion_page_from_url: Callable[[object, str], str]
    save_queue: Callable[[str, dict[str, dict]], None]
    event_log_path: str
    logger: object


def enqueue_failed_url(
    queue: dict[str, dict],
    url: str,
    reason: str,
    deps: FailedQueueDeps,
) -> dict[str, dict]:
    if not url:
        return queue
    db_path = _failed_queue_db_path(deps.queue_path)
    row = _enqueue_failed_url_row(
        db_path,
        url,
        reason=reason,
        retry_max=deps.retry_max,
        retry_interval_min=deps.retry_interval_min,
        updated_at=datetime.now(deps.kst).isoformat(),
    )
    queue = load_failed_url_queue(deps.queue_path, logger=deps.logger)
    append_ops_event(
        deps.event_log_path,
        "failed_url_enqueued",
        {"url": url, "reason": reason[:200], "retries": row["retries"]},
        deps.logger,
    )
    return queue


def dequeue_failed_url(
    queue: dict[str, dict],
    url: str,
    deps: FailedQueueDeps,
) -> dict[str, dict]:
    db_path = _failed_queue_db_path(deps.queue_path)
    queue = load_failed_url_queue(deps.queue_path, logger=deps.logger)
    if url in queue:
        _delete_failed_url_row(db_path, url)
        queue.pop(url, None)
        append_ops_event(deps.event_log_path, "failed_url_dequeued", {"url": url}, deps.logger)
    return queue


def retry_failed_urls_once(
    queue: dict[str, dict],
    deps: FailedQueueDeps,
) -> tuple[dict[str, dict], dict]:
    db_path = _failed_queue_db_path(deps.queue_path)
    queue = load_failed_url_queue(deps.queue_path, logger=deps.logger)
    if not queue:
        return queue, {"queued": 0, "requeued": 0, "claimed": 0}

    worker_id = f"retry-worker-{os.getpid()}-{int(time.time() * 1000)}"
    claimed_urls = _claim_due_failed_urls(
        db_path,
        worker_id,
        lease_sec=max(deps.retry_interval_min, 1) * 60,
        limit=20,
    )
    if not claimed_urls:
        return load_failed_url_queue(deps.queue_path, logger=deps.logger), {"queued": len(queue), "requeued": 0, "claimed": 0}

    snapshot = _read_failed_queue_rows(db_path)
    notion = deps.get_notion_client()
    now_ts = time.time()
    requeued = 0
    for url in claimed_urls:
        item = snapshot.get(url, {})
        try:
            vid = deps.extract_video_id(url)
            if not vid:
                _delete_failed_url_row(db_path, url)
                continue
            existing = deps.find_duplicate_pages(notion, vid)
            completed = [p for p in existing if not deps.is_incomplete(p)]
            incomplete = [p for p in existing if deps.is_incomplete(p)]
            if completed:
                _delete_failed_url_row(db_path, url)
                continue
            if incomplete:
                notion.pages.update(
                    page_id=incomplete[0]["id"],
                    properties={"주제": {"rich_text": []}},
                )
            else:
                deps.create_notion_page_from_url(notion, url)
            _delete_failed_url_row(db_path, url)
            requeued += 1
            append_ops_event(deps.event_log_path, "failed_url_requeued", {"url": url}, deps.logger)
        except Exception as exc:
            if _failed_url_row_exists(db_path, url):
                retries = int(item.get("retries", 0) or 0) + 1
                next_ts = now_ts + (
                    86400 if retries > deps.retry_max else max(deps.retry_interval_min, 1) * 60
                )
                last_error = (
                    f"max_retries_exceeded: {str(exc)[:200]}"
                    if retries > deps.retry_max
                    else str(exc)[:200]
                )
                _update_failed_url_row(
                    db_path,
                    url,
                    retries=retries,
                    next_ts=next_ts,
                    last_error=last_error,
                    updated_at=datetime.now(deps.kst).isoformat(),
                )
            append_ops_event(deps.event_log_path, "failed_url_retry_error", {"url": url, "error": str(exc)[:200]}, deps.logger)
    queue = load_failed_url_queue(deps.queue_path, logger=deps.logger)
    return queue, {"queued": len(queue), "requeued": requeued, "claimed": len(claimed_urls)}
