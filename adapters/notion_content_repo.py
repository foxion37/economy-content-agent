from datetime import datetime
from typing import Any, Callable


def create_page_from_url(notion, database_id: str, url: str) -> str:
    response = notion.pages.create(
        parent={"database_id": database_id},
        properties={
            "URL": {"url": url},
            "콘텐츠 제목": {"title": [{"text": {"content": "처리 중..."}}]},
        },
    )
    return response["id"]


def get_unprocessed_pages(
    database_id: str,
    notion_api_key: str,
    httpx_module,
) -> list[dict[str, Any]]:
    response = httpx_module.post(
        f"https://api.notion.com/v1/databases/{database_id}/query",
        headers={
            "Authorization": f"Bearer {notion_api_key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        },
        json={
            "filter": {
                "and": [
                    {"property": "URL", "url": {"is_not_empty": True}},
                    {"property": "주제", "rich_text": {"is_empty": True}},
                ]
            }
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("results", [])


def write_result(
    notion,
    page_id: str,
    video_title: str,
    analysis: dict[str, Any],
    verified_name: str,
    verified_affiliation: str,
    role: str,
    now_fn: Callable[[], datetime],
    build_blocks: Callable[[dict[str, Any], str, str, str], list[Any]],
    out=None,
) -> tuple[str, str]:
    hashtags = " ".join(analysis.get("hashtags", []))
    summary = analysis.get("summary", "")
    person_str = f"{verified_affiliation} ({role}) / {verified_name}" if role and role != "미상" else f"{verified_affiliation} / {verified_name}"
    opinion = analysis.get("opinion", "")
    timestamp = now_fn().strftime("%Y-%m-%d %H:%M")

    def rt(text: str) -> list[dict[str, Any]]:
        return [{"text": {"content": text[:2000]}}]

    notion.pages.update(
        page_id=page_id,
        properties={
            "콘텐츠 제목": {"title": rt(video_title)},
            "주제": {"rich_text": rt(hashtags)},
            "한 줄 요약": {"rich_text": rt(summary)},
            "출연자": {"rich_text": rt(person_str)},
            "인물의견": {"rich_text": rt(opinion)},
            "처리일시": {"rich_text": rt(timestamp)},
        },
    )

    try:
        existing = notion.blocks.children.list(block_id=page_id)
        if existing.get("results"):
            if out:
                out("  ℹ️ 이미 본문 블록이 있어 추가를 건너뜀")
        else:
            notion.blocks.children.append(
                block_id=page_id,
                children=build_blocks(analysis, verified_name, verified_affiliation, role),
            )
    except Exception as exc:
        if out:
            out(f"  ⚠️ 본문 블록 추가 실패: {exc}")

    return person_str, timestamp
