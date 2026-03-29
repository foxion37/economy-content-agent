from dataclasses import dataclass
from typing import Any
import re


COMMON_CONTENT_FIELDS = (
    "title",
    "channel",
    "hashtags",
    "summary",
    "person_str",
    "opinion",
    "mentioned_products",
    "key_sectors",
    "economic_outlook",
    "timestamp",
)


@dataclass(slots=True)
class ContentSyncRecord:
    url: str
    title: str = ""
    channel: str = ""
    hashtags: str = ""
    summary: str = ""
    person_str: str = ""
    opinion: str = ""
    mentioned_products: str = ""
    key_sectors: str = ""
    economic_outlook: str = ""
    timestamp: str = ""
    source: str = ""
    source_ref: str = ""


def _compact_text(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text)


def _tokenize_csvish(value: Any) -> list[str]:
    text = _compact_text(value)
    if not text:
        return []
    parts = re.split(r"[,\n;/|]+", text)
    return [p.strip() for p in parts if p and p.strip()]


def _tokenize_hashtags(value: Any) -> list[str]:
    text = _compact_text(value)
    if not text:
        return []
    parts = re.split(r"[\s,]+", text)
    tags: list[str] = []
    for part in parts:
        token = part.strip()
        if not token:
            continue
        if not token.startswith("#"):
            token = f"#{token}"
        tags.append(token)
    return tags


def normalized_field_value(field: str, value: Any) -> str:
    if field == "hashtags":
        return " ".join(sorted(dict.fromkeys(_tokenize_hashtags(value))))
    if field in {"mentioned_products", "key_sectors"}:
        return ", ".join(sorted(dict.fromkeys(_tokenize_csvish(value))))
    return _compact_text(value)


def normalized_record(record: ContentSyncRecord) -> dict[str, str]:
    data = {"url": _compact_text(record.url)}
    for field in COMMON_CONTENT_FIELDS:
        data[field] = normalized_field_value(field, getattr(record, field))
    return data


def differing_fields(notion_record: ContentSyncRecord, sheet_record: ContentSyncRecord) -> list[str]:
    notion_norm = normalized_record(notion_record)
    sheet_norm = normalized_record(sheet_record)
    diffs: list[str] = []
    for field in COMMON_CONTENT_FIELDS:
        if notion_norm[field] != sheet_norm[field]:
            diffs.append(field)
    return diffs


def classify_sync(
    notion_records: list[ContentSyncRecord],
    sheet_records: list[ContentSyncRecord],
) -> dict[str, Any]:
    notion_map = {normalized_field_value("url", row.url): row for row in notion_records if normalized_field_value("url", row.url)}
    sheet_map = {normalized_field_value("url", row.url): row for row in sheet_records if normalized_field_value("url", row.url)}

    only_notion: list[ContentSyncRecord] = []
    only_sheet: list[ContentSyncRecord] = []
    in_sync: list[dict[str, Any]] = []
    field_conflict: list[dict[str, Any]] = []

    notion_urls = set(notion_map)
    sheet_urls = set(sheet_map)

    for url in sorted(notion_urls - sheet_urls):
        only_notion.append(notion_map[url])

    for url in sorted(sheet_urls - notion_urls):
        only_sheet.append(sheet_map[url])

    for url in sorted(notion_urls & sheet_urls):
        notion_row = notion_map[url]
        sheet_row = sheet_map[url]
        diffs = differing_fields(notion_row, sheet_row)
        if diffs:
            field_conflict.append(
                {
                    "url": url,
                    "fields": diffs,
                    "notion": notion_row,
                    "sheet": sheet_row,
                }
            )
            continue
        in_sync.append(
            {
                "url": url,
                "notion": notion_row,
                "sheet": sheet_row,
            }
        )

    return {
        "in_sync": in_sync,
        "only_notion": only_notion,
        "only_sheet": only_sheet,
        "field_conflict": field_conflict,
    }


def render_sync_report(
    sync_result: dict[str, Any],
    *,
    notion_total: int,
    sheet_total: int,
) -> str:
    lines = [
        "# Content Sync Report",
        "",
        "## Summary",
        f"- Notion complete rows: {notion_total}",
        f"- Sheets complete rows: {sheet_total}",
        f"- In sync: {len(sync_result['in_sync'])}",
        f"- Only in Notion: {len(sync_result['only_notion'])}",
        f"- Only in Sheets: {len(sync_result['only_sheet'])}",
        f"- Field conflicts: {len(sync_result['field_conflict'])}",
        "",
    ]

    def add_record_section(title: str, rows: list[Any]) -> None:
        lines.append(f"## {title}")
        if not rows:
            lines.append("- none")
            lines.append("")
            return
        for row in rows:
            record = row if isinstance(row, ContentSyncRecord) else row.get("notion") or row.get("sheet")
            url = getattr(record, "url", "") if record else ""
            label = getattr(record, "title", "") if record else ""
            extra = f" — {label}" if label else ""
            lines.append(f"- {url}{extra}")
        lines.append("")

    add_record_section("Only In Notion", sync_result["only_notion"])
    add_record_section("Only In Sheets", sync_result["only_sheet"])

    lines.append("## Field Conflicts")
    if not sync_result["field_conflict"]:
        lines.append("- none")
        lines.append("")
    else:
        for row in sync_result["field_conflict"]:
            lines.append(f"- {row['url']} ({', '.join(row['fields'])})")
        lines.append("")

    return "\n".join(lines)
