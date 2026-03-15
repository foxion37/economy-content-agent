from datetime import datetime
from typing import Any, Callable


def read_rows(service, spreadsheet_id: str, data_range: str, logger=None) -> list[dict[str, Any]]:
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=data_range,
        ).execute()
        values = result.get("values", [])
        if len(values) < 2:
            return []
        headers = values[0]
        rows = []
        for row in values[1:]:
            padded = row + [""] * (len(headers) - len(row))
            rows.append(dict(zip(headers, padded)))
        return rows
    except Exception as exc:
        if logger:
            logger.error(f"시트 데이터 조회 실패: {exc}")
        return []


def read_indicator_rows(service, spreadsheet_id: str, data_range: str, logger=None) -> dict[str, Any]:
    try:
        values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=data_range,
        ).execute().get("values", [])
        if not values:
            return {}

        master_signal = values[0][0] if values[0] else ""
        headers = values[2] if len(values) > 2 else []
        col = {h.strip(): i for i, h in enumerate(headers)}

        def _get(row: list[str], key: str, default: str = "") -> str:
            idx = col.get(key)
            if idx is None or idx >= len(row):
                return default
            value = row[idx]
            return value if "#REF!" not in value else default

        indicators = []
        for row in values[3:]:
            if not row:
                continue
            name = _get(row, "지표명")
            if not name.strip() or "#REF!" in name:
                continue
            indicators.append(
                {
                    "지표명": name,
                    "현재 수치": _get(row, "현재 수치"),
                    "전일 수치": _get(row, "전일 수치"),
                    "기준": _get(row, "기준(평균)"),
                    "MA-20": _get(row, "MA-20"),
                    "MA-120": _get(row, "MA-120"),
                    "MA-200": _get(row, "MA-200"),
                    "신호등": _get(row, "신호등"),
                    "비고": _get(row, "비고"),
                }
            )
        return {"master_signal": master_signal, "indicators": indicators}
    except Exception as exc:
        if logger:
            logger.error(f"경제 지표 시트 조회 실패: {exc}")
        return {}


def append_analysis_row(
    service,
    spreadsheet_id: str,
    headers: list[str],
    url: str,
    video_title: str,
    channel: str,
    analysis: dict[str, Any],
    person_str: str,
    timestamp: str,
    is_complete: Callable[[str, str, str], bool],
    out=None,
):
    summary = analysis.get("summary", "")
    opinion = analysis.get("opinion", "")
    if not is_complete(summary, person_str, opinion):
        if out:
            out("  ℹ️ 구글 시트 스킵 (summary/person/opinion 미완성)")
        return False

    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=spreadsheet_id,
        range="A:A",
    ).execute()
    existing_rows = result.get("values", [])

    if not existing_rows:
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range="A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
        existing_urls = []
    else:
        existing_urls = [row[0] for row in existing_rows[1:] if row]

    if url in existing_urls:
        if out:
            out("  ℹ️ 구글 시트 중복 URL 스킵")
        return False

    outlook = analysis.get("economic_outlook") or {}
    if isinstance(outlook, dict):
        direction = outlook.get("direction", "")
        description = outlook.get("description", "")
        outlook_text = f"{direction}: {description}" if direction else description
    else:
        outlook_text = str(outlook)

    row = [
        url,
        video_title,
        channel,
        " ".join(analysis.get("hashtags", [])),
        analysis.get("summary", ""),
        person_str,
        analysis.get("opinion", ""),
        ", ".join(analysis.get("mentioned_products", [])),
        ", ".join(analysis.get("key_sectors", [])),
        outlook_text,
        timestamp,
    ]
    sheet.values().append(
        spreadsheetId=spreadsheet_id,
        range="A:K",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()
    if out:
        out("  ✓ 구글 시트 기록 완료")
    return True


def append_briefing_log(
    service,
    spreadsheet_id: str,
    tab: str,
    headers: list[str],
    briefing: str,
    start_kst: datetime,
    end_kst: datetime,
    video_count: int,
    indicator_count: int,
    sent_ok: bool,
):
    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A:H",
    ).execute().get("values", [])
    if not values:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
    row = [
        datetime.now(start_kst.tzinfo).strftime("%Y-%m-%d %H:%M:%S"),
        start_kst.strftime("%Y-%m-%d %H:%M"),
        end_kst.strftime("%Y-%m-%d %H:%M"),
        video_count,
        indicator_count,
        "성공" if sent_ok else "실패",
        "",
        (briefing or "")[:45000],
    ]
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A:H",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

