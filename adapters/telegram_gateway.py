import re
from typing import Callable, Optional

import httpx


def strip_html_tags(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"<a\s+[^>]*>(.*?)</a>", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"</?[^>]+>", "", text)
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def split_html_message(text: str, max_len: int = 4000) -> list[str]:
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


def send_message_to_chat(
    bot_token: str,
    chat_id: str,
    text: str,
    reply_markup: Optional[dict] = None,
    logger=None,
) -> bool:
    if not chat_id or not bot_token:
        if logger:
            logger.warning("chat_id 또는 TELEGRAM_BOT_TOKEN 미설정 — 텔레그램 발송 스킵")
        return False

    for idx, chunk in enumerate(split_html_message(text)):
        try:
            payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
            if idx == 0 and reply_markup:
                payload["reply_markup"] = reply_markup
            resp = httpx.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            continue
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                if logger:
                    logger.warning("텔레그램 HTML 전송 400 — plain text fallback 시도")
                plain = strip_html_tags(chunk)
                try:
                    payload2 = {"chat_id": chat_id, "text": plain}
                    if idx == 0 and reply_markup:
                        payload2["reply_markup"] = reply_markup
                    resp2 = httpx.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json=payload2,
                        timeout=30,
                    )
                    resp2.raise_for_status()
                    continue
                except Exception as fallback_exc:
                    if logger:
                        logger.error(f"텔레그램 fallback 발송 실패: {fallback_exc}")
                    return False
            if logger:
                logger.error(f"텔레그램 채널 발송 실패: {exc}")
            return False
        except Exception as exc:
            if logger:
                logger.error(f"텔레그램 채널 발송 실패: {exc}")
            return False
    return True
