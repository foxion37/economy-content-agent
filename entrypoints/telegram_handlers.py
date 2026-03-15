from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(slots=True)
class StartPayloadResult:
    handled: bool
    reply_text: str


def handle_start_payload(
    payload: str,
    user_id: str,
    username: str,
    save_daily_feedback: Callable[[str, str, str], None],
) -> Optional[StartPayloadResult]:
    normalized = (payload or "").strip().lower()
    if normalized in ("daily_feedback_good", "daily_feedback_bad"):
        label = "좋아요" if normalized.endswith("good") else "아쉬워요"
        save_daily_feedback(label, user_id, username)
        return StartPayloadResult(True, f"피드백이 기록되었습니다: {label}")
    if normalized == "settings":
        return StartPayloadResult(
            True,
            "설정 메뉴는 준비 중입니다. 현재는 링크 분석 기능을 바로 사용할 수 있습니다.",
        )
    return None
