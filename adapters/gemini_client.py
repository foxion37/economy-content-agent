import json
import re
from typing import Any, Callable, Optional

from google import genai
from google.genai import types as genai_types


def repair_json(text: str) -> str:
    text = re.sub(r"```(?:json)?\s*|\s*```", "", text).strip()
    text = re.sub(r",\s*([}\]])", r"\1", text)

    def _fix_newlines(match: re.Match) -> str:
        return match.group(0).replace("\n", "\\n").replace("\r", "\\r")

    return re.sub(r'"(?:[^"\\]|\\.)*"', _fix_newlines, text, flags=re.DOTALL)


def parse_json_text(raw: str, normalize: Optional[Callable[[dict[str, Any]], dict[str, Any]]] = None):
    for attempt in (raw, repair_json(raw)):
        try:
            data = json.loads(attempt)
            return normalize(data) if normalize and isinstance(data, dict) else data
        except json.JSONDecodeError:
            pass
        match = re.search(r'[\[{][\s\S]*[\]}]', attempt)
        if match:
            chunk = match.group(0)
            for nested in (chunk, repair_json(chunk)):
                try:
                    data = json.loads(nested)
                    return normalize(data) if normalize and isinstance(data, dict) else data
                except json.JSONDecodeError:
                    pass
    return None


def analyze_with_gemini(
    api_key: str,
    metadata: dict[str, Any],
    build_prompt: Callable[..., str],
    normalize: Callable[[dict[str, Any]], dict[str, Any]],
    transcript: str = "",
    comments: Optional[list[str]] = None,
    channel_titles: Optional[list[str]] = None,
    channel_about: str = "",
    speaker_hint: str = "",
    recurring_person: str = "",
) -> dict[str, Any]:
    client = genai.Client(api_key=api_key)
    prompt = build_prompt(
        metadata,
        transcript=transcript,
        comments=comments,
        channel_titles=channel_titles,
        channel_about=channel_about,
        speaker_hint=speaker_hint,
        recurring_person=recurring_person,
    )
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            response_mime_type="application/json",
        ),
    )
    parsed = parse_json_text(response.text.strip(), normalize=normalize)
    if parsed is None:
        raise ValueError(f"JSON 파싱 실패. 응답 앞부분: {response.text.strip()[:300]}")
    return parsed

