from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(slots=True)
class VideoJob:
    url: str
    source: str
    page_id: Optional[str] = None
    chat_id: Optional[int] = None
    allow_interactive_review: bool = False


@dataclass(slots=True)
class PageContext:
    page_id: str
    props: dict[str, Any]


@dataclass(slots=True)
class EvidenceBundle:
    url: str
    video_id: str
    metadata: dict[str, Any]
    transcript: str = ""
    comments: list[str] = field(default_factory=list)
    channel_about: str = ""
    channel_titles: list[str] = field(default_factory=list)
    speaker_hint: str = ""
    recurring_person: str = ""


@dataclass(slots=True)
class PipelineResult:
    status: str
    page_id: Optional[str] = None
    message: str = ""
    analysis: Optional[dict[str, Any]] = None
    payload: Optional[Any] = None

