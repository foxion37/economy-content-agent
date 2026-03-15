from dataclasses import dataclass
from typing import Any, Callable, Optional

from domain.models import EvidenceBundle, PageContext, PipelineResult, VideoJob


@dataclass(slots=True)
class ContentPipelineDeps:
    extract_video_id: Callable[[str], Optional[str]]
    fetch_metadata: Callable[[str], dict[str, Any]]
    fetch_transcript: Callable[[str], str]
    fetch_comments: Callable[[str, str], list[str]]
    fetch_channel_about: Callable[[str], str]
    fetch_channel_titles: Callable[[str], list[str]]
    extract_speaker_hint: Callable[[str], str]
    detect_recurring_person: Callable[[list[str]], str]
    analyze: Callable[..., dict[str, Any]]
    finalize: Callable[[PageContext, VideoJob, EvidenceBundle, dict[str, Any]], Optional[dict[str, Any]]]
    on_invalid_url: Callable[[PageContext, VideoJob], PipelineResult]
    on_missing_metadata: Callable[[PageContext, VideoJob], PipelineResult]
    on_unknown_person: Optional[
        Callable[[PageContext, VideoJob, EvidenceBundle, dict[str, Any]], Optional[PipelineResult]]
    ] = None
    on_exception: Optional[
        Callable[[PageContext, VideoJob, Exception], PipelineResult]
    ] = None


def run_content_pipeline(
    job: VideoJob,
    page: PageContext,
    deps: ContentPipelineDeps,
) -> PipelineResult:
    """Shared content-processing engine used by Telegram, pollers, retry, and CLI."""
    try:
        video_id = deps.extract_video_id(job.url)
        if not video_id:
            return deps.on_invalid_url(page, job)

        metadata = deps.fetch_metadata(video_id)
        if not metadata:
            return deps.on_missing_metadata(page, job)

        channel_id = metadata.get("channel_id", "")
        transcript = deps.fetch_transcript(video_id)
        comments = deps.fetch_comments(video_id, channel_id)
        channel_about = deps.fetch_channel_about(channel_id)
        channel_titles = deps.fetch_channel_titles(channel_id)
        speaker_hint = deps.extract_speaker_hint(transcript)
        recurring_person = deps.detect_recurring_person(channel_titles)

        evidence = EvidenceBundle(
            url=job.url,
            video_id=video_id,
            metadata=metadata,
            transcript=transcript,
            comments=comments,
            channel_about=channel_about,
            channel_titles=channel_titles,
            speaker_hint=speaker_hint,
            recurring_person=recurring_person,
        )

        analysis = deps.analyze(
            metadata,
            transcript,
            comments=comments,
            channel_titles=channel_titles,
            channel_about=channel_about,
            speaker_hint=speaker_hint,
            recurring_person=recurring_person,
        )

        if deps.on_unknown_person:
            review_result = deps.on_unknown_person(page, job, evidence, analysis)
            if review_result is not None:
                return review_result

        result = deps.finalize(page, job, evidence, analysis)
        if result:
            return PipelineResult(
                status="done",
                page_id=page.page_id,
                analysis=analysis,
                payload=result,
            )
        return PipelineResult(
            status="perm_fail",
            page_id=page.page_id,
            message="finalize_failed",
            analysis=analysis,
        )
    except Exception as exc:
        if deps.on_exception:
            return deps.on_exception(page, job, exc)
        raise

