import re
from typing import Callable, Optional

from googleapiclient.discovery import build
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi


def extract_video_id(url: str) -> Optional[str]:
    patterns = [
        r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:embed/)([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def fetch_youtube_metadata(video_id: str, api_key: str, clean: Callable[[str], str]) -> dict:
    youtube = build("youtube", "v3", developerKey=api_key)
    response = youtube.videos().list(part="snippet", id=video_id).execute()
    if not response["items"]:
        return {}

    snippet = response["items"][0]["snippet"]
    return {
        "title": clean(snippet.get("title", "")),
        "description": clean(snippet.get("description", "")[:5000]),
        "channel": clean(snippet.get("channelTitle", "")),
        "channel_id": snippet.get("channelId", ""),
        "published_at": snippet.get("publishedAt", ""),
        "tags": [clean(tag) for tag in snippet.get("tags", [])[:20]],
    }


def fetch_youtube_comments(
    video_id: str,
    api_key: str,
    channel_id: str = "",
    max_results: int = 20,
) -> list[str]:
    youtube = build("youtube", "v3", developerKey=api_key)
    resp = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=max_results,
        order="relevance",
        textFormat="plainText",
    ).execute()
    owner_comments: list[str] = []
    other_comments: list[str] = []
    for item in resp.get("items", []):
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        text = snippet.get("textDisplay", "")
        if not text:
            continue
        author_channel = snippet.get("authorChannelId", {}).get("value", "")
        is_owner = bool(channel_id and author_channel == channel_id)
        (owner_comments if is_owner else other_comments).append(text[:300])
    return owner_comments + other_comments


def fetch_channel_about(channel_id: str, api_key: str, clean: Callable[[str], str]) -> str:
    if not channel_id:
        return ""
    youtube = build("youtube", "v3", developerKey=api_key)
    resp = youtube.channels().list(part="snippet", id=channel_id).execute()
    items = resp.get("items", [])
    if not items:
        return ""
    return clean(items[0]["snippet"].get("description", "")[:2000])


def fetch_channel_recent_video_titles(
    channel_id: str,
    api_key: str,
    clean: Callable[[str], str],
    max_results: int = 10,
) -> list[str]:
    if not channel_id:
        return []
    youtube = build("youtube", "v3", developerKey=api_key)
    channel_resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    items = channel_resp.get("items", [])
    if not items:
        return []
    uploads_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    playlist_resp = youtube.playlistItems().list(
        part="snippet",
        playlistId=uploads_id,
        maxResults=max_results,
    ).execute()
    return [
        clean(item["snippet"]["title"])
        for item in playlist_resp.get("items", [])
        if item.get("snippet", {}).get("title")
    ]


def fetch_transcript(video_id: str, clean: Callable[[str], str]) -> str:
    try:
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video_id)
        try:
            transcript = transcript_list.find_transcript(["ko"])
        except NoTranscriptFound:
            try:
                transcript = transcript_list.find_transcript(["en"])
            except NoTranscriptFound:
                return ""
        fetched = transcript.fetch()
        lines = []
        for snippet in fetched.snippets:
            minute = int(snippet.start) // 60
            second = int(snippet.start) % 60
            lines.append(f"[{minute}:{second:02d}] {snippet.text}")
        return clean("\n".join(lines)[:20000])
    except TranscriptsDisabled:
        return ""
    except Exception:
        return ""
