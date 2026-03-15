#!/usr/bin/env python3
"""
Economy expert data cleansing pipeline.

Features:
1) Name normalization (remove titles/roles from name)
2) Affiliation canonicalization (alias mapping + fuzzy fallback)
3) Temporal/currentness scoring
4) Conflict resolution per person
5) Review queue export for ambiguous/low-confidence records

No third-party dependencies are required.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, List, Tuple


# 직책/호칭 패턴 (한글 + 영문)
ROLE_TOKENS = [
    "교수",
    "부교수",
    "조교수",
    "연구원",
    "선임연구원",
    "책임연구원",
    "박사",
    "대표",
    "대표이사",
    "이사",
    "상무",
    "전무",
    "부장",
    "팀장",
    "위원장",
    "위원",
    "센터장",
    "원장",
    "소장",
    "총장",
    "학장",
    "사장",
    "국장",
    "장관",
    "차관",
    "의원",
    "CEO",
    "CTO",
    "CFO",
    "COO",
    "President",
    "Director",
    "Manager",
    "Professor",
    "Researcher",
    "Analyst",
    "PhD",
    "Dr",
    "Mr",
    "Ms",
]

ROLE_RE = re.compile(
    r"(?i)\b(" + "|".join(re.escape(t) for t in sorted(ROLE_TOKENS, key=len, reverse=True)) + r")\.?\b"
)

# 과거 직책 신호
PAST_SIGNALS = [
    "전 ",
    "前",
    "former",
    "ex-",
    "퇴임",
    "역임",
    "전직",
    "前직",
]

# 출처 신뢰도
SOURCE_RELIABILITY = {
    "official": 1.0,
    "government": 0.95,
    "university": 0.9,
    "association": 0.85,
    "news": 0.7,
    "portal": 0.6,
    "blog": 0.4,
    "unknown": 0.5,
}

# 소속 표준화 사전 (필요시 확장)
AFFILIATION_CANONICAL = {
    "서울대학교": ["서울대", "서울대학교", "seoul national university", "snu"],
    "한국은행": ["한국은행", "bank of korea", "bok"],
    "기획재정부": ["기획재정부", "기재부", "ministry of economy and finance", "moef"],
    "한국개발연구원": ["kdi", "한국개발연구원", "korea development institute"],
    "연세대학교": ["연세대", "연세대학교", "yonsei university"],
    "고려대학교": ["고려대", "고려대학교", "korea university"],
}

DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y")


@dataclass
class ScoredRow:
    row: Dict[str, str]
    person_key: str
    normalized_name: str
    normalized_affiliation: str
    is_past_title: bool
    is_past_affiliation: bool
    score: float


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def remove_bracket_content(name: str) -> str:
    return re.sub(r"[\(\[\{（【].*?[\)\]\}）】]", " ", name)


def normalize_name(raw_name: str) -> str:
    name = normalize_space(raw_name)
    name = remove_bracket_content(name)
    name = name.replace(",", " ")
    name = ROLE_RE.sub(" ", name)
    # 접두/접미 표식 제거
    name = re.sub(r"(?i)^(prof|dr|mr|ms)\.?\s+", "", name)
    name = re.sub(r"(?i)\s+(prof|dr|phd)\.?$", "", name)
    # 허용 문자(한글/영문/공백/하이픈)만 유지
    name = re.sub(r"[^A-Za-z가-힣\-\s]", " ", name)
    name = normalize_space(name)
    return name


def has_past_signal(text: str) -> bool:
    t = (text or "").lower()
    return any(sig.lower() in t for sig in PAST_SIGNALS)


def normalize_affiliation(raw_affiliation: str) -> str:
    text = normalize_space(raw_affiliation)
    low = text.lower()

    # exact alias hit
    for canonical, aliases in AFFILIATION_CANONICAL.items():
        for alias in aliases:
            if low == alias.lower():
                return canonical

    # fuzzy alias fallback
    best_label = text
    best_score = 0.0
    for canonical, aliases in AFFILIATION_CANONICAL.items():
        for alias in aliases:
            s = SequenceMatcher(None, low, alias.lower()).ratio()
            if s > best_score:
                best_score = s
                best_label = canonical

    # 임계값은 높게 둬서 과교정 방지
    if best_score >= 0.86:
        return best_label
    return text


def parse_date(date_text: str) -> dt.date | None:
    raw = normalize_space(date_text)
    if not raw:
        return None
    for f in DATE_FORMATS:
        try:
            d = dt.datetime.strptime(raw, f).date()
            if f in ("%Y-%m", "%Y/%m"):
                return d.replace(day=1)
            if f == "%Y":
                return d.replace(month=1, day=1)
            return d
        except ValueError:
            continue
    return None


def recency_score(source_date: dt.date | None, today: dt.date) -> float:
    if source_date is None:
        return 0.5
    days = (today - source_date).days
    if days < 0:
        days = 0
    # 최근일수록 점수↑ (약 1년 기준 절반 감소)
    return math.exp(-days / 365.0)


def reliability_score(source_type: str) -> float:
    return SOURCE_RELIABILITY.get((source_type or "").strip().lower(), SOURCE_RELIABILITY["unknown"])


def compute_score(
    source_type: str,
    source_date: dt.date | None,
    is_past_title: bool,
    is_past_affiliation: bool,
    today: dt.date,
) -> float:
    rel = reliability_score(source_type)
    rec = recency_score(source_date, today)
    currentness = 1.0
    if is_past_title:
        currentness -= 0.35
    if is_past_affiliation:
        currentness -= 0.35
    currentness = max(currentness, 0.0)
    # 가중합: 신뢰도 45%, 최신성 35%, 현재성 20%
    return 0.45 * rel + 0.35 * rec + 0.20 * currentness


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def write_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def enrich_rows(rows: List[Dict[str, str]], today: dt.date) -> List[ScoredRow]:
    output: List[ScoredRow] = []
    for row in rows:
        raw_name = row.get("name", "")
        raw_aff = row.get("affiliation", "")
        raw_title = row.get("title", "")
        source_type = row.get("source_type", "unknown")
        d = parse_date(row.get("source_date", ""))
        norm_name = normalize_name(raw_name)
        norm_aff = normalize_affiliation(raw_aff)
        past_title = has_past_signal(raw_title)
        past_aff = has_past_signal(raw_aff)
        score = compute_score(source_type, d, past_title, past_aff, today)

        # person_key는 이름 기반. 실제 운영에서는 email/기관ID 등 추가 결합 권장.
        person_key = norm_name.lower()
        output.append(
            ScoredRow(
                row=row,
                person_key=person_key,
                normalized_name=norm_name,
                normalized_affiliation=norm_aff,
                is_past_title=past_title,
                is_past_affiliation=past_aff,
                score=score,
            )
        )
    return output


def resolve_conflicts(scored_rows: List[ScoredRow], margin_threshold: float) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    grouped: Dict[str, List[ScoredRow]] = defaultdict(list)
    for r in scored_rows:
        grouped[r.person_key].append(r)

    current_rows: List[Dict[str, str]] = []
    history_rows: List[Dict[str, str]] = []
    review_rows: List[Dict[str, str]] = []

    for person_key, items in grouped.items():
        items = sorted(items, key=lambda x: x.score, reverse=True)
        top = items[0]
        second = items[1] if len(items) > 1 else None
        margin = (top.score - second.score) if second else 1.0
        needs_review = top.score < 0.75 or margin < margin_threshold

        for i, it in enumerate(items):
            base = dict(it.row)
            base.update(
                {
                    "person_key": person_key,
                    "normalized_name": it.normalized_name,
                    "normalized_affiliation": it.normalized_affiliation,
                    "is_past_title": str(it.is_past_title),
                    "is_past_affiliation": str(it.is_past_affiliation),
                    "confidence_score": f"{it.score:.4f}",
                    "rank_within_person": str(i + 1),
                    "review_required": str(needs_review),
                }
            )
            history_rows.append(base)

        winner = dict(top.row)
        winner.update(
            {
                "person_key": person_key,
                "name": top.normalized_name,
                "affiliation": top.normalized_affiliation,
                "normalized_name": top.normalized_name,
                "normalized_affiliation": top.normalized_affiliation,
                "is_past_title": str(top.is_past_title),
                "is_past_affiliation": str(top.is_past_affiliation),
                "confidence_score": f"{top.score:.4f}",
                "candidate_count": str(len(items)),
                "margin_to_second": f"{margin:.4f}",
                "rank_within_person": "1",
                "review_required": str(needs_review),
            }
        )
        current_rows.append(winner)

        if needs_review:
            review = dict(winner)
            review["review_reason"] = (
                "low_confidence" if top.score < 0.75 else "small_margin_between_candidates"
            )
            review_rows.append(review)

    return current_rows, history_rows, review_rows


def make_sample(path: str) -> None:
    rows = [
        {
            "id": "1",
            "name": "홍길동 교수",
            "affiliation": "서울대",
            "title": "경제학과 교수",
            "source_type": "university",
            "source_date": "2025-11-03",
            "source_url": "https://example.edu/profile/hgd",
        },
        {
            "id": "2",
            "name": "홍길동 (前 한국은행 위원)",
            "affiliation": "前 한국은행",
            "title": "전 위원",
            "source_type": "news",
            "source_date": "2023-05-10",
            "source_url": "https://example.com/news/1",
        },
        {
            "id": "3",
            "name": "Jane Kim, PhD",
            "affiliation": "KDI",
            "title": "Senior Researcher",
            "source_type": "official",
            "source_date": "2026-01-20",
            "source_url": "https://example.org/experts/jane-kim",
        },
        {
            "id": "4",
            "name": "Dr. Jane Kim",
            "affiliation": "Korea Development Institute",
            "title": "former analyst",
            "source_type": "blog",
            "source_date": "2024-01-01",
            "source_url": "https://example.blog/post",
        },
    ]
    fieldnames = list(rows[0].keys())
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Expert dataset cleansing pipeline")
    parser.add_argument("--input", default="experts_raw.csv", help="Input CSV path")
    parser.add_argument("--outdir", default="output", help="Output directory")
    parser.add_argument(
        "--margin-threshold",
        type=float,
        default=0.08,
        help="Top-1 vs Top-2 score margin threshold for manual review",
    )
    parser.add_argument(
        "--make-sample",
        action="store_true",
        help="Create a sample input CSV and exit",
    )
    args = parser.parse_args()

    if args.make_sample:
        make_sample(args.input)
        print(f"[ok] sample file created: {args.input}")
        return

    if not os.path.exists(args.input):
        raise FileNotFoundError(
            f"Input not found: {args.input}\n"
            f"Run with --make-sample first, or provide a valid CSV."
        )

    today = dt.date.today()
    rows = read_csv_rows(args.input)
    if not rows:
        raise ValueError("Input CSV is empty.")

    scored = enrich_rows(rows, today)
    current_rows, history_rows, review_rows = resolve_conflicts(scored, args.margin_threshold)

    # 필드 구성
    all_keys = set()
    for group in (current_rows, history_rows, review_rows):
        for r in group:
            all_keys.update(r.keys())
    fieldnames = sorted(all_keys)

    current_path = os.path.join(args.outdir, "experts_current.csv")
    history_path = os.path.join(args.outdir, "experts_history.csv")
    review_path = os.path.join(args.outdir, "experts_review_queue.csv")

    write_csv(current_path, current_rows, fieldnames)
    write_csv(history_path, history_rows, fieldnames)
    write_csv(review_path, review_rows, fieldnames)

    print(f"[ok] current: {current_path} ({len(current_rows)} rows)")
    print(f"[ok] history: {history_path} ({len(history_rows)} rows)")
    print(f"[ok] review : {review_path} ({len(review_rows)} rows)")


if __name__ == "__main__":
    main()
