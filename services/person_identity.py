from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(slots=True)
class GeminiSamePersonDeps:
    get_person_prop: Callable[[dict, str], str]
    gemini_json: Callable[[str], dict]
    print_fn: Callable[[str], None]


def gemini_verify_same_person(
    name: str,
    affiliation: str,
    role: str,
    candidates: list[dict],
    deps: GeminiSamePersonDeps,
) -> Optional[dict]:
    """후보 인물 목록 중 동일 인물을 Gemini로 판별. 없으면 None."""
    if not candidates:
        return None

    candidates_text = "\n".join(
        f"{i+1}. 이름={deps.get_person_prop(p, '이름')}, "
        f"소속={deps.get_person_prop(p, '소속')}, "
        f"직책={deps.get_person_prop(p, '직책')}"
        for i, p in enumerate(candidates)
    )

    result = deps.gemini_json(f"""아래 검색 인물이 기존 인물 목록 중 동일한 사람인지 판단하세요.

검색 인물: 이름={name}, 소속={affiliation}, 직책={role}

기존 인물 목록:
{candidates_text}

규칙:
- 이름 표기가 약간 달라도 (오탈자, 띄어쓰기, 영문/한글 혼용 등) 동일 인물이면 해당 번호 반환
- 소속·직책이 유사하면 동일 인물로 판단
- 확실하지 않으면 0 반환

JSON으로만 응답: {{"match": 번호, "confidence": 0~1, "reason": "판단 근거"}}""")

    if result and isinstance(result, dict):
        idx = result.get("match", 0)
        conf = float(result.get("confidence", 0.0) or 0.0)
        if idx and 1 <= idx <= len(candidates) and conf >= 0.70:
            matched = candidates[idx - 1]
            matched_name = deps.get_person_prop(matched, "이름")
            deps.print_fn(
                f"    → Gemini 동일 인물 판별: '{name}' ≡ '{matched_name}' "
                f"(conf={conf:.2f}, {result.get('reason', '')})"
            )
            return matched
    return None


def score_person_confidence(
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str,
    yt_conf: float,
    conflict_count: int,
    non_econ: bool,
    confidence_min: float,
    confidence_strict_min: float,
) -> tuple[float, str]:
    """인물 데이터 신뢰도 점수(0~1)와 상태값 반환."""
    score = 0.15
    if affiliation and affiliation not in ("정보 없음", "미상"):
        score += 0.12
    if role and role not in ("정보 없음", "미상"):
        score += 0.12
    if career and career not in ("정보 없음", "미상"):
        score += 0.13
    if expertise and expertise not in ("정보 없음", "미상"):
        score += 0.10

    src = (source_url or "").lower()
    if "youtube.com/" in src or "youtu.be/" in src:
        score += 0.18
    elif source_url:
        score += 0.08

    score += max(0.0, min(1.0, float(yt_conf or 0.0))) * 0.20
    if conflict_count > 0:
        score -= min(0.24, 0.12 * conflict_count)
    if non_econ:
        score -= 0.25

    score = max(0.0, min(1.0, score))
    if score >= confidence_min:
        status = "확정"
    elif score >= confidence_strict_min:
        status = "검토 필요"
    else:
        status = "보류"
    return score, status


@dataclass(slots=True)
class UncertainPersonDeps:
    is_missing_person_value: Callable[[str], bool]
    is_non_economic_profile: Callable[..., bool]
    needs_person_category_review: Callable[..., bool]
    is_likely_korean_fullname: Callable[[str], bool]
    find_conflicting_candidates: Callable[[object, str, str, str, str], list[str]]
    is_youtube_url: Callable[[str], bool]
    score_person_confidence: Callable[..., tuple[float, str]]
    person_confidence_min: float


def is_uncertain_person(
    notion,
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    source_url: str,
    opinion: str,
    deps: UncertainPersonDeps,
) -> tuple[bool, list[str], float, str]:
    reasons: list[str] = []
    soft_reasons: list[str] = []
    if any(token in (name or "") for token in [",", ";", "&", "|"]) or " and " in (name or ""):
        reasons.append("이름 필드에 다중 인물 표기 의심")
    if not source_url and (deps.is_missing_person_value(career) and deps.is_missing_person_value(expertise)):
        reasons.append("검색 근거 부족 또는 프로필 품질 낮음")
    if deps.is_missing_person_value(affiliation) and deps.is_missing_person_value(role):
        reasons.append("소속/직책 대부분 불명확")

    non_econ = deps.is_non_economic_profile(affiliation, role, career, expertise)
    if non_econ:
        reasons.append("경제와 무관한 직업/경력으로 동명이인 의심")
    if deps.needs_person_category_review(affiliation, role, career, expertise, opinion):
        reasons.append("허용 카테고리(경제/정치/국제정세/방송인/유튜버) 외 인물로 보여 검토 필요")
    if not deps.is_likely_korean_fullname(name):
        reasons.append("이름이 일반 한국식 이름 형식 아님")

    conflicts = deps.find_conflicting_candidates(notion, name, affiliation, role, opinion)
    if conflicts:
        reasons.append(f"유사 인물 후보 존재: {', '.join(conflicts)}")

    if deps.is_youtube_url(source_url):
        yt_conf = 0.90
    elif source_url:
        yt_conf = 0.45
    else:
        yt_conf = 0.0
        soft_reasons.append("YouTube/외부 근거 미확인")

    score, status = deps.score_person_confidence(
        affiliation,
        role,
        career,
        expertise,
        source_url,
        yt_conf,
        len(conflicts),
        non_econ,
    )
    if soft_reasons and (reasons or score < deps.person_confidence_min):
        reasons.extend(soft_reasons)
    if score < deps.person_confidence_min:
        reasons.append(f"신뢰도 점수 미달 ({score:.2f} < {deps.person_confidence_min:.2f})")
    return (len(reasons) > 0), reasons, score, status


@dataclass(slots=True)
class NonEconomicSuspectsDeps:
    get_person_prop: Callable[[dict, str], str]
    is_non_economic_profile: Callable[..., bool]
    needs_person_category_review: Callable[..., bool]


def non_economic_person_suspects(
    all_persons: list[dict],
    deps: NonEconomicSuspectsDeps,
) -> list[dict]:
    """비경제 또는 허용 카테고리 외 인물 의심 항목 반환."""
    suspects: list[dict] = []
    for p in all_persons:
        name = deps.get_person_prop(p, "이름")
        if not name:
            continue
        aff = deps.get_person_prop(p, "소속")
        role = deps.get_person_prop(p, "직책")
        career = deps.get_person_prop(p, "주요 경력")
        expertise = deps.get_person_prop(p, "전문 분야")
        latest_opinion = deps.get_person_prop(p, "최근 발언")
        reason = None
        if deps.is_non_economic_profile(aff, role, career, expertise, latest_opinion):
            reason = "비경제/동명이인 의심"
        elif deps.needs_person_category_review(aff, role, career, expertise, latest_opinion):
            reason = "허용 카테고리 외 검토 필요"

        if reason:
            suspects.append(
                {
                    "name": name,
                    "affiliation": aff,
                    "role": role,
                    "career": career,
                    "expertise": expertise,
                    "reason": reason,
                    "page_id": p.get("id", ""),
                }
            )
    return suspects
