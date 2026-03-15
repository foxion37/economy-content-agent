import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Callable, Optional


@dataclass(slots=True)
class PersonLookupDeps:
    person_name_aliases: dict[str, str]
    recent_match_store: dict[str, tuple[str, float]]
    person_match_cooldown_sec: int
    normalize_identity_text: Callable[[str], str]
    compact_identity_text: Callable[[str], str]
    similarity: Callable[[str, str], float]
    normalize_opinion_text: Callable[[str], str]
    extract_prop_text: Callable[[dict], str]
    sanitize_person_fields: Callable[[str, str, str], tuple[str, str, str]]
    query_person_db_exact: Callable[[object, str], Optional[dict]]
    get_all_persons_from_db: Callable[[object], list[dict]]
    get_person_prop: Callable[[dict, str], str]
    gemini_verify_same_person: Callable[[str, str, str, list[dict]], Optional[dict]]
    list_person_blocks: Callable[[object, str], list[dict]]
    parse_opinions_from_person_blocks: Callable[[list[dict]], list[dict]]
    dedup_opinions: Callable[[list[dict]], list[dict]]
    now_ts: Callable[[], float]


def person_name_key(name: str, deps: PersonLookupDeps) -> str:
    return deps.compact_identity_text(
        deps.person_name_aliases.get(deps.normalize_identity_text(name), name)
    )


def person_aff_key(affiliation: str, deps: PersonLookupDeps) -> str:
    a = deps.compact_identity_text(affiliation)
    a = re.sub(r"(주식회사|유한회사|inc|co|corp|ltd)$", "", a)
    return a


def person_lookup_keys(name: str, affiliation: str, deps: PersonLookupDeps) -> list[str]:
    nk = person_name_key(name, deps)
    ak = person_aff_key(affiliation, deps)
    keys = []
    if nk and ak:
        keys.append(f"{nk}|{ak}")
    if nk:
        keys.append(nk)
    return keys


def remember_person_match(name: str, affiliation: str, page_id: str, deps: PersonLookupDeps) -> None:
    if not page_id:
        return
    now = deps.now_ts()
    for key in person_lookup_keys(name, affiliation, deps):
        deps.recent_match_store[key] = (page_id, now)


def forget_person_match(page_id: str, deps: PersonLookupDeps) -> None:
    if not page_id:
        return
    stale_keys = [k for k, v in deps.recent_match_store.items() if v and v[0] == page_id]
    for key in stale_keys:
        deps.recent_match_store.pop(key, None)


def find_recent_person_match(
    all_persons: list[dict],
    name: str,
    affiliation: str,
    deps: PersonLookupDeps,
) -> Optional[dict]:
    now = deps.now_ts()
    id_map = {p.get("id"): p for p in all_persons}
    for key in person_lookup_keys(name, affiliation, deps):
        value = deps.recent_match_store.get(key)
        if not value:
            continue
        page_id, ts = value
        if now - ts > deps.person_match_cooldown_sec:
            continue
        person = id_map.get(page_id)
        if person:
            return person
        deps.recent_match_store.pop(key, None)
    return None


def candidate_hint_score(
    cand_name: str,
    cand_aff: str,
    channel: str,
    opinion: str,
    cand_props: dict,
    deps: PersonLookupDeps,
) -> float:
    score = 0.0
    c_name = person_name_key(cand_name, deps)
    c_aff = person_aff_key(cand_aff, deps)
    ch = deps.compact_identity_text(channel)
    op = deps.normalize_opinion_text(opinion)

    latest_channel = deps.compact_identity_text(deps.extract_prop_text(cand_props.get("최근 채널", {})))
    top_channels = deps.compact_identity_text(deps.extract_prop_text(cand_props.get("채널 TOP3", {})))
    latest_opinion = deps.normalize_opinion_text(deps.extract_prop_text(cand_props.get("최근 발언", {})))

    if c_name and ch and c_name in ch:
        score += 0.35
    if ch and latest_channel and (ch in latest_channel or latest_channel in ch):
        score += 0.22
    if ch and top_channels and ch in top_channels:
        score += 0.18
    if op and latest_opinion:
        if SequenceMatcher(None, op, latest_opinion).ratio() >= 0.86:
            score += 0.22
    if c_aff and ch and c_aff in ch:
        score += 0.15
    return score


def find_conflicting_candidates(
    notion,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    deps: PersonLookupDeps,
) -> list[str]:
    out: list[str] = []
    target_name = deps.compact_identity_text(name)
    target_aff = deps.normalize_identity_text(affiliation)
    target_role = deps.normalize_identity_text(role)
    target_op = deps.normalize_opinion_text(opinion)

    block_fetches = 0
    for person in deps.get_all_persons_from_db(notion):
        cand_name = deps.get_person_prop(person, "이름")
        if not cand_name:
            continue
        if deps.compact_identity_text(cand_name) == target_name:
            continue
        aff2 = deps.normalize_identity_text(deps.get_person_prop(person, "소속"))
        role2 = deps.normalize_identity_text(deps.get_person_prop(person, "직책"))
        aff_sim = deps.similarity(target_aff, aff2)
        role_sim = deps.similarity(target_role, role2)
        if aff_sim < 0.74 and role_sim < 0.82:
            continue
        latest = ""
        # Notion block 조회는 비용이 커서 유사 후보 중 일부만 본문 의견까지 확장 확인한다.
        if block_fetches < 3:
            try:
                blocks = deps.list_person_blocks(notion, person["id"])
                ops = deps.dedup_opinions(deps.parse_opinions_from_person_blocks(blocks))
                latest = deps.normalize_opinion_text(ops[-1].get("text", "")) if ops else ""
                block_fetches += 1
            except Exception:
                latest = ""
        op_sim = SequenceMatcher(None, target_op, latest).ratio() if target_op and latest else 0.0
        if op_sim >= 0.78 or (aff_sim >= 0.84 and role_sim >= 0.84):
            out.append(cand_name)
    return out[:5]


def find_person_in_notion_db(
    notion,
    name: str,
    affiliation: str,
    role: str,
    channel: str,
    opinion: str,
    deps: PersonLookupDeps,
) -> Optional[dict]:
    name, affiliation, role = deps.sanitize_person_fields(name, affiliation, role)

    all_persons = deps.get_all_persons_from_db(notion)
    recent = find_recent_person_match(all_persons, name, affiliation, deps)
    if recent:
        return recent

    exact = deps.query_person_db_exact(notion, name)
    if exact:
        remember_person_match(name, affiliation, exact.get("id", ""), deps)
        return exact

    name_key = person_name_key(name, deps)
    aff_key = person_aff_key(affiliation, deps)

    best: Optional[dict] = None
    best_score = 0.0
    gemini_ranked: list[tuple[float, dict]] = []
    for person in all_persons:
        cand_name = deps.get_person_prop(person, "이름")
        cand_aff = deps.get_person_prop(person, "소속")
        cand_role = deps.get_person_prop(person, "직책")
        cand_props = person.get("properties", {})
        cand_name_key = person_name_key(cand_name, deps)
        cand_aff_key = person_aff_key(cand_aff, deps)

        if name_key and cand_name_key == name_key:
            if aff_key and cand_aff_key and deps.similarity(cand_aff_key, aff_key) >= 0.72:
                remember_person_match(name, affiliation, person["id"], deps)
                return person

        short_name_case = len(name_key) == 2 and len(cand_name_key) >= 3 and cand_name_key.startswith(name_key)
        if short_name_case:
            hint = candidate_hint_score(cand_name, cand_aff, channel, opinion, cand_props, deps)
            if hint >= 0.45 or deps.similarity(cand_aff, affiliation) >= 0.88:
                remember_person_match(name, affiliation, person["id"], deps)
                return person

        name_sim = deps.similarity(cand_name, name)
        aff_sim = deps.similarity(cand_aff, affiliation)
        role_sim = deps.similarity(cand_role, role)
        hint = candidate_hint_score(cand_name, cand_aff, channel, opinion, cand_props, deps)
        score = (name_sim * 0.55) + (aff_sim * 0.20) + (role_sim * 0.10) + (hint * 0.25)
        if score > best_score:
            best_score = score
            best = person
        if (
            name_sim >= 0.45
            or aff_sim >= 0.55
            or role_sim >= 0.60
            or hint >= 0.45
            or (name_key and cand_name_key and name_key in cand_name_key)
        ):
            gemini_ranked.append((score, person))

    if best and best_score >= 0.82:
        remember_person_match(name, affiliation, best["id"], deps)
        return best

    gemini_candidates = [person for _, person in sorted(gemini_ranked, key=lambda x: x[0], reverse=True)[:20]]
    matched = deps.gemini_verify_same_person(name, affiliation, role, gemini_candidates)
    if matched:
        remember_person_match(name, affiliation, matched["id"], deps)
    return matched
