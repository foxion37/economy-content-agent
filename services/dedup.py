import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(slots=True)
class PersonDedupeDeps:
    get_person_prop: Callable[[dict, str], str]
    get_person_count: Callable[[dict], int]
    get_all_persons_from_db: Callable[[object], list[dict]]
    compact_identity_text: Callable[[str], str]
    normalize_identity_text: Callable[[str], str]
    normalize_opinion_text: Callable[[str], str]
    dedup_opinions: Callable[[list[dict]], list[dict]]
    parse_opinions_from_person_blocks: Callable[[list[dict]], list[dict]]
    list_person_blocks: Callable[[object, str], list[dict]]
    similarity: Callable[[str, str], float]
    jaccard: Callable[[set[str], set[str]], float]
    google_search: Callable[[str, int], str]
    gemini_json: Callable[[str], dict]
    best_value: Callable[..., str]
    analyze_opinion_consistency: Callable[[str, list[dict]], dict]
    build_person_body_blocks: Callable[[str, list[dict], dict[str, int], dict], list[dict]]
    clear_and_write_person_blocks: Callable[[object, str, str, list[dict]], None]
    invalidate_person_db_cache: Callable[[], None]
    person_name_aliases: dict[str, str]
    person_sync_sleep_sec: float
    logger: object
    print_fn: Callable[[str], None]


def person_fingerprint(notion, page: dict, deps: PersonDedupeDeps) -> dict:
    name = deps.get_person_prop(page, "이름")
    affiliation = deps.get_person_prop(page, "소속")
    role = deps.get_person_prop(page, "직책")

    channels: set[str] = set()
    opinions: set[str] = set()
    try:
        blocks = deps.list_person_blocks(notion, page["id"])
        for op in deps.dedup_opinions(deps.parse_opinions_from_person_blocks(blocks)):
            ch = deps.normalize_identity_text(op.get("channel", ""))
            tx = deps.normalize_opinion_text(op.get("text", ""))
            if ch:
                channels.add(ch)
            if tx:
                opinions.add(tx[:120])
    except Exception:
        pass

    return {
        "page": page,
        "name": name,
        "affiliation": affiliation,
        "role": role,
        "name_n": deps.compact_identity_text(name),
        "aff_n": deps.compact_identity_text(affiliation),
        "role_n": deps.compact_identity_text(role),
        "channels": channels,
        "opinions": opinions,
    }


def duplicate_score(a: dict, b: dict, deps: PersonDedupeDeps) -> tuple[float, dict]:
    name_sim = deps.similarity(a.get("name", ""), b.get("name", ""))
    aff_sim = deps.similarity(a.get("affiliation", ""), b.get("affiliation", ""))
    role_sim = deps.similarity(a.get("role", ""), b.get("role", ""))
    ch_sim = deps.jaccard(a.get("channels", set()), b.get("channels", set()))
    op_sim = deps.jaccard(a.get("opinions", set()), b.get("opinions", set()))

    score = (
        name_sim * 0.42
        + aff_sim * 0.23
        + role_sim * 0.13
        + ch_sim * 0.12
        + op_sim * 0.10
    )
    if name_sim >= 0.92 and (aff_sim >= 0.50 or role_sim >= 0.60):
        score += 0.05

    details = {
        "name_sim": round(name_sim, 3),
        "aff_sim": round(aff_sim, 3),
        "role_sim": round(role_sim, 3),
        "ch_sim": round(ch_sim, 3),
        "op_sim": round(op_sim, 3),
        "score": round(min(score, 1.0), 3),
    }
    return min(score, 1.0), details


def google_confirm_duplicate(a: dict, b: dict, deps: PersonDedupeDeps) -> tuple[bool, float, str]:
    q1 = f"{a.get('name','')} {a.get('affiliation','')} {a.get('role','')}"
    q2 = f"{b.get('name','')} {b.get('affiliation','')} {b.get('role','')}"
    snippets = deps.google_search(q1, num=3) + "\n" + deps.google_search(q2, num=3)
    if not snippets.strip():
        return False, 0.0, "검색 결과 부족"

    result = deps.gemini_json(f"""아래 두 인물이 동일인인지 판단하세요.

A: 이름={a.get("name","")}, 소속={a.get("affiliation","")}, 직책={a.get("role","")}
B: 이름={b.get("name","")}, 소속={b.get("affiliation","")}, 직책={b.get("role","")}

Google 검색 스니펫:
{snippets[:2500]}

JSON으로만 응답:
{{"same_person": true/false, "confidence": 0~1, "reason": "근거"}}""")

    if not isinstance(result, dict):
        return False, 0.0, "Gemini 판별 실패"
    same = bool(result.get("same_person", False))
    conf = float(result.get("confidence", 0.0) or 0.0)
    reason = str(result.get("reason", ""))[:300]
    return same, conf, reason


def cluster_groups_by_edges(nodes: list[dict], edges: list[tuple[int, int]]) -> list[list[dict]]:
    parent = list(range(len(nodes)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, j in edges:
        union(i, j)

    groups_map: dict[int, list[dict]] = {}
    for idx, node in enumerate(nodes):
        root = find(idx)
        groups_map.setdefault(root, []).append(node["page"])
    return [group for group in groups_map.values() if len(group) >= 2]


def find_duplicate_person_groups_hybrid(
    notion,
    all_persons: list[dict],
    deps: PersonDedupeDeps,
) -> tuple[list[list[dict]], list[dict]]:
    if len(all_persons) < 2:
        return [], []

    fps = [person_fingerprint(notion, p, deps) for p in all_persons]
    auto_edges: list[tuple[int, int]] = []
    review_cases: list[dict] = []

    for i in range(len(fps)):
        for j in range(i + 1, len(fps)):
            score, details = duplicate_score(fps[i], fps[j], deps)
            if score >= 0.86:
                auto_edges.append((i, j))
                continue
            if score < 0.70 and details.get("name_sim", 0) < 0.86:
                continue

            try:
                same, conf, reason = google_confirm_duplicate(fps[i], fps[j], deps)
            except Exception as exc:
                deps.logger.warning(
                    f"Google 중복 확인 실패 ({fps[i]['name']} vs {fps[j]['name']}): {exc}"
                )
                same, conf, reason = False, 0.0, "Google 검색 실패"
            if same and conf >= 0.72:
                auto_edges.append((i, j))
                continue

            review_cases.append({
                "i": i,
                "j": j,
                "a": {"name": fps[i]["name"], "affiliation": fps[i]["affiliation"], "role": fps[i]["role"]},
                "b": {"name": fps[j]["name"], "affiliation": fps[j]["affiliation"], "role": fps[j]["role"]},
                "score": round(score, 3),
                "details": details,
                "google_conf": round(conf, 3),
                "google_reason": reason,
            })

    groups = cluster_groups_by_edges(fps, auto_edges)
    return groups, review_cases


def merge_person_group(notion, group: list[dict], deps: PersonDedupeDeps) -> None:
    primary = max(group, key=lambda p: deps.get_person_count(p))
    duplicates = [p for p in group if p["id"] != primary["id"]]

    primary_name = deps.get_person_prop(primary, "이름")
    dup_names = [deps.get_person_prop(d, "이름") for d in duplicates]
    deps.print_fn(f"  병합: '{primary_name}' (주인공) ← {dup_names}")

    all_pages = [primary] + duplicates
    merged_props = {
        "소속": deps.best_value(*[deps.get_person_prop(p, "소속") for p in all_pages]),
        "직책": deps.best_value(*[deps.get_person_prop(p, "직책") for p in all_pages]),
        "주요 경력": deps.best_value(*[deps.get_person_prop(p, "주요 경력") for p in all_pages]),
        "전문 분야": deps.best_value(*[deps.get_person_prop(p, "전문 분야") for p in all_pages]),
    }
    total_count = sum(deps.get_person_count(p) for p in all_pages)

    all_opinions_raw: list[dict] = []
    for page in all_pages:
        try:
            blocks = deps.list_person_blocks(notion, page["id"])
            all_opinions_raw.extend(deps.parse_opinions_from_person_blocks(blocks))
        except Exception:
            pass
    all_opinions = deps.dedup_opinions(all_opinions_raw)
    all_opinions.sort(key=lambda o: o.get("date", ""))

    channel_counts: dict[str, int] = {}
    for op in all_opinions:
        ch = op.get("channel", "")
        if ch:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1

    consistency = deps.analyze_opinion_consistency(primary_name, all_opinions)

    def rt(text: str) -> list:
        return [{"text": {"content": str(text)[:2000]}}]

    try:
        notion.pages.update(
            page_id=primary["id"],
            properties={
                "소속": {"rich_text": rt(merged_props["소속"])},
                "직책": {"rich_text": rt(merged_props["직책"])},
                "주요 경력": {"rich_text": rt(merged_props["주요 경력"])},
                "전문 분야": {"rich_text": rt(merged_props["전문 분야"])},
                "등장 횟수": {"number": total_count},
            },
        )
    except Exception as exc:
        deps.print_fn(f"    ⚠️ 주인공 속성 업데이트 실패: {exc}")

    blocks = deps.build_person_body_blocks(primary_name, all_opinions, channel_counts, consistency)
    deps.clear_and_write_person_blocks(notion, primary["id"], primary_name, blocks)

    for dup in duplicates:
        try:
            notion.pages.update(page_id=dup["id"], archived=True)
            deps.print_fn(f"    → 보관 처리: '{deps.get_person_prop(dup, '이름')}' ({dup['id'][:8]}…)")
        except Exception as exc:
            deps.print_fn(f"    ⚠️ 보관 실패 ({dup['id'][:8]}…): {exc}")
    deps.invalidate_person_db_cache()


def auto_dedup_people_db(notion, deps: PersonDedupeDeps) -> dict:
    all_persons = deps.get_all_persons_from_db(notion)
    groups, review_cases = find_duplicate_person_groups_hybrid(notion, all_persons, deps)
    merged_count = 0
    for group in groups:
        try:
            merge_person_group(notion, group, deps)
            merged_count += max(0, len(group) - 1)
            time.sleep(deps.person_sync_sleep_sec)
        except Exception as exc:
            deps.logger.error(f"자동 중복 병합 실패: {exc}", exc_info=True)

    refreshed = deps.get_all_persons_from_db(notion)
    by_name: dict[str, list[dict]] = {}
    for person in refreshed:
        name = deps.get_person_prop(person, "이름")
        key = deps.compact_identity_text(
            deps.person_name_aliases.get(deps.normalize_identity_text(name), name)
        )
        if not key:
            continue
        by_name.setdefault(key, []).append(person)

    forced_groups = 0
    for _, group in by_name.items():
        if len(group) < 2:
            continue
        try:
            anchor = group[0]
            anchor_fp = person_fingerprint(notion, anchor, deps)
            safe_group = [anchor]
            for cand in group[1:]:
                cand_fp = person_fingerprint(notion, cand, deps)
                score, details = duplicate_score(anchor_fp, cand_fp, deps)
                aff_anchor = deps.get_person_prop(anchor, "소속")
                aff_cand = deps.get_person_prop(cand, "소속")
                aff_same = (
                    deps.compact_identity_text(aff_anchor) == deps.compact_identity_text(aff_cand)
                    and bool(deps.compact_identity_text(aff_anchor))
                )
                if score >= 0.86 or aff_same:
                    safe_group.append(cand)
                else:
                    deps.logger.info(
                        f"이름기반 강제 병합 보류: {deps.get_person_prop(anchor, '이름')} "
                        f"score={score:.2f} details={details}"
                    )
            if len(safe_group) < 2:
                continue
            merge_person_group(notion, safe_group, deps)
            merged_count += max(0, len(safe_group) - 1)
            forced_groups += 1
            time.sleep(deps.person_sync_sleep_sec)
        except Exception as exc:
            deps.logger.error(f"이름기반 강제 병합 실패: {exc}", exc_info=True)

    return {
        "total": len(all_persons),
        "groups": len(groups),
        "forced_groups": forced_groups,
        "merged_pages": merged_count,
        "review_cases": len(review_cases),
    }
