import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional


def _sheet_end_col(headers: list[str]) -> str:
    idx = max(len(headers), 1)
    chars: list[str] = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        chars.append(chr(ord("A") + rem))
    return "".join(reversed(chars))


def _sheet_full_range(tab: str, headers: list[str]) -> str:
    return f"'{tab}'!A:{_sheet_end_col(headers)}"


@dataclass(slots=True)
class PeopleVerificationDeps:
    lookup_person_from_expert_sheet_by_channel: Callable[[str], Optional[dict]]
    compact_identity_text: Callable[[str], str]
    sanitize_person_fields: Callable[[str, str, str], tuple[str, str, str]]
    google_search: Callable[[str], str]
    gemini_json: Callable[[str], dict]
    print_fn: Callable[[str], None]


def verify_person(
    name: str,
    affiliation: str,
    role: str,
    channel: str,
    deps: PeopleVerificationDeps,
) -> tuple[str, str, str]:
    name_unknown = not name or name in ("미상", "정보 없음")
    aff_unknown = not affiliation or affiliation in ("미상", "정보 없음")
    if channel and (name_unknown or aff_unknown):
        sheet = deps.lookup_person_from_expert_sheet_by_channel(channel)
        if sheet:
            n = sheet["name"] if name_unknown else name
            a = sheet["affiliation"] if aff_unknown else affiliation
            r = sheet["role"] if (not role or role == "미상") else role
            return deps.sanitize_person_fields(n, a, r)

    if name_unknown or aff_unknown:
        return deps.sanitize_person_fields(name or "미상", affiliation or "정보 없음", role or "미상")

    if channel:
        sheet = deps.lookup_person_from_expert_sheet_by_channel(channel)
        if sheet and deps.compact_identity_text(sheet["name"]) == deps.compact_identity_text(name):
            a = sheet["affiliation"] or affiliation
            r = sheet["role"] or role
            return deps.sanitize_person_fields(name, a, r)

    try:
        snippets = deps.google_search(f"{name} {affiliation} 경제 애널리스트")
        if not snippets:
            return deps.sanitize_person_fields(name, affiliation, role)

        verified = deps.gemini_json(f"""검색 결과를 참고해 이름, 소속, 직책을 검증하고 교정하세요.

입력된 이름: {name}
입력된 소속: {affiliation}
입력된 직책: {role}
검색 결과: {snippets[:1000]}

JSON으로만 응답:
{{"name": "검증된 이름", "affiliation": "검증된 소속", "role": "검증된 직책"}}""")

        if verified:
            return deps.sanitize_person_fields(
                verified.get("name", name),
                verified.get("affiliation", affiliation),
                verified.get("role", role),
            )
    except Exception as exc:
        deps.print_fn(f"  ⚠️  인물 검증 스킵 ({exc})")

    return deps.sanitize_person_fields(name, affiliation, role)


@dataclass(slots=True)
class ExpertSheetSaveDeps:
    expert_sheet_tab: str
    expert_sheet_id: str
    expert_sheet_headers: list[str]
    ensure_sheet_tab: Callable[[str], None]
    get_sheets_service: Callable[[], object]
    person_sheet_key: Callable[[str, str], tuple[str, str]]
    extract_person_body_sheet_fields: Callable[[object, str], dict]
    sync_person_summary_props: Callable[..., None]
    get_person_prop: Callable[[dict, str], str]
    nickname_value: Callable[[str], str]
    delete_sheet_rows: Callable[[str, str, list[int]], None]
    verify_write: bool
    logger: object
    print_fn: Callable[[str], None]


def save_person_to_expert_sheet(
    name: str,
    affiliation: str,
    role: str,
    career: str,
    expertise: str,
    appearance_count: int,
    deps: ExpertSheetSaveDeps,
    notion=None,
    person_page_id: Optional[str] = None,
    source_url: str = "",
    body_fields_override: Optional[dict] = None,
    confidence_score: str = "",
    confidence_status: str = "",
) -> None:
    tab = deps.expert_sheet_tab
    full_range = _sheet_full_range(tab, deps.expert_sheet_headers)
    row_end_col = _sheet_end_col(deps.expert_sheet_headers)
    deps.print_fn(f"  [SHEETS] save_person_to_expert_sheet 시작: name={name!r}, sheet_id={deps.expert_sheet_id}, tab={tab}")
    try:
        deps.ensure_sheet_tab(tab)
        service = deps.get_sheets_service()
        sheet = service.spreadsheets()
        invalid_rows: list[int] = []

        get_resp = sheet.values().get(
            spreadsheetId=deps.expert_sheet_id,
            range=full_range,
        ).execute()
        values = get_resp.get("values", [])
        deps.print_fn(f"  [SHEETS] 현재 '{tab}' 탭 행 수: {len(values)} (헤더 포함)")

        if not values:
            upd_resp = sheet.values().update(
                spreadsheetId=deps.expert_sheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                body={"values": [deps.expert_sheet_headers]},
            ).execute()
            deps.print_fn(f"  [SHEETS] 헤더 삽입 응답: updatedCells={upd_resp.get('updatedCells')}, range={upd_resp.get('updatedRange')}")
            key_row_map: dict[tuple[str, str], int] = {}
            duplicate_rows: list[int] = []
        else:
            key_row_map = {}
            duplicate_rows = []
            for i, row in enumerate(values[1:], start=2):
                if not row:
                    continue
                row_name = row[0] if len(row) > 0 else ""
                row_affiliation = row[1] if len(row) > 1 else ""
                key = deps.person_sheet_key(row_name, row_affiliation)
                if not key[0]:
                    invalid_rows.append(i)
                    continue
                if key in key_row_map:
                    duplicate_rows.append(i)
                else:
                    key_row_map[key] = i
            deps.print_fn(f"  [SHEETS] 기존 인물 키 수: {len(key_row_map)}")

        body_fields = body_fields_override or {
            "latest_date": "",
            "latest_channel": "",
            "latest_opinion": "",
            "dominant_channel": "",
            "top_channels": "",
            "consistency_summary": "",
        }
        if notion and person_page_id:
            if body_fields_override is None:
                body_fields = deps.extract_person_body_sheet_fields(notion, person_page_id)
            if not source_url or not confidence_score or not confidence_status:
                try:
                    saved_page = notion.pages.retrieve(page_id=person_page_id)
                    if not source_url:
                        source_url = deps.get_person_prop(saved_page, "근거 링크")
                    if not confidence_score:
                        confidence_score = deps.get_person_prop(saved_page, "신뢰도 점수")
                    if not confidence_status:
                        confidence_status = deps.get_person_prop(saved_page, "신뢰도 상태")
                except Exception:
                    pass
            deps.sync_person_summary_props(
                notion,
                person_page_id,
                name,
                body_fields,
                source_url=source_url,
                confidence_score=confidence_score,
                confidence_status=confidence_status,
            )

        new_row = [
            name, affiliation, role, career, expertise, appearance_count,
            body_fields["latest_date"],
            body_fields["latest_channel"],
            body_fields["latest_opinion"],
            body_fields["dominant_channel"],
            body_fields["top_channels"],
            body_fields["consistency_summary"],
            source_url,
            confidence_score,
            confidence_status,
            deps.nickname_value(name),
        ]
        deps.print_fn(f"  [SHEETS] 저장할 행: {new_row}")
        target_key = deps.person_sheet_key(name, affiliation)

        if not target_key[0]:
            deps.print_fn("  [SHEETS] 빈 이름 행 저장 스킵")
            deps.delete_sheet_rows(deps.expert_sheet_id, tab, invalid_rows)
            return

        if target_key in key_row_map:
            row_num = key_row_map[target_key]
            upd_resp = sheet.values().update(
                spreadsheetId=deps.expert_sheet_id,
                range=f"'{tab}'!A{row_num}:{row_end_col}{row_num}",
                valueInputOption="RAW",
                body={"values": [new_row]},
            ).execute()
            deps.print_fn(f"  [SHEETS] update 응답: updatedCells={upd_resp.get('updatedCells')}, range={upd_resp.get('updatedRange')}")
        else:
            app_resp = sheet.values().append(
                spreadsheetId=deps.expert_sheet_id,
                range=full_range,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [new_row]},
            ).execute()
            upd = app_resp.get("updates", {})
            deps.print_fn(f"  [SHEETS] append 응답: updatedCells={upd.get('updatedCells')}, updatedRange={upd.get('updatedRange')}")

        deps.delete_sheet_rows(deps.expert_sheet_id, tab, invalid_rows)
        deps.delete_sheet_rows(deps.expert_sheet_id, tab, duplicate_rows)

        if deps.verify_write:
            verify_resp = sheet.values().get(
                spreadsheetId=deps.expert_sheet_id,
                range=full_range,
            ).execute()
            verify_values = verify_resp.get("values", [])
            names_after = [row[0] for row in verify_values[1:] if row]
            if name in names_after:
                deps.print_fn(f"  [SHEETS] ✓ 재조회 확인: '{name}' 기록됨 (총 {len(names_after)}명)")
            else:
                deps.print_fn(f"  [SHEETS] ❌ 재조회 실패: '{name}' 없음. 현재 목록={names_after}")

    except Exception as exc:
        deps.logger.error(f"[SHEETS] Economic_Expert 기록 실패 ({name}): {exc}", exc_info=True)


@dataclass(slots=True)
class ProcessPersonDeps:
    kst: object
    sanitize_person_fields: Callable[[str, str, str], tuple[str, str, str]]
    person_db_lock: object
    find_person_in_notion_db: Callable[..., Optional[dict]]
    update_existing_person_record: Callable[..., tuple]
    remember_person_match: Callable[[str, str, str], None]
    forget_person_match: Callable[[str], None]
    is_missing_person_value: Callable[[str], bool]
    collect_person_info_from_search: Callable[[str, str, str], dict]
    google_search_items: Callable[[str, int], list]
    pick_best_source_link: Callable[[list, str, str], str]
    notify_uncertain_person_with_ticket: Callable[[str, str, str, str, list[str]], None]
    create_person_in_notion_db: Callable[..., Optional[str]]
    is_uncertain_person: Callable[..., tuple]
    is_person_review_approved: Callable[[str, str, str], bool]
    person_uncertain_action: str
    remove_person_from_expert_sheet: Callable[[str, str], None]
    update_person_page_body: Callable[..., dict]
    backfill_person_profile: Callable[[object, str, str, str, str, str, str], None]
    set_person_source_url_if_missing: Callable[[object, str, str], None]
    save_person_to_expert_sheet: Callable[..., None]
    logger: object
    print_fn: Callable[[str], None]
    needs_manual_person_input: Optional[Callable[[str, str, str, str], bool]] = field(default=None)
    warn_manual_person_input_needed: Optional[Callable[[str, str, str, str, str], None]] = field(default=None)


@dataclass(slots=True)
class UpsertPersonResult:
    page_id: Optional[str]
    affiliation: str
    role: str
    career: str
    expertise: str
    source_url: str
    new_count: int
    created_new: bool
    aborted: bool = False


def _upsert_person_core(
    notion,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    channel: str,
    deps,
    log_prefix: str,
) -> UpsertPersonResult:
    logger = getattr(deps, "logger", None)
    print_fn = getattr(deps, "print_fn", print)

    page_id: Optional[str] = None
    career = "정보 없음"
    expertise = "정보 없음"
    source_url = ""
    new_count = 1
    created_new = False
    initial_existing = None

    try:
        with deps.person_db_lock:
            initial_existing = deps.find_person_in_notion_db(
                notion, name, affiliation, role, channel=channel, opinion=opinion
            )
    except Exception as exc:
        if logger:
            logger.error(f"{log_prefix} 인물 DB 검색 예외: {exc}", exc_info=True)
        initial_existing = None

    if initial_existing:
        with deps.person_db_lock:
            page_id, affiliation, role, career, expertise, source_url, new_count = deps.update_existing_person_record(
                notion, initial_existing, affiliation, role
            )
            deps.remember_person_match(name, affiliation, page_id)
        if (
            deps.is_missing_person_value(affiliation)
            or deps.is_missing_person_value(role)
            or deps.is_missing_person_value(career)
        ):
            try:
                info = deps.collect_person_info_from_search(name, affiliation, role, channel=channel)
                affiliation = info.get("affiliation", affiliation)
                role = info.get("role", role)
                career = info.get("career", career)
                expertise = info.get("expertise", expertise)
                source_url = source_url or info.get("source_url", "")
            except Exception as exc:
                if logger:
                    logger.error(f"{log_prefix} 기존 인물 정보 보강 실패: {exc}", exc_info=True)
        if not source_url:
            items = deps.google_search_items(f"{name} {affiliation} {role} 경제 애널리스트", num=3)
            source_url = deps.pick_best_source_link(items, name, affiliation)
        needs_backfill = (
            deps.is_missing_person_value(career)
            or deps.is_missing_person_value(expertise)
            or not source_url
            or deps.is_missing_person_value(affiliation)
            or deps.is_missing_person_value(role)
        )
        if needs_backfill:
            try:
                deps.backfill_person_profile(
                    notion, page_id, affiliation, role, career, expertise, source_url
                )
            except Exception as exc:
                if logger:
                    logger.error(f"{log_prefix} 기존 인물 프로필 backfill 실패: {exc}", exc_info=True)
        return UpsertPersonResult(page_id, affiliation, role, career, expertise, source_url, new_count, created_new, False)

    print_fn(f"{log_prefix} 신규 인물 — Google Search로 정보 수집 중...")
    try:
        person_info = deps.collect_person_info_from_search(name, affiliation, role, channel=channel)
        affiliation = person_info.get("affiliation", affiliation)
        role = person_info.get("role", role)
        career = person_info.get("career", "정보 없음")
        expertise = person_info.get("expertise", "정보 없음")
        source_url = person_info.get("source_url", "")
        homonym_suspected = bool(person_info.get("homonym_suspected"))
        print_fn(f"{log_prefix} 수집된 career={career[:60]!r}")
    except Exception as exc:
        if logger:
            logger.error(f"{log_prefix} Google Search 수집 실패: {exc}", exc_info=True)
        homonym_suspected = False

    with deps.person_db_lock:
        try:
            rechecked = deps.find_person_in_notion_db(
                notion, name, affiliation, role, channel=channel, opinion=opinion
            )
        except Exception as exc:
            if logger:
                logger.error(f"{log_prefix} 재대조 검색 예외: {exc}", exc_info=True)
            rechecked = None

        if rechecked:
            page_id, affiliation, role, career_saved, expertise_saved, source_saved, new_count = deps.update_existing_person_record(
                notion, rechecked, affiliation, role
            )
            deps.remember_person_match(name, affiliation, page_id)
            if not deps.is_missing_person_value(career_saved):
                career = career_saved
            if not deps.is_missing_person_value(expertise_saved):
                expertise = expertise_saved
            if source_saved:
                source_url = source_saved
            print_fn(f"{log_prefix} 재대조로 기존 인물 매칭 (등장 횟수: {new_count})")
        else:
            if homonym_suspected:
                deps.notify_uncertain_person_with_ticket(
                    "", name, affiliation, role,
                    ["동명이인 의심(비경제 직업 탐지) - 수동 확인 필요"]
                )
                return UpsertPersonResult(None, affiliation, role, career, expertise, source_url, new_count, False, True)
            if callable(deps.needs_manual_person_input) and callable(deps.warn_manual_person_input_needed):
                if deps.needs_manual_person_input(affiliation, role, career, expertise):
                    deps.warn_manual_person_input_needed(name, affiliation, role, career, expertise)
            try:
                page_id = deps.create_person_in_notion_db(
                    notion, name, affiliation, role, career, expertise, source_url=source_url
                )
                created_new = bool(page_id)
                deps.remember_person_match(name, affiliation, page_id or "")
            except Exception as exc:
                if logger:
                    logger.error(f"{log_prefix} 노션 페이지 생성 예외: {exc}", exc_info=True)
            if not page_id:
                if logger:
                    logger.error(f"{log_prefix} 노션 인물 페이지 생성 실패")
                return UpsertPersonResult(None, affiliation, role, career, expertise, source_url, new_count, created_new, True)
            print_fn(f"{log_prefix} 노션 인물 DB 생성 완료")

    return UpsertPersonResult(page_id, affiliation, role, career, expertise, source_url, new_count, created_new, False)


def process_person_db(
    notion,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    channel: str,
    deps: ProcessPersonDeps,
) -> Optional[str]:
    name, affiliation, role = deps.sanitize_person_fields(name, affiliation, role)

    if not name or name in ("미상", "정보 없음"):
        deps.print_fn("  [PERSON_DB] 스킵 (이름 미상)")
        return None

    deps.print_fn(f"  [PERSON_DB] 처리 시작: name={name!r}, affiliation={affiliation!r}, role={role!r}")
    date_str = datetime.now(deps.kst).strftime("%Y.%m.%d")
    upserted = _upsert_person_core(
        notion, name, affiliation, role, opinion, channel, deps, "  [PERSON_DB]"
    )
    page_id = upserted.page_id
    affiliation = upserted.affiliation
    role = upserted.role
    career = upserted.career
    expertise = upserted.expertise
    source_url = upserted.source_url
    new_count = upserted.new_count
    created_new = upserted.created_new
    if upserted.aborted:
        return None

    uncertain, reasons, conf_score, conf_status = deps.is_uncertain_person(
        notion, name, affiliation, role, career, expertise, source_url, opinion
    )
    pending_review = False
    if uncertain:
        if deps.is_person_review_approved(name, affiliation, role):
            deps.print_fn(f"  [PERSON_DB] ✅ 기존 승인 이력으로 재질문 생략: {name}")
        else:
            pending_review = True
            deps.print_fn(f"  [PERSON_DB] ⚠️ 불확실 인물(score={conf_score:.2f}, status={conf_status}): {reasons}")
            if deps.person_uncertain_action == "delete" and page_id:
                try:
                    notion.pages.update(page_id=page_id, archived=True)
                    deps.print_fn(f"  [PERSON_DB] 보관(삭제) 처리: {name}")
                    deps.forget_person_match(page_id)
                except Exception:
                    pass
                deps.remove_person_from_expert_sheet(name, affiliation)
                return None
            if created_new and page_id:
                try:
                    notion.pages.update(page_id=page_id, archived=True)
                    deps.print_fn(f"  [PERSON_DB] 불확실 신규 인물 임시 보관: {name}")
                    deps.forget_person_match(page_id)
                except Exception:
                    pass
            deps.notify_uncertain_person_with_ticket(page_id or "", name, affiliation, role, reasons)
            if created_new:
                return None

    body_fields = None
    if page_id:
        try:
            body_fields = deps.update_person_page_body(notion, page_id, name, channel, opinion, date_str)
        except Exception as exc:
            deps.logger.error(f"  [PERSON_DB] 본문 업데이트 실패: {exc}", exc_info=True)
        if created_new:
            try:
                deps.backfill_person_profile(
                    notion, page_id, affiliation, role, career, expertise, source_url
                )
            except Exception as exc:
                deps.logger.error(f"  [PERSON_DB] 인물 프로필 backfill 실패: {exc}", exc_info=True)
        deps.set_person_source_url_if_missing(notion, page_id, source_url)

    deps.save_person_to_expert_sheet(
        name, affiliation, role, career, expertise, new_count,
        notion=notion, person_page_id=page_id, source_url=source_url,
        body_fields_override=body_fields,
        confidence_score=f"{conf_score:.2f}",
        confidence_status=conf_status,
    )
    if pending_review:
        deps.print_fn("  [PERSON_DB] ℹ️ 기존 인물은 기록을 유지하고 수동 검토만 추가 요청했습니다.")

    return page_id


@dataclass(slots=True)
class PeopleMaintenanceDeps:
    logger: object
    print_fn: Callable[[str], None]
    sync_people_from_notion: Callable[[], None]
    get_notion_client: Callable[[], object]
    people_purge_on_maintenance: bool
    people_accumulate_mode: bool
    purge_people_without_youtube_source: Callable[[object], dict]
    normalize_person_name_column: Callable[[object], None]
    people_rebuild_on_maintenance: bool
    rebuild_expert_sheet: Callable[[object], None]
    check_people_sync_status: Callable[[], None]
    queue_non_economic_people_review: Callable[[], None]
    enrich_missing_person_profiles: Callable[[object], None]
    auto_dedup_people_db: Callable[[object], dict]
    dedup_person_page_opinions: Callable[[object], None]


def run_people_maintenance_light(deps: PeopleMaintenanceDeps) -> None:
    deps.print_fn("\n👥 인물 데이터 경량 관리 시작...")
    deps.sync_people_from_notion()
    notion = deps.get_notion_client()

    if deps.people_purge_on_maintenance and not deps.people_accumulate_mode:
        purge_stats = deps.purge_people_without_youtube_source(notion)
        deps.print_fn(
            f"🧼 근거 유튜브 링크 기준 정리: total={purge_stats['total']}, "
            f"kept={purge_stats['kept']}, purged={purge_stats['purged']}"
        )
        if purge_stats["samples"]:
            deps.print_fn(f"   · 삭제 샘플: {purge_stats['samples']}")
    else:
        deps.print_fn("  ℹ️ 적립형 모드: 자동 purge 생략")

    deps.normalize_person_name_column(notion)
    if deps.people_rebuild_on_maintenance and not deps.people_accumulate_mode:
        deps.rebuild_expert_sheet(notion)
    else:
        deps.print_fn("  ℹ️ 적립형 모드: 전체 rebuild 생략 (개별 upsert 유지)")
    try:
        deps.check_people_sync_status()
    except Exception as exc:
        deps.logger.error(f"인물 DB 자동 검증 실패: {exc}", exc_info=True)
    deps.print_fn("👥 인물 데이터 경량 관리 완료\n")


def run_people_maintenance_once(deps: PeopleMaintenanceDeps) -> None:
    deps.print_fn("\n👥 인물 데이터 정밀 클렌징 시작...")
    run_people_maintenance_light(deps)
    notion = deps.get_notion_client()
    deps.enrich_missing_person_profiles(notion)
    dedup_stats = deps.auto_dedup_people_db(notion)
    deps.print_fn(
        f"👥 자동 중복 정리: groups={dedup_stats['groups']}, "
        f"forced_groups={dedup_stats.get('forced_groups', 0)}, "
        f"merged_pages={dedup_stats['merged_pages']}, review_pending={dedup_stats['review_cases']}"
    )
    deps.dedup_person_page_opinions(notion)
    if deps.people_rebuild_on_maintenance and not deps.people_accumulate_mode:
        deps.rebuild_expert_sheet(notion)
    try:
        deps.queue_non_economic_people_review()
    except Exception as exc:
        deps.logger.error(f"비경제/카테고리 검토 큐 적재 실패: {exc}", exc_info=True)
    deps.print_fn("👥 인물 데이터 정밀 클렌징 완료\n")


@dataclass(slots=True)
class RebuildPeopleDeps:
    get_notion_client: Callable[[], object]
    get_all_persons_from_db: Callable[[object], list[dict]]
    find_duplicate_person_groups_hybrid: Callable[[object, list[dict]], tuple[list[list[dict]], list[dict]]]
    get_person_prop: Callable[[dict, str], str]
    person_fingerprint: Callable[[object, dict], object]
    cluster_groups_by_edges: Callable[[list, list[tuple[int, int]]], list[list[dict]]]
    merge_person_group: Callable[[object, list[dict]], None]
    rebuild_expert_sheet: Callable[[object], None]
    person_sync_sleep_sec: float
    sys_module: object
    print_fn: Callable[[str], None]


def rebuild_people_db(deps: RebuildPeopleDeps) -> None:
    notion = deps.get_notion_client()

    deps.print_fn("\n📊 인물 DB 전체 조회 중...")
    all_persons = deps.get_all_persons_from_db(notion)
    total = len(all_persons)
    deps.print_fn(f"  → {total}명 발견\n")

    if total == 0:
        deps.print_fn("인물 DB가 비어있습니다.")
        return

    deps.print_fn("🔍 중복 인물 탐지 중 (교차검증 + Google 보강)...")
    duplicate_groups, review_cases = deps.find_duplicate_person_groups_hybrid(notion, all_persons)

    approved_review_edges: list[tuple[int, int]] = []
    if review_cases:
        deps.print_fn(f"  → 애매 케이스 {len(review_cases)}건 발견 (수동 확인 필요)")
        for idx, case in enumerate(review_cases, 1):
            a = case["a"]
            b = case["b"]
            deps.print_fn(
                f"    [{idx}] 후보: '{a['name']}'({a['affiliation']}/{a['role']}) "
                f"↔ '{b['name']}'({b['affiliation']}/{b['role']}) "
                f"| score={case['score']} | google={case['google_conf']}"
            )
            if case.get("google_reason"):
                deps.print_fn(f"         근거: {case['google_reason']}")

            if deps.sys_module.stdin.isatty():
                ans = input("         병합할까요? [y/N]: ").strip().lower()
                if ans in ("y", "yes"):
                    approved_review_edges.append((case["i"], case["j"]))
            else:
                deps.print_fn("         (비대화 실행: 자동 보류)")

    if approved_review_edges:
        fps = [deps.person_fingerprint(notion, person) for person in all_persons]
        manual_groups = deps.cluster_groups_by_edges(fps, approved_review_edges)
        duplicate_groups.extend(manual_groups)

    uniq: dict[tuple[str, ...], list[dict]] = {}
    for group in duplicate_groups:
        key = tuple(sorted(person["id"] for person in group))
        uniq[key] = group
    duplicate_groups = list(uniq.values())

    if duplicate_groups:
        deps.print_fn(f"  → {len(duplicate_groups)}개 중복 그룹 발견\n")
        merged_count = 0
        for i, group in enumerate(duplicate_groups, 1):
            names = [deps.get_person_prop(person, "이름") for person in group]
            deps.print_fn(f"  [{i}/{len(duplicate_groups)}] {names}")
            deps.merge_person_group(notion, group)
            merged_count += len(group) - 1
            time.sleep(max(deps.person_sync_sleep_sec, 0.1))
        deps.print_fn(f"\n✅ 중복 병합 완료 — {merged_count}개 중복 페이지 보관 처리")
    else:
        deps.print_fn("  → 중복 없음")

    deps.print_fn("\n📊 Economic_Expert 시트 재구성 중...")
    deps.rebuild_expert_sheet(notion)
    deps.print_fn("✅ 인물 DB 정리 완료")


@dataclass(slots=True)
class SyncPeopleDeps:
    get_notion_client: Callable[[], object]
    notion_database_id: str
    notion_api_key: str
    httpx_module: object
    get_rich_text: Callable[[dict], str]
    parse_person_str: Callable[[str], tuple[str, str, str]]
    resolve_content_person_relation_prop: Callable[[dict], str]
    get_person_prop: Callable[[dict, str], str]
    get_person_count: Callable[[dict], int]
    google_search_items: Callable[[str, int], list]
    pick_best_source_link: Callable[[list, str, str], str]
    set_person_source_url_if_missing: Callable[[object, str, str], None]
    save_person_to_expert_sheet: Callable[..., None]
    extract_video_id: Callable[[str], str]
    fetch_youtube_metadata: Callable[[str], dict]
    sync_person_and_link: Callable[..., Optional[str]]
    person_sync_sleep_sec: float
    kst: object
    datetime_module: object
    time_module: object
    print_fn: Callable[[str], None]


def sync_people_from_notion(deps: SyncPeopleDeps) -> None:
    notion = deps.get_notion_client()

    pages: list[dict] = []
    cursor = None
    while True:
        body: dict = {
            "filter": {
                "and": [
                    {"property": "URL", "url": {"is_not_empty": True}},
                    {"property": "한 줄 요약", "rich_text": {"is_not_empty": True}},
                ]
            },
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor
        resp = deps.httpx_module.post(
            f"https://api.notion.com/v1/databases/{deps.notion_database_id}/query",
            headers={
                "Authorization": f"Bearer {deps.notion_api_key}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    total = len(pages)
    deps.print_fn(f"\n📋 인물 DB 동기화 대상: {total}개 콘텐츠 페이지\n")
    if total == 0:
        deps.print_fn("동기화할 데이터가 없습니다.")
        return

    success = skip_unnamed = skip_already = fail = 0

    for i, page in enumerate(pages, 1):
        props = page.get("properties", {})
        content_page_id = page["id"]

        title_raw = (props.get("콘텐츠 제목", {}) or {}).get("title") or []
        video_title = title_raw[0]["text"]["content"] if title_raw else "(제목 없음)"
        person_str = deps.get_rich_text(props.get("출연자", {}))
        opinion = deps.get_rich_text(props.get("인물의견", {}))
        timestamp = deps.get_rich_text(props.get("처리일시", {}))
        url = props.get("URL", {}).get("url") or ""

        label = f"[{i}/{total}] {video_title[:40]}"
        name, affiliation, role = deps.parse_person_str(person_str)
        if not name or name in ("미상", "정보 없음"):
            skip_unnamed += 1
            deps.print_fn(f"{label} — 스킵 (인물 미상)")
            continue

        relation_prop = deps.resolve_content_person_relation_prop(props)
        existing_relation = props.get(relation_prop, {}).get("relation", []) if relation_prop else []
        if existing_relation:
            skip_already += 1
            person_page_id = existing_relation[0].get("id")
            if person_page_id:
                try:
                    person_page = notion.pages.retrieve(page_id=person_page_id)
                    p_name = deps.get_person_prop(person_page, "이름") or name
                    p_aff = deps.get_person_prop(person_page, "소속") or affiliation
                    p_role = deps.get_person_prop(person_page, "직책") or role
                    p_career = deps.get_person_prop(person_page, "주요 경력")
                    p_exp = deps.get_person_prop(person_page, "전문 분야")
                    p_cnt = deps.get_person_count(person_page)
                    p_src = deps.get_person_prop(person_page, "근거 링크")
                    if not p_src:
                        items = deps.google_search_items(f"{p_name} {p_aff} {p_role} 경제 애널리스트", num=3)
                        p_src = deps.pick_best_source_link(items, p_name, p_aff)
                        deps.set_person_source_url_if_missing(notion, person_page_id, p_src)

                    deps.save_person_to_expert_sheet(
                        p_name, p_aff, p_role, p_career, p_exp, p_cnt,
                        notion=notion, person_page_id=person_page_id, source_url=p_src,
                    )
                except Exception as exc:
                    deps.print_fn(f"{label} — 이미 연결됨(백필 실패): {exc}")
            deps.print_fn(f"{label} — 스킵 (이미 연결됨: {name}, 시트 백필 시도)")
            continue

        channel = ""
        video_id = deps.extract_video_id(url)
        if video_id:
            try:
                channel = deps.fetch_youtube_metadata(video_id).get("channel", "")
            except Exception:
                pass

        date_str = timestamp[:10].replace("-", ".") if timestamp else deps.datetime_module.now(deps.kst).strftime("%Y.%m.%d")

        deps.print_fn(f"{label} — {name} 처리 중...")
        try:
            person_page_id = deps.sync_person_and_link(
                notion, content_page_id, name, affiliation, role, opinion, channel, date_str
            )
            if person_page_id:
                success += 1
            else:
                fail += 1
        except Exception as exc:
            deps.print_fn(f"  ❌ 실패: {exc}")
            fail += 1

        deps.time_module.sleep(max(deps.person_sync_sleep_sec, 0.1))

    deps.print_fn(f"\n{'='*50}")
    deps.print_fn(f"✅ 인물 DB 동기화 완료 — 성공: {success} / 이미 연결: {skip_already} / 이름 없음: {skip_unnamed} / 실패: {fail}")
    deps.print_fn(f"{'='*50}")


@dataclass(slots=True)
class SyncPersonAndLinkDeps:
    sanitize_person_fields: Callable[[str, str, str], tuple[str, str, str]]
    person_db_lock: object
    find_person_in_notion_db: Callable[..., Optional[dict]]
    update_existing_person_record: Callable[..., tuple]
    remember_person_match: Callable[[str, str, str], None]
    forget_person_match: Callable[[str], None]
    google_search_items: Callable[[str, int], list]
    pick_best_source_link: Callable[[list, str, str], str]
    collect_person_info_from_search: Callable[[str, str, str], dict]
    is_missing_person_value: Callable[[str], bool]
    notify_uncertain_person_with_ticket: Callable[[str, str, str, str, list[str]], None]
    needs_manual_person_input: Callable[[str, str, str, str], bool]
    warn_manual_person_input_needed: Callable[[str, str, str, str, str], None]
    create_person_in_notion_db: Callable[..., Optional[str]]
    is_uncertain_person: Callable[..., tuple]
    is_person_review_approved: Callable[[str, str, str], bool]
    person_uncertain_action: str
    remove_person_from_expert_sheet: Callable[[str, str], None]
    update_person_page_body: Callable[..., dict]
    backfill_person_profile: Callable[[object, str, str, str, str, str, str], None]
    set_person_source_url_if_missing: Callable[[object, str, str], None]
    resolve_content_person_relation_prop: Callable[[dict], str]
    content_person_relation_prop: str
    save_person_to_expert_sheet: Callable[..., None]
    logger: object
    print_fn: Callable[[str], None]


def sync_person_and_link(
    notion,
    content_page_id: str,
    name: str,
    affiliation: str,
    role: str,
    opinion: str,
    channel: str,
    date_str: str,
    deps: SyncPersonAndLinkDeps,
) -> Optional[str]:
    name, affiliation, role = deps.sanitize_person_fields(name, affiliation, role)
    upserted = _upsert_person_core(
        notion, name, affiliation, role, opinion, channel, deps, "    →"
    )
    person_page_id = upserted.page_id
    affiliation = upserted.affiliation
    role = upserted.role
    career = upserted.career
    expertise = upserted.expertise
    source_url = upserted.source_url
    new_count = upserted.new_count
    created_new = upserted.created_new
    if upserted.aborted or not person_page_id:
        return None
    if created_new:
        deps.print_fn("    → 노션 인물 DB 생성 완료")
    else:
        deps.print_fn(f"    → 기존/재대조 인물 반영 완료 (등장 횟수: {new_count})")

    uncertain, reasons, conf_score, conf_status = deps.is_uncertain_person(
        notion, name, affiliation, role, career, expertise, source_url, opinion
    )
    pending_review = False
    if uncertain:
        if deps.is_person_review_approved(name, affiliation, role):
            deps.print_fn(f"    ✅ 기존 승인 이력으로 재질문 생략: {name}")
        else:
            pending_review = True
            deps.print_fn(f"    ⚠️ 불확실 인물 보류(score={conf_score:.2f}, status={conf_status}): {reasons}")
            if deps.person_uncertain_action == "delete":
                try:
                    notion.pages.update(page_id=person_page_id, archived=True)
                    deps.forget_person_match(person_page_id)
                except Exception:
                    pass
                deps.remove_person_from_expert_sheet(name, affiliation)
                return None
            else:
                if created_new and person_page_id:
                    try:
                        notion.pages.update(page_id=person_page_id, archived=True)
                        deps.print_fn(f"    → 불확실 신규 인물 임시 보관: {name}")
                        deps.forget_person_match(person_page_id)
                    except Exception:
                        pass
                deps.notify_uncertain_person_with_ticket(person_page_id or "", name, affiliation, role, reasons)
                if created_new:
                    return None

    body_fields = None
    try:
        body_fields = deps.update_person_page_body(notion, person_page_id, name, channel, opinion, date_str)
    except Exception as exc:
        deps.logger.error(f"    ⚠️ 인물 본문 업데이트 실패: {exc}", exc_info=True)
    if created_new:
        try:
            deps.backfill_person_profile(
                notion, person_page_id, affiliation, role, career, expertise, source_url
            )
        except Exception as exc:
            deps.logger.error(f"    ⚠️ 인물 프로필 backfill 실패: {exc}", exc_info=True)
    deps.set_person_source_url_if_missing(notion, person_page_id, source_url)

    try:
        content_page = notion.pages.retrieve(page_id=content_page_id)
        content_props = content_page.get("properties", {})
        relation_prop = deps.resolve_content_person_relation_prop(content_props)
        candidates: list[str] = []
        if relation_prop:
            candidates.append(relation_prop)
        if deps.content_person_relation_prop and deps.content_person_relation_prop not in candidates:
            candidates.append(deps.content_person_relation_prop)

        if not candidates:
            deps.print_fn("    ℹ️ 콘텐츠 DB에 인물 relation 컬럼이 없어 연결을 건너뜁니다.")
            candidates = []

        last_err = None
        linked = False
        for cand in candidates:
            try:
                notion.pages.update(
                    page_id=content_page_id,
                    properties={cand: {"relation": [{"id": person_page_id}]}},
                )
                deps.print_fn(f"    ✓ 콘텐츠 DB ↔ 인물 DB 연결 완료 ({cand})")
                linked = True
                break
            except Exception as exc:
                last_err = exc
                continue

        if candidates and not linked:
            available_types = {k: v.get("type") for k, v in (content_props or {}).items()}
            raise ValueError(
                f"relation 연결 실패. candidates={candidates}, properties={available_types}, last_err={last_err}"
            )
    except Exception as exc:
        deps.print_fn(f"    ⚠️ Relation 연결 실패: {exc}")

    deps.save_person_to_expert_sheet(
        name, affiliation, role, career, expertise, new_count,
        notion=notion,
        person_page_id=person_page_id,
        source_url=source_url,
        body_fields_override=body_fields,
        confidence_score=f"{conf_score:.2f}",
        confidence_status=conf_status,
    )
    if pending_review:
        deps.print_fn("    ℹ️ 기존 인물은 relation 유지, 수동 검토만 추가로 요청했습니다.")

    return person_page_id
