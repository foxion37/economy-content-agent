from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AppRuntimeConfig:
    notion_database_id: str
    person_db_id: str
    google_sheet_id: str
    expert_sheet_id: str
    telegram_channel_id: str
    people_accumulate_mode: bool
    people_dawn_hour: int
    people_dawn_minute: int
    person_confidence_min: float
    person_confidence_strict_min: float

