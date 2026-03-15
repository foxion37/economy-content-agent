import signal
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(slots=True)
class RetryWorkerDeps:
    logger: object
    interval_min: int
    retry_once: Callable[[], dict]


def run_retry_worker(deps: RetryWorkerDeps) -> None:
    shutdown = threading.Event()

    def _request_shutdown(*_args) -> None:
        shutdown.set()

    try:
        if threading.current_thread() is not threading.main_thread():
            raise ValueError("signal handlers require main thread")
        signal.signal(signal.SIGTERM, _request_shutdown)
        signal.signal(signal.SIGINT, _request_shutdown)
    except ValueError:
        deps.logger.info("🔁 retry worker: 비주 스레드 실행으로 signal handler 등록 생략")

    deps.logger.info(f"🔁 전용 retry worker 시작 ({deps.interval_min}분)")
    while not shutdown.is_set():
        try:
            stats = deps.retry_once()
            if stats.get("claimed", 0) > 0 or stats.get("requeued", 0) > 0:
                deps.logger.info(f"🔁 retry worker 처리 결과: {stats}")
        except Exception as exc:
            deps.logger.error(f"retry worker 오류: {exc}", exc_info=True)
        shutdown.wait(timeout=max(deps.interval_min, 1) * 60)
    deps.logger.info("🔁 retry worker 종료")
