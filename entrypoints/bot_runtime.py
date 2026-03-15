import asyncio
from typing import Awaitable, Callable

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)


def build_bot_application(
    token: str,
    start_command: Callable,
    person_decision_callback: Callable,
    unknown_decision_callback: Callable,
    person_fix_command: Callable,
    person_form_command: Callable,
    person_testbed_command: Callable,
    person_merge_command: Callable,
    person_skip_command: Callable,
    handle_message: Callable,
) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(person_decision_callback, pattern=r"^person_decide:"))
    app.add_handler(CallbackQueryHandler(unknown_decision_callback, pattern=r"^unknown_decide:"))
    app.add_handler(CommandHandler("person_fix", person_fix_command))
    app.add_handler(CommandHandler("person_form", person_form_command))
    app.add_handler(CommandHandler("person_testbed", person_testbed_command))
    app.add_handler(CommandHandler("person_merge", person_merge_command))
    app.add_handler(CommandHandler("person_skip", person_skip_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return app


async def run_bot_runtime(app: Application, *background_tasks: Awaitable) -> None:
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    try:
        await asyncio.gather(*background_tasks)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
