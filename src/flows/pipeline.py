from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

from config.loader import load_config

from .tse import run_tse_flow

APP_SETTINGS = load_config()


@flow(
    name="Pipeline Flow",
    flow_run_name="pipeline_flow",
    task_runner=ThreadPoolTaskRunner(max_workers=APP_SETTINGS.FLOW.MAX_RUNNERS),  # type: ignore
    description="Onde os outros Flows são chamados e coordenados.",
    log_prints=True,
)
def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [],
):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline ETL")

    run_tse_flow.submit(start_date, refresh_cache, ignore_tasks)
