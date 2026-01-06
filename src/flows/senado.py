from datetime import date

from prefect import flow, get_run_logger, task
from prefect.futures import resolve_futures_to_results

from config.parameters import TasksNames
from tasks.extract.senado import extract_colegiados


@flow(
    name="Senado Flow",
    flow_run_name="senado_flow",
    description="Orquestramento de tasks do endpoint Senado.",
    log_prints=True,
)
def senado_flow(start_date: date, end_date: date, ignore_tasks: list[str]):
    logger = get_run_logger()
    logger.info("Iniciando execução da Flow do Senado")

    ## EXTRACT COLEGIADOS
    extract_senado_colegiados_f = None
    if TasksNames.EXTRACT_SENADO_COLEGIADOS not in ignore_tasks:
        extract_senado_colegiados_f = extract_colegiados.submit()


@task(
    name="Run Senado Flow",
    task_run_name="run_senado_flow",
    description="Task que permite executar o Flow do Senado de forma concorrente em relação às outras flows.",
)
def run_senado_flow(start_date: date, end_date: date, ignore_tasks: list[str]):
    senado_flow(start_date, end_date, ignore_tasks)
