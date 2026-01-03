from datetime import date, datetime, timedelta
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_results
from prefect.task_runners import ThreadPoolTaskRunner

from config.loader import load_config
from config.parameters import TasksNames
from tasks.extract import camara, senado
from tasks.extract.tse import TSE_ENDPOINTS, extract_tse

APP_SETTINGS = load_config()


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=APP_SETTINGS.FLOW.MAX_RUNNERS),  # type: ignore
    log_prints=True,
)
async def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [],
):
    logger = get_run_logger()
    logger.info("Iniciando pipeline")

    # TSE: ~30 endpoints em paralelo
    extract_tse_f = None
    if TasksNames.EXTRACT_TSE not in ignore_tasks:
        extract_tse_f = [
            cast(Any, extract_tse)
            .with_options(refresh_cache=refresh_cache)
            .submit(name, url)
            for name, url in TSE_ENDPOINTS.items()
        ]

    # CÂMARA DOS DEPUTADOS

    ## EXTRACT LEGISLATURA
    extract_camara_legislatura_f = None
    if TasksNames.EXTRACT_CAMARA_LEGISLATURA not in ignore_tasks:
        extract_camara_legislatura_f = camara.extract_legislatura(start_date)

    ## EXTRACT DEPUTADOS
    extract_camara_deputados_f = None
    if (
        extract_camara_legislatura_f
        and TasksNames.EXTRACT_CAMARA_DEPUTADOS not in ignore_tasks
    ):
        extract_camara_deputados_f = camara.extract_deputados.submit(
            extract_camara_legislatura_f
        )

    ## EXTRACT ASSIDUIDADE
    extract_camara_assiduidade_f = None
    if (
        extract_camara_deputados_f
        and TasksNames.EXTRACT_CAMARA_ASSIDUIDADE not in ignore_tasks
    ):
        resolve_futures_to_results(extract_camara_deputados_f)
        extract_camara_assiduidade_f = camara.extract_assiduidade_deputados.submit(
            cast(list[int], extract_camara_deputados_f), start_date, end_date
        )

    ## EXTRACT FRENTES
    extract_camara_frentes_f = None
    if (
        extract_camara_legislatura_f
        and TasksNames.EXTRACT_CAMARA_FRENTES not in ignore_tasks
    ):
        id_legislatura = extract_camara_legislatura_f["dados"][0]["id"]
        extract_camara_frentes_f = camara.extract_frentes.submit(id_legislatura)

    ## EXTRACT FRENTES MEMBROS
    extract_camara_frentes_membros_f = None
    if (
        extract_camara_frentes_f
        and TasksNames.EXTRACT_CAMARA_FRENTES_MEMBROS not in ignore_tasks
    ):
        extract_camara_frentes_membros_f = camara.extract_frentes_membros.submit(
            cast(Any, extract_camara_frentes_f)
        )

    ## EXTRACT DETALHES DEPUTADOOS
    extract_camara_detalhes_deputados_f = None
    if (
        extract_camara_frentes_membros_f
        and TasksNames.EXTRACT_CAMARA_DETALHES_DEPUTADOS not in ignore_tasks
    ):
        if extract_camara_frentes_membros_f:
            resolve_futures_to_results(extract_camara_frentes_membros_f)
        extract_camara_detalhes_deputados_f = camara.extract_detalhes_deputados.submit(
            cast(list[int], extract_camara_deputados_f)
        )

    ## EXTRACT DISCURSOS DEPUTADOS
    extract_camara_discursos_deputados_f = None
    if (
        extract_camara_deputados_f
        and TasksNames.EXTRACT_CAMARA_DISCURSOS_DEPUTADOS not in ignore_tasks
    ):
        if extract_camara_detalhes_deputados_f:
            resolve_futures_to_results(extract_camara_detalhes_deputados_f)
        extract_camara_discursos_deputados_f = (
            camara.extract_discursos_deputados.submit(
                cast(list[int], extract_camara_deputados_f), start_date, end_date
            )
        )

    ## EXTRACT PROPOSIÇÕES CÂMARA
    extract_camara_proposicoes_f = None
    if TasksNames.EXTRACT_CAMARA_PROPOSICOES not in ignore_tasks:
        if extract_camara_discursos_deputados_f:
            resolve_futures_to_results(extract_camara_discursos_deputados_f)
        extract_camara_proposicoes_f = camara.extract_proposicoes_camara.submit(
            start_date, end_date
        )

    ## EXTRACT DETALHES PROPOSIÇÕES CÂMARA
    extract_camara_detalhes_proposicoes_f = None
    if (
        extract_camara_proposicoes_f
        and TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES not in ignore_tasks
    ):
        if extract_camara_proposicoes_f:
            resolve_futures_to_results(extract_camara_proposicoes_f)
        extract_camara_detalhes_proposicoes_f = (
            camara.extract_detalhes_proposicoes_camara.submit(
                cast(list[int], extract_camara_proposicoes_f)
            )
        )

    ## EXTRACT AUTORES PROPOSIÇÕES CÂMARA
    extract_camara_autores_proposicoes_f = None
    if (
        extract_camara_proposicoes_f
        and TasksNames.EXTRACT_CAMARA_AUTORES_PROPOSICOES not in ignore_tasks
    ):
        if extract_camara_proposicoes_f:
            resolve_futures_to_results(extract_camara_proposicoes_f)
        extract_camara_autores_proposicoes_f = (
            camara.extract_autores_proposicoes_camara.submit(
                cast(list[int], extract_camara_proposicoes_f)
            )
        )

    ## EXTRACT VOTAÇÕES CÂMARA
    extract_camara_votacoes_f = None
    if TasksNames.EXTRACT_CAMARA_VOTACOES not in ignore_tasks:
        if extract_camara_autores_proposicoes_f:
            resolve_futures_to_results(extract_camara_autores_proposicoes_f)
        extract_camara_votacoes_f = camara.extract_votacoes_camara.submit(
            start_date, end_date
        )

    ## EXTRACT DETALHES VOTAÇÕES CÂMARA
    extract_camara_detalhes_votacoes_f = None
    if (
        extract_camara_votacoes_f
        and TasksNames.EXTRACT_CAMARA_DETALHES_VOTACOES not in ignore_tasks
    ):
        extract_camara_detalhes_votacoes_f = (
            camara.extract_detalhes_votacoes_camara.submit(
                cast(list[str], extract_camara_votacoes_f)
            )
        )

    ## EXTRACT ORIENTAÇÕES VOTAÇÕES CÂMARA
    extract_camara_orientacoes_votacoes_f = None
    if (
        extract_camara_votacoes_f
        and TasksNames.EXTRACT_CAMARA_ORIENTACOES_VOTACOES not in ignore_tasks
    ):
        if extract_camara_detalhes_votacoes_f:
            resolve_futures_to_results(extract_camara_detalhes_votacoes_f)
        extract_camara_orientacoes_votacoes_fs = (
            camara.extract_orientacoes_votacoes_camara.submit(
                cast(list[str], extract_camara_votacoes_f)
            )
        )

    ## EXTRACT VOTOS VOTAÇÕES CÂMARA
    extract_camara_votos_votacoes_f = None
    if (
        extract_camara_votacoes_f
        and TasksNames.EXTRACT_CAMARA_VOTOS_VOTACOES not in ignore_tasks
    ):
        if extract_camara_orientacoes_votacoes_f:
            resolve_futures_to_results(extract_camara_orientacoes_votacoes_f)
        extract_camara_votos_votacoes_f = camara.extract_votos_votacoes_camara.submit(
            cast(list[str], extract_camara_votacoes_f)
        )

    ## EXTRACT DESPESAS DEPUTADOS
    # BUGADO ""Parâmetro(s) inválido(s).""
    extract_camara_despesas_deputados_f = None
    if (
        extract_camara_legislatura_f
        and TasksNames.EXTRACT_CAMARA_DESPESAS_DEPUTADOS not in ignore_tasks
    ):
        resolve_futures_to_results(extract_camara_discursos_deputados_f)
        extract_camara_despesas_deputados_f = camara.extract_despesas_deputados.submit(
            cast(list[int], extract_camara_deputados_f),
            start_date,
            extract_camara_legislatura_f,
        )

    # SENADO

    ## EXTRACT COLEGIADOS
    extract_senado_colegiados_f = None
    if TasksNames.EXTRACT_SENADO_COLEGIADOS not in ignore_tasks:
        extract_senado_colegiados_f = senado.extract_colegiados.submit()

    return resolve_futures_to_results(
        {
            "extract_tse": extract_tse_f,
            "extract_camara_legislatura": extract_camara_legislatura_f,
            "extract_camara_deputados": extract_camara_deputados_f,
            "extract_camara_assiduidade": extract_camara_assiduidade_f,
            "extract_camara_frentes": extract_camara_frentes_f,
            "extract_camara_frentes_membros": extract_camara_frentes_membros_f,
            "extract_camara_detalhes_deputados": extract_camara_detalhes_deputados_f,
            "extract_camara_discursos_deputados": extract_camara_discursos_deputados_f,
            "extract_camara_proposicoes": extract_camara_proposicoes_f,
            "extract_camara_detalhes_proposicoes": extract_camara_detalhes_proposicoes_f,
            "extract_camara_autores_proposicoes": extract_camara_autores_proposicoes_f,
            "extract_camara_votacoes": extract_camara_votacoes_f,
            "extract_camara_detalhes_votacoes": extract_camara_detalhes_votacoes_f,
            "extract_camara_orientacoes_votacoes": extract_camara_orientacoes_votacoes_f,
            "votos_votacoes_camara_fs": extract_camara_votos_votacoes_f,
            "extract_camara_despesas_deputados": extract_camara_despesas_deputados_f,
        }
    )


if __name__ == "__main__":
    pipeline.serve(  # type: ignore
        name="deploy-1"
    )
