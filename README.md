uv run prefect deployment run 'pipeline/deploy-1' -p ignore_tasks='["extract_camara_despesas_deputados", "extract_tse", "extract_camara_deputados", "extract_camara_assiduidade", "extract_camara_frentes", "extract_camara_frentes_membros", "extract_camara_detalhes_deputados", "extract_camara_discursos_deputados", "extract_camara_proposicoes", "extract_camara_detalhes_proposicoes", "extract_camara_autores_proposicoes"]' -p start_date='2025-12-01' -p end_date='2026-01-01'


# uv run prefect deployment run 'pipeline/deploy-1' -p ignore_tasks='["extract_camara_despesas_deputados"]'
