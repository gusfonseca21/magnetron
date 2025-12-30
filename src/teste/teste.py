import asyncio
from datetime import date, timedelta


def generate_urls(start_date: date, end_date: date) -> list[str]:
    # Documentação do endpoint diz que a dataInicio e dataFim só podem ser utilizadas se estiverem no mesmo ano.
    # Votações com dataInicio e dataFim com diferença maior que três meses retorna erro.
    current_start = start_date
    urls = []

    while current_start < end_date:
        current_end = current_start + timedelta(days=90)

        if current_end > end_date:
            current_end = end_date

        if current_end.year > current_start.year:
            current_end = date(current_start.year, 12, 31)

        urls.append(
            f"/votacoes?dataInicio={current_start}&dataFim={current_end}&itens=100"
        )

        current_start = current_end + timedelta(days=1)

    return urls


if __name__ == "__main__":
    print(generate_urls(start_date=date(2025, 12, 1), end_date=date(2026, 1, 12)))
