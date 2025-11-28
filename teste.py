from datetime import datetime, date
from typing import Literal



LegislaturaProps = Literal["id", "leg_start_date", "leg_end_date"]

def get_legislatura_data(legislatura_dict: dict, property: LegislaturaProps) -> int | date:
    leg_data = legislatura_dict.get("dados", None)
    if not leg_data:
        raise ValueError(f"Não foram encontrados dados sobre Legislatura no objeto passado: {leg_data}")
    
    prop_data = leg_data.get(property, None)
    if not prop_data:
        raise ValueError(f"A propriedade '{property}' não existe dentro de Legislatura. Propriedades disponíveis: {prop_data.keys}")
    
    if property == "id":
        try:
            return int(prop_data)
        except ValueError:
            raise ValueError(f"O valor de '{property}' ('{prop_data}') não é conversível para um número inteiro.")
    else:
        date_format = "%Y-%m-%d"
        try:
            return datetime.strptime(str(prop_data), date_format).date()
        except ValueError:
            raise ValueError(f"O valor de '{property}' ('{prop_data}') não é conversível para um objeto de data.")

if __name__ == "__main__":

    value = get_legislatura_data()