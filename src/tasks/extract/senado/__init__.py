from .colegiados import extract_colegiados
from .detalhes_senadores import extract_detalhes_senadores
from .discursos_senadores import extract_discursos_senadores
from .senadores import extract_senadores

__all__ = [
    "extract_colegiados",
    "extract_senadores",
    "extract_detalhes_senadores",
    "extract_discursos_senadores",
]
