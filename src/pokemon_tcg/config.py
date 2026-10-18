"""Caminhos e variáveis de ambiente do projeto.

Lê só `os.environ`. O `.env` entra no ambiente por fora: `uv run --env-file .env`
(Makefile) e `env_file` (docker-compose). Assim este módulo não depende de libs
extras e os testes controlam o ambiente com `monkeypatch`.
"""

import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DIR_DADOS = Path(os.environ.get("DIR_DADOS", RAIZ / "dados"))

CAMINHO_DUCKDB = DIR_DADOS / "warehouse.duckdb"
DIR_BRUTO = DIR_DADOS / "bruto"
DIR_QUARENTENA = DIR_DADOS / "quarentena"

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://tcg:tcg@localhost:5432/tcg"
)
MYP_API_TOKEN = os.environ.get("MYP_API_TOKEN") or None

USER_AGENT = "pokemon-tcg-pessoal/0.1 (+oliveirafer06@gmail.com)"
LIGA_INTERVALO_S = 360  # robots.txt da Liga: Crawl-delay 360
LIMIAR_DIAS_PRECO_VELHO = 7
