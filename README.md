# pokemon-tcg

Inventário, cartas faltantes por coleção e preços em BRL das minhas cartas Pokémon TCG
(lendários e tipo Dragão, em português). Projeto pessoal de aprendizado, roda só local.

- Requisitos: `docs/superpowers/specs/2026-10-17-pokemon-tcg-requisitos-design.md`
- Plano da fase 1: `docs/superpowers/plans/2026-10-17-fase-1-fundacao-de-dados.md`
- Decisões: `docs/adr/` · Padrões de engenharia de dados: `docs/padroes/`

## Pré-requisitos

- [uv](https://docs.astral.sh/uv/)
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/)
- `cp .env.example .env` e preencher

## Ligar / Desligar

```sh
make up     # sobe postgres, prefect-server (http://localhost:4200), servidor-fluxos, evidence (http://localhost:3000)
make down   # derruba tudo; os dados ficam no volume pg_dados e em dados/
```

Depois do `make down`, feche o Docker Desktop para liberar toda a memória.

## Testes

```sh
make test                  # uv run pytest
uv run pytest -m "not e2e" # sem os testes de navegador
```
