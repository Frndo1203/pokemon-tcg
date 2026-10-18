# Fixtures

- `tcgdex_carta_pt.json`: resposta real de `GET /v2/pt/cards/swsh7-218` (2026-10-18).
- `pokeapi_especie_384.json`: resposta real de `GET /api/v2/pokemon-species/384` (2026-10-18).
- `myp_carta.json`: **sintética**, montada a partir do schema `Produto` do swagger oficial (sem token). `card_code`, `edition_code` e preços são inventados; trocar por uma resposta real no Step 6 da Tarefa 10.
- `ptax_periodo.json`: resposta real da PTAX EUR de 09/10 a 16/10/2026 (3 boletins por dia; sem cotação em 10, 11 e 12/10: fim de semana + feriado).
- `tcgdex_carta_ja.json`: resposta real de `GET /v2/ja/cards/M5-115` com `pricing.cardmarket`. Atenção: em `ja`, `rarity` e `types` vêm em inglês.
