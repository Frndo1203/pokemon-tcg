# Fase 1 — Fundação de Dados: Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Projeto de aprendizado.** Cada tarefa diz **quem escreve**. Em tarefas marcadas *Fernando*, o executor (humano ou agente) não escreve o código central: explica o conceito, entrega o esqueleto (assinaturas e testes deste plano), deixa Fernando implementar e revisa como mentor. Tarefas marcadas *Assistente* são boilerplate e podem ser escritas direto.
>
> **TDD com suíte completa primeiro.** A **Tarefa T** escreve a suíte de testes inteira da fase (pytest, unit tests e testes de auditoria do dbt, e2e do dashboard) contra stubs, e confirma que tudo falha pelo motivo certo. Nenhum código de produção é escrito antes disso. Nas Tarefas 1–14, os passos "Teste que falha" descrevem testes **já escritos na Tarefa T**: executar o passo = rodar e confirmar vermelho, sem reescrever. Teste só muda depois da Tarefa T se estiver errado (motivo registrado no commit), nunca para se ajustar ao código.

**Goal:** Inventário, catálogo, faltantes por coleção e preços BRL com histórico próprio, visíveis num dashboard local que liga e desliga com um comando.

**Architecture:** Fontes (TCGdex com preços Cardmarket, PokeAPI, MYP, câmbio PTAX, inventário Postgres) entram por dlt numa camada `bruto` do DuckDB; dbt transforma em staging → intermediate → marts, com snapshot SCD2 de preços; Prefect orquestra ingestão → auditoria → publicação; Evidence lê os marts. Tudo sobe com `make up` e desce com `make down`.

**Tech Stack:** Python 3.12, uv, dlt, DuckDB, Postgres 16 (Docker), psycopg 3, dbt-core + dbt-duckdb (≥ 1.8, para unit tests), dbt-metricflow[duckdb], Prefect 3 + prefect-dbt, Evidence.dev, httpx, pytest, Docker Compose, Make.

**Spec:** `docs/superpowers/specs/2026-10-17-pokemon-tcg-requisitos-design.md`

## Global Constraints

- Tudo em português: nomes de tabelas, colunas, modelos dbt, módulos, mensagens, docs. Identificadores sem acento (`condicao`, `preco_medio`).
- Custo zero. Proibido: Spark, Kafka, nuvem, dbt Cloud, qualquer serviço pago.
- Toda requisição HTTP usa `User-Agent` identificável (`pokemon-tcg-pessoal/0.1 (+oliveirafer06@gmail.com)`); TCGdex com cache local. LigaPokémon fora da fase 1 (Cloudflare; ver ADR-0005).
- MYP: token só via variável `MYP_API_TOKEN` em `.env` (não versionado); atribuição visível "Preços: MYP Cards" com link `https://mypcards.com` em toda tela que mostra preço MYP.
- Segredos nunca em código nem em log.
- Identidade da carta = id TCGdex (`<set.id>-<localId>`, ex. `sv03.5-199`) = edição + número. Nunca só o número.
- Raridades-alvo: Dupla Rara (Double Rare) e acima; fora: Comum, Incomum, Rara, Rara Holo.
- Preço de referência: médio (`avg_price` MYP; `avg` Cardmarket × PTAX do dia ou do último dia útil anterior); mínimo e máximo como faixa (Cardmarket não tem máximo: faixa só com mínimo). Todo preço exibido mostra "preço visto em DD/MM".
- Commits: mensagens em português, estilo Conventional Commits, **sem** trailer `Co-Authored-By`.
- UI: toda construção/redesenho de interface passa pela skill Hallmark; pronta só após screenshots a 1440px e 390px checadas.
- TDD: nenhum código de produção antes da Tarefa T terminar com a suíte inteira vermelha; cada tarefa termina com seus testes verdes **e** `uv run pytest -m "not e2e"` sem regressão nos testes já verdes.

## Review Focus

1. **Carta no inventário que não existe no catálogo** (id digitado errado, edição nova ainda não carregada): CLI recusa com mensagem clara; dbt acusa em `copias_sem_catalogo`. Testes nas Tarefas 4 e 6.
2. **Cópia editada ou apagada depois da carga incremental**: a alteração aparece no DuckDB; apagar é soft delete (`removido_em`), nunca `DELETE`. Teste na Tarefa 5.
3. **Preço observado em fim de semana ou feriado** (sem PTAX no dia): usa a cotação do último dia útil anterior; sem nenhuma cotação anterior, o BRL fica nulo e um teste dbt avisa, sem inventar câmbio. Teste na Tarefa 11.
4. **Preço absurdo (R$ 0 ou 100× a mediana)**: auditoria bloqueia a publicação e os marts mantêm a versão anterior. Teste na Tarefa 11.
5. **DuckDB travado por outro processo** (CLI lendo enquanto o fluxo escreve): mensagem "pipeline rodando, tente de novo em instantes", sem stack trace. Teste na Tarefa 4.

---

## Estrutura de arquivos

```
pokemon-tcg/
  pyproject.toml  Makefile  docker-compose.yml  Dockerfile  .env.example  .gitignore  README.md
  src/pokemon_tcg/
    config.py                 # caminhos e variáveis de ambiente
    controle.py               # marcadores de prontidão (Readiness Marker)
    fontes/
      tcgdex.py               # fonte dlt do catálogo
      pokeapi.py              # fonte dlt de espécies
      myp.py                  # fonte dlt de preços MYP
      inventario.py           # fonte dlt incremental Postgres -> DuckDB
      cardmarket.py           # preços Cardmarket (EUR) do detalhe TCGdex, só cartas-alvo
      ptax.py                 # câmbio EUR/BRL do Banco Central
    inventario/
      schema.sql              # tabela copias no Postgres
      cli.py                  # comando `tcg`
    fluxos/
      ingestao.py  transformacao.py  precos.py  frescor.py  metricas.py  servir.py
  dbt/
    dbt_project.yml  profiles.yml
    seeds/ raridades_alvo.csv  de_para_edicoes_myp.csv
    models/staging/  models/intermediate/  models/marts/  models/semantic/
    snapshots/snp_precos.sql
  evidence/  (projeto Evidence: pages/index.md, fichario.md, venda.md, precos.md)
  docs/adr/  docs/padroes/
  dados/  (não versionado) warehouse.duckdb, bruto/
  tests/ fixtures/  test_*.py
```

---

### Task 0: Esqueleto do repositório e liga/desliga

**Quem escreve:** Assistente. **Conceito:** nenhum padrão do livro; é a base que permite rodar tudo com um comando.

**Files:**
- Create: `pyproject.toml`, `Makefile`, `docker-compose.yml`, `Dockerfile`, `.env.example`, `.gitignore`, `src/pokemon_tcg/__init__.py`, `src/pokemon_tcg/config.py`, `tests/test_config.py`, `docs/adr/0000-modelo.md`, `docs/padroes/_modelo.md`, `README.md`

**Interfaces:**
- Produces: `config.CAMINHO_DUCKDB: Path` (`dados/warehouse.duckdb`), `config.DIR_BRUTO: Path` (`dados/bruto`), `config.POSTGRES_DSN: str` (de `POSTGRES_DSN`), `config.MYP_API_TOKEN: str | None`, `config.USER_AGENT: str`, `config.LIMIAR_DIAS_PRECO_VELHO: int = 7`.
- `make up` sobe `postgres`, `prefect-server`, `servidor-fluxos`, `evidence`; `make down` derruba tudo; `make test` roda `uv run pytest`.

- [ ] **Step 1:** `git init`; `uv init --package --name pokemon-tcg --python 3.12`; adicionar dependências do Tech Stack.
- [ ] **Step 2: Teste que falha** — `tests/test_config.py`:

```python
from pokemon_tcg import config

def test_user_agent_identificavel():
    assert "@" in config.USER_AGENT

def test_token_myp_nao_tem_valor_padrao(monkeypatch):
    monkeypatch.delenv("MYP_API_TOKEN", raising=False)
    import importlib; importlib.reload(config)
    assert config.MYP_API_TOKEN is None
```

- [ ] **Step 3:** `uv run pytest tests/test_config.py -v` → FAIL (`ModuleNotFoundError` ou atributo ausente).
- [ ] **Step 4:** Implementar `config.py` lendo `.env` com `os.environ` (sem libs extras). `.gitignore` inclui `.env`, `dados/`, `dbt/target/`, `evidence/.evidence/`.
- [ ] **Step 5:** `uv run pytest -v` → PASS.
- [ ] **Step 6:** Compose com volumes `./dados` e `./dbt` montados no `servidor-fluxos`; Postgres com volume nomeado `pg_dados`. Verificar: `make up` → `docker compose ps` mostra 4 serviços `running` (o `servidor-fluxos` pode reiniciar até a Task 12 existir; aceitável). `make down` → `docker compose ps` vazio.
- [ ] **Step 7:** README com "Ligar / Desligar" (inclui: feche o Docker Desktop para liberar toda a memória). Modelos de ADR (contexto, opções, decisão, porquê, consequências) e de página de padrão (o que é, problema, onde está no repo, alternativa não usada, capítulo do livro).
- [ ] **Step 8:** Commit `chore: esqueleto do projeto com liga/desliga`.

---

### Task T: Suíte de testes completa (vermelha)

**Quem escreve:** Fernando escreve os testes. Assistente escreve os stubs e a infraestrutura do `conftest.py`. **Conceito:** escrever todos os testes antes do código transforma o plano num contrato executável. Os stubs fixam as interfaces (nomes, parâmetros, tipos) sem comportamento, então cada teste falha por falta de comportamento, não por erro de import. Ver a falha prova que o teste consegue pegar o defeito.

**Files:**
- Create: stubs de todos os módulos de `src/pokemon_tcg/` listados na estrutura de arquivos; stubs de todos os modelos dbt, seeds (só cabeçalho) e `snp_precos.sql`; `tests/conftest.py`; todos os `tests/test_*.py` das Tarefas 1–13; `tests/e2e/test_dashboard.py`; arquivos `.yml` de unit tests dbt e `dbt/tests/auditoria_*.sql`; todas as fixtures em `tests/fixtures/`.

**Interfaces:**
- Consumes: os blocos **Interfaces** de todas as tarefas abaixo. São a fonte única das assinaturas.
- Produces: suíte completa; stubs Python com a assinatura exata e corpo `raise NotImplementedError`; stubs dbt com as colunas do contrato e `where false`, para que o projeto compile e os unit tests falhem na asserção.

- [ ] **Step 1: Spikes e fixtures** (puxados das tarefas, porque os testes dependem deles):
  - Tarefa 1, Step 1: spike do GraphQL TCGdex + `tests/fixtures/tcgdex_carta_pt.json`.
  - Tarefa 2: `tests/fixtures/pokeapi_especie_384.json`.
  - Tarefa 7: `tests/fixtures/ptax_periodo.json` (resposta real de uma semana).
  - Tarefa 8: `tests/fixtures/tcgdex_carta_ja.json` (carta `ja` real com `pricing.cardmarket`).
  - Tarefa 10: `tests/fixtures/myp_carta.json` montada a partir do schema `Produto` do swagger oficial (sem token ainda).
  - Tarefa 13, Step 1: spike do `mf query` com dbt-duckdb.
- [ ] **Step 2: Stubs Python** de todas as assinaturas das Tarefas 1–13 (inclui exceções `CartaNaoEncontrada`, `WarehouseOcupado`, `FonteNaoPronta` e as dataclasses `ResumoCardmarket`, `ResumoMyp`). Exceções e dataclasses são definidas de verdade, porque são contrato, não comportamento.
- [ ] **Step 3: Stubs dbt** de todos os modelos, seeds e snapshot das Tarefas 3, 6, 11, 13. Verificar: `uv run dbt parse --project-dir dbt` → sem erro.
- [ ] **Step 4: `conftest.py`:** fixtures `conn` (banco `tcg_teste` no Postgres do compose, recriado por teste), `catalogo_fake`, `duckdb_travado`, `pipeline` (dlt com DuckDB em `tmp_path`), `fake_dbt`, `precos_de_10_dias_atras`, as fixtures de HTML/JSON, e os helpers `contar`, `valor`, `dias`, `alvo`, `fake_tcgdex`, `fake_ptax`, `fake_fontes`. Marcador `e2e` registrado em `pyproject.toml`.
- [ ] **Step 5: Testes pytest** de todas as tarefas, exatamente como descritos nos passos "Teste que falha" das Tarefas 1, 2, 4, 5, 7, 8, 10, 12, 13, incluindo os da **Review Focus**.
- [ ] **Step 6: Testes dbt:** unit tests das Tarefas 3, 6, 11 e testes de auditoria da Tarefa 11 (tag `auditoria_precos`), mais os testes de dados (`unique`, `not_null`, `relationships`, `copias_sem_catalogo` com `severity: warn`).
- [ ] **Step 7: Teste e2e do dashboard** (`tests/e2e/test_dashboard.py`, `pytest-playwright`, marcador `e2e`): para cada página (`/`, `/fichario`, `/venda`, `/precos`), a 1440px e 390px: carrega sem erro no console; sem rolagem horizontal (`document.documentElement.scrollWidth <= window.innerWidth`); `/` e `/fichario` contêm "faltam"; `/venda` contém "preço visto em" e "Cardmarket" (e "Preços: MYP Cards" quando houver preço MYP).
- [ ] **Step 8: Para cada teste, uma linha de comentário** dizendo que mudança no código de produção o faria falhar. Teste sem resposta clara é reescrito ou removido.
- [ ] **Step 9: Verificar vermelho:**
  - `uv run pytest -m "not e2e" -q` → **0 errors** de coleta; todos os testes das Tarefas 1–13 falham com `NotImplementedError` ou `AssertionError`; só `tests/test_config.py` (Tarefa 0) passa.
  - `uv run dbt build --project-dir dbt --select test_type:unit` → projeto compila; unit tests falham na asserção.
  - Falha por `ImportError`, `NameError` ou erro de compilação dbt = stub ou teste errado: corrigir antes de seguir.
- [ ] **Step 10:** Revisão de mentor da suíte (o que está bom, o que melhorar, testes que testam mock em vez de comportamento). Commit `test: suíte completa da fase 1 (vermelha)`.

**Pergunta de checagem:** um teste que passa logo depois de escrito, contra um stub que só levanta `NotImplementedError`, está dizendo o quê?

---

### Task 1: Catálogo TCGdex (Full Loader)

**Quem escreve:** Fernando. **Conceito — Full Loader:** recarregar o conjunto inteiro a cada execução e substituir o anterior (`write_disposition="replace"`). Serve quando a fonte é pequena e não informa o que mudou. Alternativa: incremental, que exigiria um cursor que o TCGdex não oferece.

**Files:**
- Create: `src/pokemon_tcg/fontes/tcgdex.py`, `tests/test_tcgdex.py`, `tests/fixtures/tcgdex_carta_pt.json`, `docs/adr/0001-dlt-para-ingestao.md`, `docs/adr/0002-duckdb-como-warehouse.md`, `docs/padroes/full-loader.md`

**Interfaces:**
- Produces: `normalizar_carta(bruta: dict, idioma: str) -> dict` com chaves `carta_id, idioma, set_id, set_nome, numero, nome, raridade, tipos (list[str]), dex_ids (list[int]), imagem_url`; `fonte_tcgdex(idiomas: tuple[str, ...] = ("pt", "en", "ja"))` (fonte dlt, recurso `tcgdex_cartas`, `replace`); tabela DuckDB `bruto.tcgdex_cartas`.

- [ ] **Step 1 (feito na Tarefa T): Spike (15 min):** confirmar que o GraphQL `https://api.tcgdex.net/v2/graphql` devolve em lote, com paginação, os campos `id localId name rarity types dexId image set{id name}` para `pt`. Confirmar também que o **mesmo id** (ex. `sv03.5-199`) existe em `pt` e `en`. Anotar o resultado na ADR-0001. Se o GraphQL não servir, usar REST por set (`/v2/{lang}/sets/{id}`) + detalhe por carta com cache em `dados/bruto/tcgdex/`.
- [ ] **Step 2 (feito na Tarefa T):** Salvar uma carta real `pt` em `tests/fixtures/tcgdex_carta_pt.json` (um Dragão, ex. Rayquaza).
- [ ] **Step 3: Teste que falha:**

```python
def test_normaliza_carta_pt(carta_pt):
    c = normalizar_carta(carta_pt, "pt")
    assert c["carta_id"] == f'{c["set_id"]}-{c["numero"]}'
    assert c["idioma"] == "pt"
    assert "Dragão" in c["tipos"]
    assert isinstance(c["dex_ids"], list) and all(isinstance(d, int) for d in c["dex_ids"])

def test_carta_sem_dex_id_vira_lista_vazia(carta_pt):
    carta_pt.pop("dexId", None)
    assert normalizar_carta(carta_pt, "pt")["dex_ids"] == []
```

- [ ] **Step 4:** `uv run pytest tests/test_tcgdex.py -v` → FAIL.
- [ ] **Step 5:** Implementar `normalizar_carta` e `fonte_tcgdex` com `@dlt.source` / `@dlt.resource(write_disposition="replace", primary_key=("carta_id", "idioma"))`, destino `dlt.destinations.duckdb(config.CAMINHO_DUCKDB)`, dataset `bruto`.
- [ ] **Step 6:** `uv run pytest tests/test_tcgdex.py -v` → PASS.
- [ ] **Step 7:** Rodar a carga real. Verificar: `duckdb dados/warehouse.duckdb "select idioma, count(*) from bruto.tcgdex_cartas group by 1"` → três linhas (`pt`, `en`, `ja`), cada uma > 0. Rodar de novo e confirmar contagens iguais (replace, sem duplicar).
- [ ] **Step 8:** Gerar a lista de raridades distintas: `select distinct idioma, raridade from bruto.tcgdex_cartas where idioma in ('pt','en') order by 1,2` → salvar como base da seed na Task 3.
- [ ] **Step 9:** Escrever ADR-0001 (dlt vs Airbyte), ADR-0002 (DuckDB), página `full-loader.md`. Commit `feat: ingestão do catálogo TCGdex (full loader)`.

**Pergunta de checagem:** se o TCGdex corrigir a raridade de uma carta, o que acontece com o valor antigo no `bruto`? Por que isso é aceitável aqui e não seria nos preços?

---

### Task 2: Espécies PokeAPI (lendários e míticos)

**Quem escreve:** Fernando. **Conceito:** mesmo Full Loader, fonte quase imutável; carga única com cache em disco. Prepara o **Static Joiner** (Task 3): juntar a um conjunto de referência que muda raramente.

**Files:**
- Create: `src/pokemon_tcg/fontes/pokeapi.py`, `tests/test_pokeapi.py`, `tests/fixtures/pokeapi_especie_384.json`

**Interfaces:**
- Produces: `buscar_especie(dex_id: int, dir_cache: Path) -> dict` (JSON cru, lê do cache se existir); `normalizar_especie(bruta: dict) -> dict` com `dex_id: int, nome: str, is_lendario: bool, is_mitico: bool`; fonte dlt `fonte_pokeapi()` → tabela `bruto.pokeapi_especies` (`replace`). Respostas HTTP cacheadas em `dados/bruto/pokeapi/{id}.json`; nova execução não refaz requisições já em cache.

- [ ] **Step 1 (feito na Tarefa T):** Salvar fixture da espécie 384 (Rayquaza).
- [ ] **Step 2: Teste que falha:**

```python
def test_rayquaza_e_lendario(especie_384):
    e = normalizar_especie(especie_384)
    assert e == {"dex_id": 384, "nome": "rayquaza", "is_lendario": True, "is_mitico": False}

def test_usa_cache_sem_rede(tmp_path, monkeypatch, especie_384):
    # cache preenchido -> nenhuma chamada HTTP
    ...  # grava especie_384 em tmp_path/384.json, monkeypatch httpx.get para falhar
    assert buscar_especie(384, dir_cache=tmp_path)["id"] == 384
```

- [ ] **Step 3:** Rodar → FAIL. **Step 4:** Implementar `buscar_especie(dex_id: int, dir_cache: Path) -> dict` e `normalizar_especie`. Total de espécies via `GET /api/v2/pokemon-species?limit=1` (`count`).
- [ ] **Step 5:** Rodar → PASS. Carga real; verificar `select count(*) from bruto.pokeapi_especies where is_lendario or is_mitico` → entre 60 e 120.
- [ ] **Step 6:** Commit `feat: espécies PokeAPI com cache`.

---

### Task 3: dbt — catálogo enriquecido e coleções (Static Joiner)

**Quem escreve:** Fernando. **Conceito — Static Joiner:** enriquecer cada carta com dados de uma dimensão estável (lendário/mítico), via join no `dex_id`. Cartas com vários `dex_ids` (tag team) são lendárias se **qualquer** um for.

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/models/staging/_fontes.yml`, `stg_tcgdex__cartas.sql`, `stg_pokeapi__especies.sql`, `dbt/seeds/raridades_alvo.csv`, `dbt/models/intermediate/int_cartas_enriquecidas.sql`, `int_cartas_enriquecidas.yml`, `docs/padroes/static-joiner.md`, `docs/adr/0003-raridades-alvo-como-seed.md`

**Interfaces:**
- Consumes: `bruto.tcgdex_cartas`, `bruto.pokeapi_especies`.
- Produces: seed `raridades_alvo(raridade text, idioma text, is_alvo boolean)`; modelo `int_cartas_enriquecidas` com grão (`carta_id`, `idioma`) e colunas do catálogo + `is_dragao`, `is_lendario` (lendário ou mítico), `is_alvo`, `colecoes` (lista: `'Dragões'`, `'Lendários'`, ambos ou vazia; só preenchida quando `is_alvo`).

- [ ] **Step 1:** `profiles.yml` com `dbt-duckdb`, `path: "{{ env_var('DUCKDB_PATH') }}"`. Verificar `uv run dbt debug --project-dir dbt` → `All checks passed!`.
- [ ] **Step 2:** Seed `raridades_alvo.csv` a partir da lista da Task 1 Step 8, classificando cada valor (`is_alvo=true` para Dupla Rara e acima). Teste dbt: `accepted_values` não se aplica; usar `unique` em (`raridade`,`idioma`).
- [ ] **Step 3: Unit test dbt que falha** (`int_cartas_enriquecidas.yml`):
  - `rayquaza_nas_duas_colecoes`: carta `pt`, tipos `['Dragão']`, `dex_ids [384]`, raridade alvo, espécie 384 lendária → `colecoes = ['Dragões','Lendários']`.
  - `tag_team_com_um_lendario`: `dex_ids [25, 150]`, 150 lendário → `is_lendario = true`.
  - `raridade_comum_nao_entra`: Dragão com raridade `Comum` → `is_alvo = false`, `colecoes = []`.
  - `japones_classificado_pela_raridade_en`: carta `ja` usa a seed com `idioma='en'` (a seed não tem nomes japoneses; o fallback é decisão deste plano). Se o TCGdex `ja` devolver raridade em inglês, o join é direto.
- [ ] **Step 4:** `uv run dbt build --project-dir dbt --select int_cartas_enriquecidas` → FAIL.
- [ ] **Step 5:** Implementar staging (renomear para português, tipar) e `int_cartas_enriquecidas`.
- [ ] **Step 6:** Mesmo comando → PASS. Verificar: `select count(*) from int_cartas_enriquecidas where idioma='pt' and list_contains(colecoes,'Dragões')` → > 0.
- [ ] **Step 7:** Docs + commit `feat: catálogo enriquecido com coleções`.

**Pergunta de checagem:** por que a definição de coleção é dado (seed + colunas) e não um `if` no Python?

---

### Task 4: Inventário no Postgres + CLI de cadastro

**Quem escreve:** Fernando (CLI e validações); Assistente pode dar o `schema.sql`. **Conceito:** separar OLTP (escritas pontuais, várias conexões) de OLAP (leituras analíticas). DuckDB aceita um escritor só; Postgres aguenta o bot e a CLI escrevendo enquanto o pipeline roda.

**Files:**
- Create: `src/pokemon_tcg/inventario/schema.sql`, `src/pokemon_tcg/inventario/cli.py`, `tests/test_cli_inventario.py`, `docs/adr/0004-postgres-para-inventario.md`
- Modify: `pyproject.toml` (`[project.scripts] tcg = "pokemon_tcg.inventario.cli:main"`)

**Interfaces:**
- Produces: tabela `copias`:

```sql
copia_id uuid primary key default gen_random_uuid(),
carta_id text not null,
idioma text not null check (idioma in ('pt','en','ja')),
condicao text not null check (condicao in ('M','NM','SP','MP','HP','D')),
origem text not null check (origem in ('pacote','compra')),
preco_pago_brl numeric(10,2),
data_entrada date not null default current_date,
destino_manual text check (destino_manual in ('colecao','venda')),
criado_em timestamptz not null default now(),
atualizado_em timestamptz not null default now(),  -- trigger atualiza em todo UPDATE
removido_em timestamptz
```

- `adicionar_copia(conn, carta_id: str, idioma: str, condicao: str, origem: str, preco_pago_brl: Decimal | None = None, destino_manual: str | None = None) -> UUID`
- `remover_copia(conn, copia_id: UUID) -> None` (soft delete: preenche `removido_em`)
- `listar_copias(conn) -> list[dict]` (só `removido_em is null`, mais recentes primeiro)
- `carta_existe(carta_id: str, idioma: str, caminho_duckdb: Path) -> bool` (abre DuckDB `read_only=True`)
- Exceções: `CartaNaoEncontrada`, `WarehouseOcupado`.
- CLI: `tcg adicionar --carta sv03.5-199 --idioma pt --condicao NM --origem pacote [--preco 12.50] [--destino venda]`, `tcg remover <copia_id>`, `tcg listar`. Feita com `argparse`.

- [ ] **Step 1: Testes que falham** (Postgres do compose, banco `tcg_teste` recriado por fixture):

```python
def test_adicionar_e_listar(conn, catalogo_fake):
    cid = adicionar_copia(conn, "sv03.5-199", "pt", "NM", "pacote")
    assert listar_copias(conn)[0]["copia_id"] == cid

def test_carta_inexistente_recusada(conn, catalogo_fake):
    with pytest.raises(CartaNaoEncontrada):
        adicionar_copia(conn, "xx-999", "pt", "NM", "pacote")

def test_remover_e_soft_delete(conn, catalogo_fake):
    cid = adicionar_copia(conn, "sv03.5-199", "pt", "NM", "pacote")
    remover_copia(conn, cid)
    linha = conn.execute("select removido_em from copias where copia_id=%s", (cid,)).fetchone()
    assert linha[0] is not None

def test_duckdb_travado_vira_mensagem(capsys, duckdb_travado):
    assert main(["adicionar", "--carta", "sv03.5-199", "--idioma", "pt",
                 "--condicao", "NM", "--origem", "pacote"]) == 2
    assert "pipeline rodando, tente de novo em instantes" in capsys.readouterr().err
```

`catalogo_fake`: DuckDB temporário com `bruto.tcgdex_cartas` contendo `sv03.5-199/pt`. `duckdb_travado`: outro processo segurando conexão de escrita no arquivo.

- [ ] **Step 2:** `uv run pytest tests/test_cli_inventario.py -v` → FAIL.
- [ ] **Step 3:** Implementar. Validação no catálogo via `carta_existe`; `duckdb.IOException` de lock → `WarehouseOcupado` → mensagem acima, código de saída 2.
- [ ] **Step 4:** Rodar → PASS. Cadastrar à mão o N's Zekrom (EN) e o Noivern (JP) reais.
- [ ] **Step 5:** ADR-0004 + commit `feat: inventário no Postgres e CLI tcg`.

---

### Task 5: Inventário para o DuckDB (Incremental Loader + Merger)

**Quem escreve:** Fernando. **Conceito — Incremental Loader:** trazer só o que mudou desde a última carga, usando um cursor (`atualizado_em`). **Merger:** gravar com upsert pela chave (`copia_id`), então rodar duas vezes não duplica. Por que soft delete: um cursor de data não enxerga linhas apagadas.

**Files:**
- Create: `src/pokemon_tcg/fontes/inventario.py`, `tests/test_fonte_inventario.py`, `docs/padroes/incremental-loader.md`, `docs/padroes/merger.md`

**Interfaces:**
- Consumes: tabela `copias` (Task 4).
- Produces: `fonte_inventario(dsn: str)` com recurso `copias`, `write_disposition="merge"`, `primary_key="copia_id"`, `dlt.sources.incremental("atualizado_em")` → `bruto.copias`.

- [ ] **Step 1: Testes que falham:**

```python
def test_rodar_duas_vezes_nao_duplica(pipeline, conn):
    adicionar_copia(conn, "sv03.5-199", "pt", "NM", "pacote")
    pipeline.run(fonte_inventario(DSN)); pipeline.run(fonte_inventario(DSN))
    assert contar(pipeline, "copias") == 1

def test_edicao_aparece(pipeline, conn):
    cid = adicionar_copia(conn, "sv03.5-199", "pt", "NM", "pacote")
    pipeline.run(fonte_inventario(DSN))
    conn.execute("update copias set condicao='SP' where copia_id=%s", (cid,))
    pipeline.run(fonte_inventario(DSN))
    assert valor(pipeline, "copias", cid, "condicao") == "SP"

def test_remocao_aparece(pipeline, conn):
    cid = adicionar_copia(conn, "sv03.5-199", "pt", "NM", "pacote")
    pipeline.run(fonte_inventario(DSN)); remover_copia(conn, cid)
    pipeline.run(fonte_inventario(DSN))
    assert valor(pipeline, "copias", cid, "removido_em") is not None
```

- [ ] **Step 2:** Rodar → FAIL. **Step 3:** Implementar com `psycopg` + `dlt.sources.incremental`. **Step 4:** Rodar → PASS.
- [ ] **Step 5:** Docs + commit `feat: carga incremental do inventário`.

**Pergunta de checagem:** se o relógio do Postgres atrasar um minuto, que linha o cursor pode perder? Como o `dlt` mitiga (dica: `lag` / deduplicação no limite)?

---

### Task 6: dbt — destino das cópias e faltantes

**Quem escreve:** Fernando. **Conceito — Decorator:** anexar atributos de contexto (idioma, condição, destino) à cópia sem alterar a carta do catálogo. **Fan-in:** catálogo + inventário convergem num mart.

**Files:**
- Create: `dbt/models/staging/stg_inventario__copias.sql`, `dbt/models/intermediate/int_copias_com_destino.sql` + `.yml`, `dbt/models/marts/faltantes_por_colecao.sql`, `resumo_colecao.sql`, `copias_sem_catalogo.sql`, `_marts.yml`, `docs/padroes/decorator.md`, `docs/padroes/fan-in.md`

**Interfaces:**
- Consumes: `bruto.copias`, `int_cartas_enriquecidas`.
- Produces:
  - `int_copias_com_destino`: grão `copia_id` (só `removido_em is null`), colunas da cópia + `colecoes`, `destino` ∈ {`colecao`, `venda`, `extra`}.
  - `faltantes_por_colecao`: grão (`colecao`, `carta_id`), cartas `pt` alvo sem cópia `pt`/`en` com destino `colecao`.
  - `resumo_colecao`: grão `colecao`; `total_alvo`, `possuidas`, `faltantes`, `pct_completo` (0–100, 1 casa).
  - `copias_sem_catalogo`: cópias cujo (`carta_id`,`idioma`) não está no catálogo. Teste dbt `severity: warn` se > 0 linhas.

**Regra de destino** (spec, seção 6.3), aplicada nesta ordem:
1. `destino_manual` preenchido → usa ele.
2. `idioma = 'ja'` e a carta pertence a alguma coleção → `extra` (guardada, não preenche vaga, não vai para venda; interpretação deste plano para "aparecem como extras").
3. Carta pertence a alguma coleção e é a 1ª cópia `pt`/`en` daquele `carta_id` (ordem: `destino_manual='colecao'` primeiro, depois `data_entrada`, `criado_em`) → `colecao`.
4. Caso contrário → `venda`.

- [ ] **Step 1: Unit tests dbt que falham** (`int_copias_com_destino.yml`):
  - `primeira_copia_fica`, `segunda_copia_vende`: duas cópias `pt` do mesmo Dragão alvo → `colecao`, `venda`.
  - `en_preenche_vaga_pt`: só uma cópia `en` de carta alvo → `colecao`.
  - `japones_e_extra`: cópia `ja` de Dragão alvo → `extra`.
  - `fora_das_colecoes_vende`: SAR de Pokémon não-dragão e não-lendário → `venda`.
  - `override_manual`: 1ª cópia com `destino_manual='venda'` → `venda`, e a 2ª cópia → `colecao`.
  - `removida_some`: cópia com `removido_em` não aparece.
- [ ] **Step 2: Unit test** de `resumo_colecao`: 4 alvos Dragões, 1 possuída → `faltantes=3`, `pct_completo=25.0`.
- [ ] **Step 3:** `uv run dbt build --project-dir dbt --select +resumo_colecao` → FAIL. **Step 4:** Implementar. **Step 5:** Mesmo comando → PASS.
- [ ] **Step 6:** Verificar com dados reais: `select * from resumo_colecao` → duas linhas, `Dragões` e `Lendários`.
- [ ] **Step 7:** Docs + commit `feat: destino das cópias e faltantes por coleção`.

---

### Task 7: Câmbio PTAX (Incremental Loader por data)

**Quem escreve:** Fernando. **Conceito — Incremental Loader por tempo:** o dlt guarda a última data carregada (`dlt.sources.incremental("dt")`) e a próxima execução só pede o período depois dela. Compare com a Task 5, onde o cursor é `atualizado_em` de uma tabela que muda; aqui o cursor é o próprio calendário. Fim de semana e feriado não têm cotação: período sem dados é normal, não erro.

**Files:**
- Create: `src/pokemon_tcg/fontes/ptax.py`, `tests/test_ptax.py`, `tests/fixtures/ptax_periodo.json`, `docs/padroes/incremental-loader.md` (seção "por tempo"), `docs/adr/0005-precos-japonesas-cardmarket-ptax.md`

**Interfaces:**
- Produces:
  - `normalizar_cotacao(bruta: dict) -> dict` com `dt: date` (de `dataHoraCotacao`), `eur_brl: Decimal` (de `cotacaoVenda`).
  - `cotacoes_ptax(de: date, ate: date, buscar: Callable[[date, date], list[dict]]) -> Iterator[dict]`: só boletim `Fechamento`, já normalizado.
  - `fonte_ptax(inicio: date = date(2026, 1, 1))` → `bruto.ptax_eur(dt, eur_brl)`, `write_disposition="merge"`, `primary_key="dt"`, cursor incremental em `dt`. Endpoint: `GET https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='MM-DD-AAAA'&@dataFinalCotacao='MM-DD-AAAA'&$format=json` (datas no formato americano).

- [ ] **Step 1 (feito na Tarefa T):** Salvar a resposta real de uma semana em `tests/fixtures/ptax_periodo.json` (inclui boletins intermediários e um fim de semana).
- [ ] **Step 2: Testes que falham:**

```python
def test_normaliza_cotacao(ptax_periodo):
    c = normalizar_cotacao(ptax_periodo["value"][-1])
    assert isinstance(c["dt"], date) and isinstance(c["eur_brl"], Decimal)

def test_so_boletim_de_fechamento(ptax_periodo):
    cs = list(cotacoes_ptax(DT1, DT2, buscar=lambda de, ate: ptax_periodo["value"]))
    assert len({c["dt"] for c in cs}) == len(cs)          # um fechamento por dia

def test_periodo_sem_cotacao_nao_e_erro():
    assert list(cotacoes_ptax(SABADO, DOMINGO, buscar=lambda de, ate: [])) == []

def test_segunda_carga_pede_so_o_que_falta(pipeline, fake_ptax):
    pipeline.run(fonte_ptax(inicio=DT1)); pipeline.run(fonte_ptax(inicio=DT1))
    assert fake_ptax.periodos[-1][0] > DT1                 # cursor andou
    assert contar(pipeline, "ptax_eur") == len(set(fake_ptax.dias))
```

- [ ] **Step 3:** Rodar → FAIL. **Step 4:** Implementar. **Step 5:** Rodar → PASS. Carga real desde 2026-01-01.
- [ ] **Step 6:** ADR-0005 (Liga saiu: Cloudflare bloqueia clientes que não são navegador, e contornar viola a spec; japonesas usam Cardmarket convertido pela PTAX; custo: preço europeu, não brasileiro) + commit `feat: câmbio EUR/BRL da PTAX`.

---

### Task 8: Preços Cardmarket via TCGdex (Data Overwrite por dia)

**Quem escreve:** Fernando. **Conceito — Data Overwrite:** cada carga substitui só a partição do dia (`delete-insert` por `dt_coleta`), então reexecutar o mesmo dia é idempotente e os dias anteriores ficam intactos. O preço vem do detalhe da carta no TCGdex (`pricing.cardmarket`, em EUR), buscado só para as cartas-alvo, **no idioma da cópia** (carta japonesa tem id e produto Cardmarket próprios).

**Files:**
- Create: `src/pokemon_tcg/fontes/cardmarket.py`, `tests/test_cardmarket.py`, `tests/fixtures/tcgdex_carta_ja.json`, `dbt/models/marts/alvos_precos.sql` + `.yml`, `docs/padroes/data-overwrite.md`

**Interfaces:**
- Consumes: `int_copias_com_destino`, `faltantes_por_colecao` (Task 6).
- Produces:
  - Mart `alvos_precos`: grão (`carta_id`, `idioma`), coluna `motivo` ∈ {`possuida`, `faltante`} (`possuida` vence). Fonte única da lista de cartas a precificar (MYP na Task 10 também lê daqui).
  - `extrair_cardmarket(carta: dict) -> dict | None` com `preco_medio_eur`, `preco_min_eur`, `tendencia_eur` (`Decimal`), `atualizado_em: date`; `None` se `pricing` ou `pricing.cardmarket` ausente/nulo.
  - `precos_cardmarket(alvos: Iterable[tuple[str, str]], buscar: Callable[[str, str], dict]) -> Iterator[dict]`: `buscar(idioma, carta_id)`; carta sem preço é pulada e contada em `ResumoCardmarket.sem_preco`.
  - `fonte_cardmarket(dt: date, alvos: Iterable[tuple[str, str]])` → `bruto.cardmarket_precos(carta_id, idioma, preco_medio_eur, preco_min_eur, tendencia_eur, atualizado_em, dt_coleta)`, `write_disposition={"disposition": "merge", "strategy": "delete-insert"}`, `merge_key="dt_coleta"`. Respostas cacheadas em `dados/bruto/tcgdex/{idioma}/{carta_id}.json` por dia (`dt=`), reaproveitando o cache da Task 1.
  - Dataclass `ResumoCardmarket(consultadas: int, com_preco: int, sem_preco: list[str])`.

- [ ] **Step 1 (feito na Tarefa T):** Salvar uma carta `ja` real com `pricing.cardmarket` em `tests/fixtures/tcgdex_carta_ja.json`.
- [ ] **Step 2: Testes que falham:**

```python
def test_extrai_preco_em_decimal(carta_pt):
    p = extrair_cardmarket(carta_pt)
    assert p["preco_medio_eur"] == Decimal(str(carta_pt["pricing"]["cardmarket"]["avg"]))

def test_carta_sem_preco_vira_none(carta_ja):
    carta_ja["pricing"] = {"cardmarket": None, "tcgplayer": None}
    assert extrair_cardmarket(carta_ja) is None

def test_busca_no_idioma_da_copia(carta_ja):
    chamadas = []
    list(precos_cardmarket([("M5-115", "ja")], buscar=lambda i, c: chamadas.append((i, c)) or carta_ja))
    assert chamadas == [("ja", "M5-115")]

def test_reprocessar_mesmo_dia_nao_duplica(pipeline, fake_tcgdex):
    pipeline.run(fonte_cardmarket(DT, ALVOS)); pipeline.run(fonte_cardmarket(DT, ALVOS))
    assert contar(pipeline, "cardmarket_precos") == len(ALVOS)

def test_outro_dia_preservado(pipeline, fake_tcgdex):
    pipeline.run(fonte_cardmarket(DT1, ALVOS)); pipeline.run(fonte_cardmarket(DT2, ALVOS)); pipeline.run(fonte_cardmarket(DT2, ALVOS))
    assert dias(pipeline, "cardmarket_precos") == {DT1, DT2}
```

  Unit test dbt de `alvos_precos`: cópia `ja` possuída → linha (`carta`, `ja`, `possuida`); carta possuída e faltante → 1 linha `possuida`.
- [ ] **Step 3:** Rodar → FAIL. **Step 4:** Implementar. **Step 5:** Rodar → PASS.
- [ ] **Step 6:** Docs + commit `feat: preços Cardmarket por carta via TCGdex`.

**Pergunta de checagem:** se o TCGdex atualizar o preço Cardmarket só uma vez por dia e você rodar a carga três vezes no mesmo dia, quantas linhas o snapshot SCD2 da Task 11 cria? Por quê?

---

### Task 9: (removida)

LigaPokémon saiu da fase 1 em 2026-10-18: o site passou a responder com desafio Cloudflare (HTTP 403) para clientes que não são navegador, e contornar isso viola a spec. Os padrões que só a Liga exercitava (Checkpointer, Dead-Letter, Schema Compatibility Enforcer em HTML) ficam para uma fonte futura. Ver ADR-0005.

---

### Task 10: Fonte MYP (carta por carta, só as cartas-alvo)

**Quem escreve:** Fernando. **Bloqueio externo:** o token MYP. Fernando pede em `mypcards.com/acesso-api` **no início desta fase**; se não houver resposta em 1 semana, cobra. A tarefa avança com fixture; só o Step 6 depende do token.

**Conceito:** buscar só o que se usa. Em vez de baixar o catálogo inteiro, a lista de cartas vem do próprio warehouse (`alvos_myp`, derivado de `alvos_precos`) e cada carta vira uma requisição a `/precos/{codigo}`. Cada execução grava a "foto" das cartas-alvo naquele dia (Data Overwrite por `dt_coleta`, como na Task 8). Carta que a MYP não conhece (404) não derruba a carga: é contada e registrada.

**Futuro (fora da fase 1):** carga do catálogo inteiro via `GET /pokemon/precos?page_number={n}&page_size=50`, se um dia o dashboard precisar de preço de carta fora dos alvos. O desenho já deixa o encaixe: `fonte_myp` recebe um iterador de `Produto`; o modo catálogo seria só outro gerador (`paginas_myp`) alimentando o mesmo recurso e a mesma tabela.

**Files:**
- Create: `src/pokemon_tcg/fontes/myp.py`, `tests/test_myp.py`, `tests/fixtures/myp_carta.json`, `dbt/seeds/de_para_edicoes_myp.csv`, `dbt/models/marts/alvos_myp.sql` + `.yml`, `docs/adr/0006-myp-primaria-cardmarket-secundaria.md`

**Interfaces:**
- Consumes: `alvos_precos` (Task 8, só idiomas `pt`/`en`: MYP não tem japonesas), seed `de_para_edicoes_myp`.
- Produces:
  - `codificar_codigo(codigo: str) -> str`: Base64 do código MYP (ex. `LOB-001` → `TE9CLTAwMQ==`).
  - `precos_por_carta(codigos: Iterable[str], buscar: Callable[[str], dict | None]) -> Iterator[dict]`: um `Produto` por código encontrado; `buscar` devolve `None` em 404 e o código entra em `ResumoMyp.nao_encontrados`.
  - `fonte_myp(token: str, dt: date, codigos: Iterable[str])` → `bruto.myp_precos(internal_code, card_code, edition_code, name_pt, min_price, avg_price, max_price, tcg_price, img_pt, link, dt_coleta)`, `delete-insert` por `dt_coleta`. Endpoint: `GET https://mypcards.com/api/v1/pokemon/precos/{codigo_base64}`, header `X-Api-Token`.
  - Dataclass `ResumoMyp(consultados: int, encontrados: int, nao_encontrados: list[str])`.
  - Mart `alvos_myp`: grão `codigo_myp` (distinto), colunas `carta_id`, `codigo_myp`, `motivo` ∈ {`possuida`, `faltante`} (`possuida` vence quando a carta é as duas coisas). Carta sem de-para de edição fica de fora e aparece num teste dbt `severity: warn`.
  - Seed `de_para_edicoes_myp(tcgdex_set_id, myp_edition_code)`.

- [ ] **Step 1: Testes que falham:**

```python
def test_codigo_vai_em_base64():
    assert codificar_codigo("LOB-001") == "TE9CLTAwMQ=="

def test_uma_requisicao_por_codigo():
    chamados = []
    def buscar(c):
        chamados.append(c)
        return {"card_code": c}
    list(precos_por_carta(["A-1", "B-2"], buscar))
    assert chamados == ["A-1", "B-2"]

def test_carta_desconhecida_nao_derruba_a_carga():
    buscar = lambda c: None if c == "X-9" else {"card_code": c}
    assert [p["card_code"] for p in precos_por_carta(["A-1", "X-9", "B-2"], buscar)] == ["A-1", "B-2"]

def test_sem_token_falha_com_mensagem():
    with pytest.raises(ValueError, match="MYP_API_TOKEN"):
        fonte_myp(token=None, dt=DT, codigos=["A-1"])
```

  Unit test dbt de `alvos_myp`: 1 cópia possuída + 1 faltante da mesma carta → 1 linha com `motivo = possuida`; carta sem de-para → 0 linhas.
- [ ] **Step 2:** Rodar → FAIL. **Step 3:** Implementar. **Step 4:** Rodar → PASS.
- [ ] **Step 5:** ADR-0006 (inclui: MYP reflete mercado de trocas entre colecionadores; sem idioma, condição nem japonesas; Cardmarket cobre o resto; só cartas-alvo, catálogo inteiro como opção futura) + commit `feat: fonte MYP por carta`.
- [ ] **Step 6 (quando houver token):** spike com 3 cartas possuídas para descobrir o formato exato de `{codigo}` (`card_code`? `edition_code` + número?) e preencher o de-para para as edições das cartas possuídas; ajustar `alvos_myp` se o formato pedir. Verificar: `select count(*) from bruto.myp_precos where dt_coleta = current_date` = `select count(*) from alvos_myp` − `nao_encontrados`.

---

### Task 11: dbt — preços unificados, histórico SCD2 e auditoria (Audit-Write-Audit-Publish)

**Quem escreve:** Fernando. **Conceitos:** **Adapter de fontes:** uma staging por fonte, todas no mesmo formato, unidas em `int_precos_unificados`. **SCD2 via snapshot:** cada mudança de preço vira nova linha com `dbt_valid_from`/`dbt_valid_to`; lacunas de dias (sistema desligado) não criam linhas falsas. **Audit-Write-Audit-Publish:** escrever primeiro numa área intermediária, auditar e só então publicar nos marts. Se a auditoria falha, os marts continuam com a versão anterior.

**Files:**
- Create: `dbt/models/staging/stg_myp__precos.sql`, `stg_cardmarket__precos.sql`, `stg_ptax__cotacoes.sql`, `dbt/models/intermediate/int_precos_unificados.sql` + `.yml`, `dbt/snapshots/snp_precos.sql`, `dbt/models/marts/historico_precos.sql`, `cartas_para_venda.sql`, `dbt/tests/auditoria_preco_positivo.sql`, `auditoria_preco_fora_da_curva.sql`, `docs/padroes/audit-write-audit-publish.md`, `docs/padroes/scd2.md`

**Interfaces:**
- Produces:
  - `int_precos_unificados`: grão (`carta_id`, `idioma_preco`, `fonte`), apenas a última `dt_coleta` de cada fonte; colunas `preco_min`, `preco_medio`, `preco_max`, `observado_em` (date). `idioma_preco` = `null` para MYP (sem separação), idioma da carta para Cardmarket. Cardmarket entra já em BRL: `avg`/`low` × `eur_brl` da PTAX de `observado_em` ou do último dia útil anterior (join *as-of*); colunas extras `moeda_origem`, `cotacao_usada`. Tag `auditoria_precos` nos testes.
  - `snp_precos`: `strategy='check'`, `check_cols=['preco_min','preco_medio','preco_max']`, `unique_key` = `carta_id || '|' || coalesce(idioma_preco,'*') || '|' || fonte`.
  - `historico_precos`: snapshot com nomes em português (`valido_de`, `valido_ate`).
  - `cartas_para_venda`: cópias com `destino='venda'` + preço de referência: Cardmarket (mesmo idioma) para `ja`; MYP para `pt`/`en` (fallback Cardmarket). Colunas `preco_referencia_brl` (médio), `preco_min_brl`, `preco_max_brl`, `fonte`, `observado_em`, `imagem_url`.
- Auditoria (testes singulares, `tags: ['auditoria_precos']`): `preco_medio > 0`; `preco_medio <= 100 * mediana` do histórico da mesma carta/fonte (sem histórico, passa).

- [ ] **Step 1: Unit tests dbt que falham:**
  - `int_precos_unificados` mantém só a última `dt_coleta` por fonte.
  - `cartas_para_venda`: cópia `ja` usa Cardmarket `ja`; cópia `pt` usa MYP; cópia `pt` sem MYP usa Cardmarket.
  - `stg_cardmarket__precos`: preço observado num domingo usa a PTAX da sexta; sem cotação anterior, `preco_medio` nulo (Review Focus 3).
- [ ] **Step 2: Teste da auditoria:** fixture com `preco_medio = 0` → `uv run dbt test --project-dir dbt --select tag:auditoria_precos` falha com 1 linha; fixture com 150× a mediana → falha.
- [ ] **Step 3:** Rodar → FAIL. **Step 4:** Implementar. **Step 5:** Rodar → PASS.
- [ ] **Step 6:** Verificar SCD2: rodar `dbt snapshot` duas vezes com o mesmo preço → 1 linha; mudar o preço no bruto e rodar de novo → 2 linhas, a antiga com `dbt_valid_to` preenchido.
- [ ] **Step 7:** Docs + commit `feat: preços unificados, histórico SCD2 e auditoria`.

**Pergunta de checagem:** se a auditoria falhar só para uma carta, o que é melhor: bloquear tudo ou publicar as outras? Que padrão do livro trata a segunda opção?

---

### Task 12: Prefect — fluxos, Readiness Marker, botão de atualização e frescor

**Quem escreve:** Fernando (fluxos); Assistente (`servir.py` e serviço no compose). **Conceitos:** **Readiness Marker:** cada ingestão grava em `controle.cargas` que terminou; a transformação só roda se as fontes exigidas estiverem prontas, o que importa porque o botão pode disparar a transformação sozinha. **Lag Detector:** ao ligar, avisar se os preços estão velhos.

**Files:**
- Create: `src/pokemon_tcg/controle.py`, `src/pokemon_tcg/fluxos/ingestao.py`, `transformacao.py`, `precos.py`, `frescor.py`, `servir.py`, `tests/test_fluxos.py`, `docs/adr/0007-prefect-orquestracao.md`, `docs/padroes/readiness-marker.md`, `docs/padroes/lag-detector.md`
- Modify: `docker-compose.yml` (`servidor-fluxos` roda `python -m pokemon_tcg.fluxos.servir`), `Makefile` (`make up` dispara `verificar_frescor` após subir)

**Interfaces:**
- `controle.marcar_pronto(fonte: str, dt: date) -> None`; `controle.esta_pronto(fonte: str, dt: date) -> bool` (tabela DuckDB `controle.cargas(fonte, dt, concluido_em)`).
- Flows:
  - `ingerir_catalogo()`: TCGdex + PokeAPI; marca prontos.
  - `ingerir_inventario()`.
  - Toda ingestão chama `marcar_pronto(<fonte>, dt)` ao terminar sem erro; nomes de fonte: `tcgdex`, `pokeapi`, `inventario`, `myp`, `cardmarket`, `ptax`.
  - `transformar(fontes_exigidas: list[str], dt: date)`: falha com `FonteNaoPronta` se faltar marcador; senão, AWAP: `dbt build --exclude tag:marts` (write) → `dbt test --select tag:auditoria_precos` (audit) → `dbt snapshot` → `dbt build --select tag:marts` (publish) → `exportar_metricas()` (Task 13) → `npm run sources` no Evidence.
  - `atualizar_precos()`: `ingerir_inventario` → `dbt build --select +alvos_precos +alvos_myp` (a lista de alvos vem do warehouse) → PTAX → Cardmarket com `alvos_precos` → MYP com `alvos_myp` (se houver token; senão loga aviso e segue) → `transformar`.
  - `verificar_frescor() -> int`: dias desde o último `observado_em`; se > `LIMIAR_DIAS_PRECO_VELHO`, cria artefato markdown no Prefect "Preços com N dias. Rode atualizar_precos." e loga `WARNING`.
- Deployments servidos por `servir.py`: `atualizar-precos`, `ingerir-catalogo`, `verificar-frescor`. **Sem agendamento** (o sistema não fica ligado).

- [ ] **Step 1: Testes que falham** (`prefect_test_harness`, dbt e fontes substituídos por fakes):

```python
def test_transformar_exige_marcador():
    with pytest.raises(FonteNaoPronta, match="cardmarket"):
        transformar(["cardmarket"], DT)

def test_auditoria_reprovada_nao_publica(fake_dbt):
    fake_dbt.falhar_em("test")
    with pytest.raises(Exception):
        transformar([], DT)
    assert "build --select tag:marts" not in fake_dbt.comandos

def test_sem_token_myp_segue_com_cardmarket(fake_fontes, monkeypatch):
    monkeypatch.delenv("MYP_API_TOKEN", raising=False)
    atualizar_precos()
    assert "cardmarket" in fake_fontes.rodadas and "myp" not in fake_fontes.rodadas

def test_frescor_avisa(precos_de_10_dias_atras, caplog):
    assert verificar_frescor() == 10
    assert "Preços com 10 dias" in caplog.text
```

- [ ] **Step 2:** Rodar → FAIL. **Step 3:** Implementar. **Step 4:** Rodar → PASS.
- [ ] **Step 5:** Verificar ponta a ponta: `make up` → UI em `http://localhost:4200` lista os 3 deployments → clicar **Run** em `atualizar-precos` → termina `Completed`; `select * from historico_precos` traz o Noivern JP.
- [ ] **Step 6:** ADR-0007 com o motivo do Fernando (conhece pouco e usa num cliente) + commit `feat: fluxos Prefect com AWAP e frescor`.

---

### Task 13: Métricas (MetricFlow → `metricas_colecao`)

**Quem escreve:** Fernando. **Conceito:** métrica definida **uma vez** (semantic model + metric no MetricFlow). O MetricFlow open source só compila SQL, então um passo do fluxo materializa o resultado numa tabela que dashboard e bot leem.

**Files:**
- Create: `dbt/models/semantic/colecao.yml`, `src/pokemon_tcg/fluxos/metricas.py`, `tests/test_metricas.py`, `docs/adr/0008-metricas-materializadas.md`

**Interfaces:**
- Métricas: `cartas_faltantes`, `cartas_possuidas`, `pct_completo`, `valor_venda_brl` (soma de `preco_referencia_brl`), dimensão `colecao`.
- `exportar_metricas() -> None`: roda `mf query --metrics cartas_faltantes,cartas_possuidas,pct_completo --group-by colecao --csv <tmp>` e `mf query --metrics valor_venda_brl --csv <tmp>`, e grava `main.metricas_colecao(metrica, colecao, valor, calculado_em)` (`colecao` nulo para `valor_venda_brl`).

- [ ] **Step 1 (feito na Tarefa T): Spike (15 min):** confirmar que `mf query` funciona com `dbt-duckdb` na versão instalada. Se não funcionar, ADR-0008 registra a alternativa: definir as métricas num único modelo dbt `metricas_colecao`.
- [ ] **Step 2: Teste que falha:** com o DuckDB de fixture da Task 6 (4 alvos, 1 possuída), `exportar_metricas()` grava `pct_completo` de `Dragões` = `25.0` e `cartas_faltantes` = `3`, igual a `resumo_colecao`.
- [ ] **Step 3:** Rodar → FAIL. **Step 4:** Implementar. **Step 5:** Rodar → PASS.
- [ ] **Step 6:** Docs + commit `feat: métricas definidas no MetricFlow e materializadas`.

---

### Task 14: Dashboard Evidence (Fichário, Para venda, Preços)

**Quem escreve:** Assistente faz o esqueleto do projeto Evidence; Fernando escreve as queries SQL das páginas. Construção de UI passa pela skill **Hallmark** (regra do CLAUDE.md global).

**Files:**
- Create: `evidence/` (via `npx degit evidence-dev/template evidence`), `evidence/sources/warehouse/connection.yaml` (DuckDB, `read_only`), `evidence/pages/index.md`, `fichario.md`, `venda.md`, `precos.md`, `docs/adr/0009-evidence-dashboard.md`

**Interfaces:**
- Consumes: `metricas_colecao`, `faltantes_por_colecao`, `int_copias_com_destino`, `cartas_para_venda`, `historico_precos`, `controle.cargas`.
- Páginas:
  - **Início:** `pct_completo` e faltantes por coleção; idade dos preços; link "Atualizar preços" → `http://localhost:4200/deployments` (UI do Prefect).
  - **Fichário:** seletor de coleção; grade de imagens (`DataTable` com `contentType="image"`) das cartas alvo; possuídas e faltantes distinguidas (faltante em escala de cinza ou rótulo "falta"); contador "faltam N".
  - **Para venda:** tabela com imagem, nome, idioma, condição, preço de referência, faixa min–max, fonte, "preço visto em DD/MM"; total em BRL.
  - **Preços:** seletor de carta; linha do tempo de `preco_medio` por fonte.
  - Rodapé de toda página com preço: "Preços: MYP Cards" com link (quando houver preço MYP) e "Cardmarket via TCGdex, convertido de EUR pela PTAX do Banco Central" (quando houver preço Cardmarket).

- [ ] **Step 1:** Esqueleto + conexão. Verificar `npm run sources` sem erro.
- [ ] **Step 2: Invocar a skill Hallmark** antes de escrever qualquer página: direção visual (tipografia, cores do tema Evidence, layout da grade do fichário, estados possuída/faltante). A Hallmark trabalha dentro dos componentes do Evidence (tema e layout), sem substituir o framework.
- [ ] **Step 3:** Páginas, seguindo a direção da Hallmark. Fernando escreve as queries SQL; o layout segue a Hallmark.
- [ ] **Step 4: Verificação visual (regra global):** abrir com Playwright, screenshot a 1440px e 390px de cada página; checar contra a intenção e o slop-test da Hallmark; conferir contra a spec: grade legível no celular, data do preço visível, atribuição MYP presente. Corrigir e refazer as screenshots até passar. Com `make up` rodando, `uv run pytest -m e2e` (escrito na Tarefa T) → PASS. Se a grade de imagens não funcionar no `DataTable`, registrar na ADR-0009 e seguir o fallback Streamlit numa tarefa nova.
- [ ] **Step 5:** Commit `feat: dashboard Evidence`.

---

### Task 15: Fechamento da fase 1

**Quem escreve:** Fernando (revisão do catálogo de padrões); Assistente (README).

**Files:**
- Create: `docs/padroes/README.md` (índice: padrão → arquivo(s) → ADR), `docs/adr/0010-por-que-nao-spark.md`, `docs/adr/0011-historico-de-precos-proprio.md`
- Modify: `README.md`

- [ ] **Step 1:** Conferir que todo modelo/tarefa que implementa um padrão tem comentário `-- padrão: docs/padroes/<nome>.md` (SQL) ou `# padrão: ...` (Python). Verificar: `grep -rL "padrão:" src/pokemon_tcg/fontes dbt/models/intermediate dbt/models/marts` → vazio, ou só arquivos sem padrão.
- [ ] **Step 2:** `uv run dbt docs generate --project-dir dbt` → sem erro; README explica `dbt docs serve`.
- [ ] **Step 3:** Rodada completa a partir do zero: `make down`, apagar `dados/`, `make up`, rodar `ingerir-catalogo` e depois `atualizar-precos (tudo)`, abrir o dashboard. Tudo verde.
- [ ] **Step 4:** `uv run pytest -v` e `uv run dbt build --project-dir dbt` → tudo PASS.
- [ ] **Step 5:** Commit `docs: fechamento da fase 1`.
