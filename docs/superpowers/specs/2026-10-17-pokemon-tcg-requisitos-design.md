# Pokémon TCG Tracker — Documento de Requisitos e Stack Técnica

- **Data:** 2026-10-17
- **Status:** rascunho para aprovação
- **Dono:** Fernando (engenheiro de dados)
- **Natureza:** projeto pessoal de aprendizado, roda localmente, custo zero

---

## 1. Intenção (o porquê)

Fernando coleciona Pokémon TCG com dois focos: **Pokémon Lendários** e **Pokémon tipo Dragão**. Só interessam as raridades de destaque (SIR, SAR, IR e afins) para montar um fichário bonito; bulk e comuns ficam de fora.

Três dores hoje:

1. **Não sabe exatamente o que tem.** Quer um inventário digital de todas as cartas que abriu ou comprou, incluindo idioma (a maioria em português, algumas em inglês e japonês, ex.: N's Zekrom em inglês, Noivern em japonês).
2. **Não sabe o que falta.** Ex.: "quantos dragões SIR ainda faltam?".
3. **Não sabe quanto valem as cartas que não quer guardar.** Cartas boas que saem nos pacotes mas não entram em nenhuma das duas coleções (ex.: uma SAR de um Pokémon que não é dragão nem lendário) devem ter preço em reais para facilitar a venda.

O projeto tem um **segundo objetivo, tão importante quanto o primeiro**: ser o veículo para Fernando aprender e praticar os padrões do livro *Data Engineering Design Patterns* (Bartosz Konieczny, O'Reilly) e algumas ferramentas novas, sempre com justificativa explícita para cada escolha.

## 2. Objetivos, não-objetivos e anti-objetivos

### Objetivos

- Inventário digital das cartas possuídas (idioma, condição, quantidade, origem).
- Visão de **faltantes** por coleção (Dragões, Lendários), filtrada por raridades-alvo.
- Visão de **cartas para venda** com preço em BRL.
- **Histórico de preços próprio**, construído a partir de snapshots.
- Bot no WhatsApp para cadastrar cartas e responder métricas (fase 2).
- Documentação que **explica cada decisão e cada padrão**: o que é, por que foi escolhido, o que faz, onde está no código.
- Tudo em **português**: interface, nomes de cartas, documentação e rótulos do modelo de dados.

### Não-objetivos

- Não é um produto para outras pessoas. Usuário único.
- Não é marketplace: não compra nem vende cartas automaticamente.
- Não cobre cartas bulk ou comuns.
- Não roda 24/7.

### Anti-objetivos (falharia mesmo funcionando)

- Usar Spark, Kafka, cloud ou qualquer ferramenta sem justificativa pelo volume real de dados. Ferramenta nova só entra com ADR explicando o porquê.
- Gerar custo recorrente (assinatura, nuvem, mensagens pagas) sem decisão explícita.
- Código que Fernando não entende: o projeto existe para aprender (ver seção 10).
- Scraping que desrespeita `robots.txt` ou contorna proteções (Cloudflare) de qualquer site.
- Documentação escrita à mão para o que uma ferramenta já gera (`dbt docs`, UI do Prefect).

## 3. Restrições

- **Execução local** no Mac do Fernando. Custo zero.
- **Liga/desliga fácil:** o sistema inteiro sobe e desce com um comando, para não consumir recursos quando não está em uso. Consequência: não existe agendamento diário garantido; os dados de preço terão lacunas, e o desenho precisa tratar isso como normal.
- **Português-first** em tudo.
- **Respeito às fontes:** LigaPokémon tem `Crawl-delay: 360` (1 requisição a cada 6 minutos) e proíbe a página de histórico de preços no `robots.txt`. MYP Cards exige token e atribuição visível ("MYP Cards" com link) onde seus dados aparecerem.

## 4. Fontes de dados (verificadas em 2026-10-17)

| Fonte | Papel | Acesso | Observações |
|---|---|---|---|
| **TCGdex** (`api.tcgdex.net/v2/{lang}`) | Catálogo de cartas | REST/GraphQL, sem chave, licença MIT, SDK Python `tcgdex-sdk` | Usar idiomas `pt`, `en`, `ja` (**não** `pt-br`, que só tem TCG Pocket). Cada carta tem `rarity` ("Ilustração Rara Especial"), `types` (`Dragão`), `dexId` (lista), `localId` (número), `set.id`, imagem (`assets.tcgdex.net/...`). Também traz preços Cardmarket (EUR) e TCGplayer (USD). Sem rate limit publicado; pede cache. |
| **PokeAPI** (`pokemon-species/{id}`) | Flag de lendário/mítico | REST, sem chave | `is_legendary`, `is_mythical`. Join pelo `dexId` do TCGdex. Carga única de ~1.025 espécies. |
| **MYP Cards API** (`mypcards.com/api/v1`) | **Preço BRL primário** | Header `X-Api-Token`, pedido em `mypcards.com/acesso-api` (aprovação manual) | Listagem paginada de **todo** o catálogo Pokémon (50 por página). `min_price`/`avg_price`/`max_price` em BRL, `tcg_price` (USD), `name_pt`, `edition_code`, `card_code`, `img_pt`. **Não tem**: separação por idioma, condição, histórico, edições japonesas ("edições orientais não são retornadas"). Reflete mercado entre colecionadores (trocas), não só lojas. Exige atribuição. |
| **LigaPokémon** (`ligapokemon.com.br`) | **Preço BRL secundário**, só onde a MYP não cobre | Scraping de HTML, sem API | Preço embutido como JSON na página: `cards_editions` (`price` por idioma, `p/m/g` = mín/méd/máx) e `cards_stock` (ofertas com `precoFinal`, `qualid`, `idioma`). Idiomas: 8=PT, 2=EN, 6=JP. Condição: 1=M, 2=NM, 3=SP, 4=MP, 5=HP. Códigos de edição próprios (precisa de tabela de-para com TCGdex). **6 minutos por requisição.** |

**Escopo da MYP:** só as cartas-alvo (possuídas + faltantes), via `GET /pokemon/precos/{codigo}` (código em Base64), poucas vezes por semana. Catálogo inteiro paginado fica como opção futura, se o dashboard precisar de preço fora dos alvos. *Decisão do Fernando, 2026-10-18.*

**Escopo da Liga:** apenas cartas japonesas **possuídas** e, opcionalmente, cartas à venda onde a condição importa. Cartas faltantes são precificadas só pela MYP. Se o token da MYP for negado, a Liga volta a ser fonte primária (ver risco R1).

**Qual número é "o preço":** `avg_price` (MYP) / `m` (Liga) é o preço de referência para valor de venda; mínimo e máximo aparecem ao lado como faixa. *Decisão do assistente.*

## 5. Stack técnica e justificativas

Cada linha vira um ADR (seção 9).

| Camada | Escolha | Por quê | Alternativa descartada |
|---|---|---|---|
| Linguagem / pacotes | **Python + uv** | Padrão de mercado em DE; uv é rápido e substitui pip/poetry. | poetry |
| Ingestão | **dlt** *(novo)* | Biblioteca, não plataforma. Estado incremental, evolução de schema e `write_disposition` (append/replace/merge) embutidos: padrões do livro visíveis em código. Fontes customizadas para MYP e Liga. | Airbyte (Fernando já conhece; plataforma pesada para local) |
| Banco do app (inventário) | **Postgres** (Docker) | O bot escreve no inventário enquanto o pipeline roda; DuckDB aceita **só um escritor**. Separar OLTP (Postgres) de OLAP (DuckDB) espelha sistemas reais e torna o inventário uma fonte de ingestão. | SQLite |
| Armazenamento bruto | **Arquivos locais**: HTML bruto da Liga + Parquet particionado por data | Guardar o HTML permite **reprocessar** quando o parser mudar sem re-raspar a 6 min/carta. | Só tabelas |
| Warehouse analítico | **DuckDB** *(novo)* | Um arquivo, sem servidor, custo zero; `dbt-duckdb` maduro; lê Parquet direto; ensina OLAP colunar. | Postgres como warehouse |
| Transformação | **dbt** | Padrão da indústria, Fernando usa diariamente. Foco em snapshots (SCD2), incrementais, testes, seeds. | — |
| Métricas | **dbt Semantic Layer (MetricFlow)** *(novo)* + **mart de métricas materializado** | Métrica definida **uma vez** no MetricFlow. Limitação: o MetricFlow open source só compila SQL; consulta ao vivo via API é recurso do dbt Cloud (pago). Por isso as métricas são materializadas numa tabela (`metricas_colecao`) que o dashboard e o bot leem: a definição continua única e os números nunca divergem. *Decisão do assistente.* | SQL duplicado em cada consumidor; dbt Cloud (pago) |
| Orquestração | **Prefect** *(novo)* | Escolha do Fernando. Fluxos e tarefas em Python puro. Sem "asset checks" nativos, então o padrão Audit-Write-Audit-Publish é construído à mão (mais didático). UI local com botão "Run". Integração `prefect-dbt`. **Motivo da escolha (Fernando):** conhece pouco e usa Prefect em um cliente, então o aprendizado aqui volta direto para o trabalho. | Dagster, Airflow |
| Dashboard | **Evidence.dev** *(novo)* | BI-as-code (SQL + markdown), lê DuckDB, renderiza imagens em tabela (`contentType="image"`), responsivo no celular. Próximo do fluxo dbt. | Streamlit (fallback se a grade de imagens do fichário ficar limitada) |
| Bot (fase 2) | **WhatsApp Cloud API oficial** | Onde o Fernando já está. Webhook ensina ingestão por push, eventos e idempotência. Núcleo do bot independente de canal (padrão adapter), então trocar por Discord é escrever outro adapter. | Discord (mais simples, sem URL pública); bibliotecas não oficiais de WhatsApp (risco de banimento: **proibidas**) |
| Exposição do webhook | **Tailscale Funnel** | Gratuito e com **URL HTTPS fixa** (`*.ts.net`), então o webhook da Meta é configurado uma vez só. Reaproveita o Tailscale já usado para o dashboard. *Decisão do assistente.* | cloudflared quick tunnel (URL muda a cada `make up`, exigiria reconfigurar o webhook); ngrok |
| Acesso remoto ao dashboard | **Tailscale** (grátis para uso pessoal) | Rede privada entre os aparelhos, sem expor portas. | Expor porta na internet |
| Liga/desliga | **Docker Compose + Makefile** | `make up` sobe tudo, `make down` libera CPU e memória. | Serviços rodando sempre |

**Fora da stack, de propósito:** Spark, Kafka, qualquer nuvem. Volume de dados é de milhares de linhas; usar essas ferramentas ensinaria a lição errada. Vira ADR ("por que não Spark").

### Custos do WhatsApp (verificado na página oficial da Meta, atualizada em 30/09/2026)

- Desde 01/10/2026, cada número comercial tem **1.000 mensagens de serviço grátis por mês** (respostas dentro da janela de 24h após mensagem do usuário). Sem forma de pagamento cadastrada, a Meta para de entregar após 1.000: **não há cobrança surpresa**.
- ~3 mensagens por cadastro de carta ≈ 300 cartas/mês grátis.
- O bot **só responde**. Mensagem iniciada pelo bot (ex.: alerta de preço) exige template e é paga. Por isso alertas proativos ficam fora do escopo.

## 6. Escopo por fase

### Fase 1 — fundação de dados (MVP)

1. **Catálogo:** carga do TCGdex (`pt`, `en`, `ja`) + espécies da PokeAPI.
2. **Inventário** no Postgres, cadastrado manualmente (seed CSV ou CLI simples). Campos por cópia: carta, idioma, condição, origem (pacote/compra), preço pago (opcional), data de entrada, destino manual opcional (sobrescreve a regra derivada abaixo). Cada cópia física é uma linha.
3. **Definição das coleções como dados (seeds editáveis):**
   - Dragões = cartas com tipo `Dragão`.
   - Lendários = cartas cujo `dexId` tem `is_legendary OR is_mythical`.
   - Ambas filtradas por **raridades-alvo** (seed editável): **Dupla Rara (Double Rare) e tudo acima**, ou seja Ultra Rara, Ilustração Rara, Ilustração Rara Especial, Hiper Rara, Brilhante Rara e equivalentes de eras antigas. Ficam fora: Comum, Incomum, Rara e Rara Holo. *Decisão do Fernando.* O seed é montado a partir dos valores distintos de `rarity` do TCGdex (`pt` e `en`), classificados um a um como alvo ou não, porque os nomes de raridade mudam entre eras.
   - **Identidade da carta = edição + número**, nunca só o número. Cartas com o mesmo número em edições diferentes são cartas diferentes (acontece com reimpressões dos clássicos, como na coleção de 30 anos). *Decisão do Fernando.*
   - **Lista de referência = catálogo `pt`.** Uma cópia em inglês da mesma edição + número preenche a vaga da carta em português. Cartas japonesas não preenchem vagas: aparecem no inventário como extras. *Sugestão do assistente, não contestada.*
   - **Regra de destino (derivada, não digitada):** uma cópia pode preencher as duas coleções ao mesmo tempo (ex.: Rayquaza é Dragão e Lendário); a primeira cópia de uma carta que pertence a alguma coleção fica na coleção; cópias extras e cartas fora das duas coleções vão para venda; Fernando pode sobrescrever manualmente por cópia. *Decisão do Fernando.*
4. **Preços:** MYP (via API, só as cartas-alvo: possuídas + faltantes, uma requisição por carta) + Liga (escopo restrito, com HTML bruto guardado). Histórico próprio via **dbt snapshot (SCD2)**.
5. **Seed de-para** de códigos de edição: TCGdex `set.id` ↔ MYP `edition_code` ↔ Liga `ed`.
6. **Marts:** `faltantes_por_colecao`, `cartas_para_venda`, `historico_precos`, `resumo_colecao` (contagens e percentuais).
7. **Métricas** no Semantic Layer: `cartas_faltantes`, `pct_completo`, `valor_venda_brl`, entre outras.
8. **Dashboard Evidence** com três telas:
   - **Fichário:** grade de imagens por coleção; possuídas coloridas, faltantes em cinza; contador "faltam N".
   - **Para venda:** cartas fora das coleções, preço MYP/Liga em BRL, valor total.
   - **Preços:** histórico por carta, sempre exibindo "preço visto em DD/MM".
9. **Orquestração Prefect** com deployment `atualizar_precos` parametrizado por escopo (`a_venda` | `colecao` | `tudo`), disparável pelo botão "Run" da UI do Prefect (link a partir do Evidence, que é estático e não hospeda botão próprio).
10. **Checagem de frescor ao ligar:** ao `make up`, um fluxo verifica a idade dos preços e avisa se passou de X dias.
11. **Documentação:** ADRs, catálogo de padrões, `dbt docs`, README com liga/desliga.

### Fase 2 — bot e segunda fonte

- **WhatsApp bot:**
  - **Cadastro:** usuário envia edição + número; bot busca candidatos no catálogo; se houver mais de um, envia lista interativa (até 10 opções); usuário escolhe; bot responde com imagem, dados e preço; grava no inventário.
  - **Perguntas:** menu fixo ("Faltantes", "% completo", "Valor da pasta de venda", "Atualizar preços") que consulta as métricas do Semantic Layer.
  - **Atualizar preços:** dispara o deployment Prefect; bot avisa quando terminar.
- Tailscale (dashboard remoto) + Tailscale Funnel (webhook).
- Número comercial: o **número de teste gratuito da Meta**; o número pessoal do Fernando é só o destinatário.
- **O bot só funciona com o sistema ligado.** Mensagens enviadas com tudo desligado se perdem depois que a Meta desiste de reenviar o webhook. Aceito como consequência do liga/desliga; o bot deve ser usado depois de `make up`. *Decisão do assistente.*

### Fase 3 — ideias (sem compromisso)

- Perguntas em texto livre no bot via LLM (Claude) traduzindo para consulta de métricas. **Tem custo por pergunta**: quebra o "custo zero", exige decisão.
- Comparação com Cardmarket/TCGplayer (EUR/USD, já vem no TCGdex).
- Alertas de preço (pagos via template, ou só dentro da janela de 24h).
- Reconhecimento de carta por foto.

## 7. Mapa de padrões (Data Engineering Design Patterns)

Cada padrão ganha uma página no catálogo (seção 9). Nomes seguem o livro; a verificação de nomenclatura exata contra o livro fica para o construtor.

| Capítulo | Padrão | Onde aparece |
|---|---|---|
| Ingestão | **Full Loader** | Catálogo TCGdex, espécies PokeAPI |
| Ingestão | **Incremental Loader** | Inventário do Postgres para o DuckDB |
| Ingestão | **Readiness Marker** | dbt só roda depois da ingestão terminar e marcar pronto |
| Erros | **Dead-Letter** | Páginas da Liga que falham no parse vão para uma área de quarentena |
| Erros | **Checkpointer** | Scraping da Liga retoma de onde parou se o Mac desligar no meio |
| Idempotência | **Merger** (upsert) | Inventário e catálogo |
| Idempotência | **Data Overwrite** por partição | Snapshot de preço por data de coleta |
| Idempotência | **Keyed Idempotency** | Webhook do WhatsApp deduplicado por `message_id` (a Meta reenvia) |
| Valor | **Static Joiner** | Carta + flag lendário + preço |
| Valor | **Decorator** | Idioma/condição/origem anexados à cópia possuída |
| Fluxo | **Fan-in** | Catálogo + preços + inventário alimentando os marts |
| Fluxo | **Exclusive Choice** | Bot: um candidato grava direto, vários pedem escolha |
| Armazenamento | **Horizontal Partitioner** | Parquet de preços particionado por data |
| Armazenamento | SCD2 (dbt snapshot) | Histórico de preços com `valid_from`/`valid_to`, lidando com lacunas |
| Qualidade | **Audit-Write-Audit-Publish** | Preço só é publicado após checagens (ex.: R$ 0, variação absurda) |
| Qualidade | **Schema Compatibility Enforcer** | Detectar mudança no HTML da Liga ou no schema da MYP |
| Observabilidade | **Lag / Flow Interruption Detector** | Checagem de frescor ao ligar o sistema |
| Integração | Replay a partir do bruto | Reprocessar HTML guardado com parser novo |
| Integração | Adapter (fontes e canais) | Uma staging por fonte de preço + modelo unificado `precos`; núcleo do bot independente de canal |

## 8. Requisitos não funcionais

- **UI:** toda construção ou redesenho de interface (dashboard Evidence, eventual fallback Streamlit) passa pela skill **Hallmark**. UI só está pronta depois de vista: screenshots a 1440px e 390px, checadas contra a intenção e o slop-test da Hallmark.
- **Idioma:** UI, mensagens do bot, nomes de modelos dbt, colunas e documentação em português.
- **Segurança:** tokens (MYP, Meta) fora do código, em `.env` não versionado ou gerenciador de segredos; nunca em logs.
- **Polidez com fontes:** respeitar `Crawl-delay` da Liga, `User-Agent` identificável, cache agressivo do TCGdex, atribuição visível da MYP no dashboard e nas respostas do bot.
- **Dados ausentes são normais:** toda exibição de preço mostra a data da observação.
- **Reprodutibilidade:** `make up` em máquina limpa (com Docker) sobe tudo; `make down` libera recursos.
- **TDD com suíte completa primeiro:** antes de qualquer código de produção, a suíte de testes inteira da fase é escrita e vista falhando (vermelha). Só então cada parte é implementada até ficar verde. Teste só muda depois disso se estiver errado, nunca para se ajustar ao código.
- **Testes:** testes do dbt (unicidade, não nulo, relações, faixas de preço) e testes Python onde ajudam a aprender (parser da Liga com HTML fixo, idempotência do webhook). Cada teste documenta o que prova.

## 9. Documentação exigida

1. **ADRs** (`docs/adr/NNNN-titulo.md`): contexto, opções consideradas, decisão, porquê, consequências. Um por escolha da seção 5, mais "histórico de preços próprio em vez do da Liga", "MYP primária, Liga secundária", "por que não Spark", "bot só responde (sem alertas pagos)".
2. **Catálogo de padrões** (`docs/padroes/<padrao>.md`): o que é, problema que resolve, onde está no repositório (caminhos), alternativa não usada e por quê, referência ao capítulo do livro.
3. **Ligação código ↔ documentação:** cada modelo/tarefa que implementa um padrão tem comentário apontando para a página do padrão.
4. **Gerado por ferramenta:** linhagem e colunas via `dbt docs`; DAG via UI do Prefect. Escrita manual só para o *porquê*.

## 10. Modo de trabalho (projeto de aprendizado)

- Fernando escreve o código central (ingestão, modelos dbt, padrões). O assistente explica o conceito, dá esqueleto e dicas, e revisa como mentor.
- Boilerplate (Docker Compose, Makefile, configuração) pode ser escrito pelo assistente.
- Passos pequenos, executáveis um de cada vez. Cada padrão novo vem com uma pergunta ou exercício curto.

## 11. Riscos e dependências

| # | Risco | Mitigação |
|---|---|---|
| R1 | **Token MYP negado ou demorado.** Ação do Fernando: pedir em `mypcards.com/acesso-api` no início da implementação; cobrar se não houver resposta em 1 semana. Enquanto não chega, a fase 1 avança por catálogo, inventário e Liga. | Camada de preço plugável; Liga vira primária (aceitando 6 min/carta e escopo menor). |
| R2 | HTML da Liga muda e quebra o parser. | HTML bruto guardado + replay; Schema Compatibility Enforcer avisa. |
| R3 | MYP não separa idioma: preço de uma carta PT pode vir misturado com EN. | Documentar como limitação; Liga para quando idioma importa. |
| R4 | Meta muda preços ou regras do WhatsApp. | Bot via adapter; Discord como saída. |
| R5 | De-para de códigos de edição entre TCGdex, MYP e Liga é manual. | Seed versionado; teste dbt que acusa carta possuída sem mapeamento. |
| R6 | Termos de uso da Liga não verificados por completo. | Escopo mínimo, respeito ao `robots.txt`, só cartas do próprio Fernando. |

## 12. Deixado para o construtor

- Nomes exatos de tabelas, colunas e arquivos (em português).
- Estrutura de pastas do repositório.
- Detalhes de layout do Evidence.
- Escolha entre seed CSV e CLI para cadastro manual na fase 1.
- Limiar de dias para o aviso de preço velho.
