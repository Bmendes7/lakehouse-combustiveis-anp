# Projeto D3 — Lakehouse de Preços de Combustíveis (ANP)

**Guia completo de construção: arquitetura, estrutura e passo a passo fase a fase**

**Trilha:** Engenharia de Dados (opcional) · **Stack:** Databricks Free Edition, PySpark, Delta Lake, Unity Catalog, GitHub Actions
**Repositório:** `github.com/Bmendes7/lakehouse-combustiveis-anp` · **Esforço:** ~40 h (8 a 10 semanas a 4–5 h/semana) · **Versão:** 4 (25/11/2026)

> Este documento substitui o `PLANO_D3.md` v1–v3 e o guia "D3 — Passo a passo". Tudo o que você precisa está aqui, na ordem em que vai precisar. Salve-o no repositório como `docs/PLANO.md`.
>
> **O que foi testado:** todo o código de `src/` e `tests/` deste documento foi executado com PySpark 4.3 (11 testes passando, ruff limpo), e as consultas da camada ouro rodaram com dados de exemplo. Os notebooks e os passos dentro do Databricks não foram executados, porque só rodam lá.

## Sumário

1. Visão geral
2. Fonte de dados
3. Arquitetura
4. Estrutura de pastas
5. Modelo de dados
6. Como trabalhar (convenções e Git)
7. Fases 0 a 10 (passo a passo)
8. Problemas comuns
9. Checklist e cronograma
10. Material para README, entrevista e próximos passos
11. Fontes

---

## 1. Visão geral

Construir um pipeline Lakehouse (bronze → prata → ouro) com os dados públicos da pesquisa de preços de combustíveis da ANP, cobrindo o que as vagas de engenharia de dados com Azure/Databricks pedem:

| Requisito da vaga | Onde o projeto cobre |
|---|---|
| Lógica e fundamentos de Python | Módulos em `src/` com funções testáveis |
| SQL intermediário (joins, agregações, subconsultas) | Camada ouro: janela, auto-join e subconsulta |
| PySpark (DataFrames) e Lakehouse | Bronze e prata em PySpark, tabelas Delta em camadas |
| Databricks (notebooks, jobs) | Notebooks versionados + Job agendado com 5 tarefas |
| Git/GitHub (branching, PR) | Branch por fase, PR com template, `main` protegida e CI |
| Qualidade e governança | Quarentena, constraints, comentários, tags, GRANTs, linhagem |
| Observabilidade | `log_execucoes` com status OK/ALERTA e e-mail de falha |
| Azure Data Factory | Equivalência documentada no README (seção 10) |

**Narrativa para entrevista:** "No trabalho monitoro preços com Python e SQL; no portfólio fiz o mesmo tipo de análise em escala, com PySpark e arquitetura Lakehouse."

---

## 2. Fonte de dados

- **Página oficial:** [Série Histórica de Preços de Combustíveis — ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)
- **Carga histórica:** combustíveis automotivos, dados semanais agrupados por semestre. **De 2022 em diante vêm em ZIP** (até 2021, CSV). Padrão:
  `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-AAAA-SS.zip`
- **Escopo:** 4 semestres — `2024-02`, `2025-01`, `2025-02`, `2026-01` (o mais recente publicado em 24/11/2026).
- **Carga incremental (teste da Fase 8):** arquivos mensais em CSV, publicados até outubro/2026, separados em gasolina+etanol e diesel+GNV:
  `.../arquivos/shpc/dsan/2026/10-gasolina-etanol.csv` e `.../arquivos/shpc/dsan/2026/10-diesel-gnv.csv`
- **Fora do escopo:** GLP (arquivos separados e preço por botijão).

**Limitações do Databricks Free Edition que afetam o projeto** ([fonte](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)):
- Só compute serverless, que roda em **modo ANSI** e em **UTC**.
- Internet de saída restrita a alguns domínios: o download da ANP é feito no seu PC e os arquivos sobem pelo Catalog.
- No máximo 5 tarefas de Job simultâneas por conta.
- Se a cota de uso estourar, o compute é desligado pelo resto do dia (em casos extremos, do mês); dados e configurações são mantidos.
- Contas inativas por muito tempo podem ser apagadas: tudo o que importa fica no GitHub.

---

## 3. Arquitetura

```
 ANP (ZIP semestral / CSV mensal)
        │  [PC] download com PowerShell  →  [Databricks] upload pelo Catalog
        ▼
 Volume  /Volumes/workspace/anp/raw/zip  ──extrair_zips()──►  raw/csv
        │
        │  01_bronze_ingestao  (PySpark, só arquivos novos)
        ▼
 BRONZE  bronze_precos            dado bruto (texto) + _arquivo_origem + _ingerido_em
        │
        │  02_silver_transformacao (padroniza, tipa com try_*, deduplica, MERGE)
        ▼
 PRATA   silver_precos  ──────►  quarentena_precos (registros inválidos + motivo)
        │               ──────►  log_execucoes     (volume, taxa de quarentena, status)
        │
        │  03_gold_agregacoes (SQL)
        ▼
 OURO    gold_preco_semanal_uf      (janela: LAG)
         gold_bandeira_vs_branca    (auto-join)
         gold_ranking_municipios    (subconsulta + DENSE_RANK)

 Orquestração: Job semanal  bronze → silver → gold → governanca → monitoramento
 Governança:   Unity Catalog (comentários, tags, GRANTs, linhagem), reaplicada a cada execução
 CI:           GitHub Actions roda ruff + pytest em todo PR
```

**Três decisões de arquitetura que valem explicar em entrevista:**
- **Lógica em `src/`, orquestração nos notebooks.** As funções recebem e devolvem DataFrame (ou só calculam), então dá para testá-las com pytest fora do Databricks.
- **Idempotência em todas as camadas.** Extração e ingestão só pegam arquivos novos, e a prata grava com MERGE: rodar duas vezes nunca duplica.
- **Quarentena em vez de descarte.** Registros ruins ficam guardados com o motivo, o que permite auditar e medir a qualidade.

---

## 4. Estrutura de pastas

```
lakehouse-combustiveis-anp/
├── README.md                       # vitrine do projeto (Fase 10)
├── .gitignore
├── requirements-dev.txt            # pyspark, pytest, ruff, databricks-sdk
├── pytest.ini                      # diz ao pytest onde está o código (src/)
├── ruff.toml                       # regras fixas do lint
│
├── docs/
│   ├── PLANO.md                    # este documento
│   ├── arquitetura.png             # diagrama (Fase 10)
│   ├── dicionario_de_dados.md      # colunas de cada tabela (Fase 10)
│   ├── decisoes.md                 # formato dos dados, decisões, problemas resolvidos
│   └── prints/                     # Job, linhagem, Catalog Explorer
│
├── scripts/
│   └── baixar_anp.ps1              # download dos ZIPs no Windows (Fase 2)
│
├── notebooks/                      # formato .py / .sql com "# Databricks notebook source"
│   ├── 00_setup.py                 # schema, volume, extração e inspeção (Fases 1–2)
│   ├── 01_bronze_ingestao.py       # Fase 3
│   ├── 02_silver_transformacao.py  # Fases 4–6
│   ├── 03_gold_agregacoes.sql      # Fase 7
│   ├── 04_governanca.sql           # Fase 9
│   ├── 05_monitoramento.sql        # Fase 6
│   └── 06_verificar_alerta.py      # Fase 8
│
├── src/
│   └── anp_lakehouse/
│       ├── __init__.py             # arquivo vazio: transforma a pasta em pacote Python
│       ├── config.py               # catálogo, schema, caminhos, encoding, mapa de colunas
│       ├── arquivos.py             # extração idempotente dos ZIPs
│       ├── ingestao.py             # leitura dos CSVs e detecção de arquivos novos
│       ├── transformacoes.py       # padronizar, tipar, deduplicar
│       ├── qualidade.py            # regras e separação válidos/quarentena
│       └── observabilidade.py      # registro de execução OK/ALERTA
│
├── tests/
│   ├── conftest.py                 # SparkSession local para os testes
│   ├── test_arquivos.py
│   ├── test_ingestao.py
│   ├── test_transformacoes.py
│   ├── test_qualidade.py
│   └── test_observabilidade.py
│
├── data/
│   ├── raw/                        # ZIPs e CSVs baixados (fora do Git)
│   └── sample/amostra_ca.csv       # ~200 linhas reais (dentro do Git)
│
├── jobs/
│   └── job_pipeline_anp.yml        # definição do Job exportada (Fase 8)
│
└── .github/
    ├── pull_request_template.md
    └── workflows/ci.yml            # Fase 5
```

---

## 5. Modelo de dados

**bronze_precos** — todas as colunas do CSV como texto + `_arquivo_origem` (caminho do CSV) + `_ingerido_em` (timestamp).

**silver_precos**
| Coluna | Tipo | Descrição |
|---|---|---|
| regiao | string | Sigla da região |
| uf | string | Sigla do estado |
| municipio | string | Município (capitalizado) |
| revenda | string | Nome do posto |
| cnpj_revenda | string | CNPJ do posto |
| bairro | string | Bairro |
| produto | string | Ex.: GASOLINA, ETANOL, DIESEL S10, GNV (confirmar nomes reais na Fase 7) |
| data_coleta | date (NOT NULL) | Data da coleta |
| valor_venda | decimal(10,3) (CHECK > 0) | Preço ao consumidor (R$/L; R$/m³ para GNV) |
| unidade_medida | string | Unidade informada pela ANP |
| bandeira | string | Distribuidora ou BRANCA |
| _arquivo_origem | string | Linhagem até o CSV |

**Chave de negócio:** `cnpj_revenda + produto + data_coleta`.

**quarentena_precos** — mesmas colunas da prata + `_motivo` + `_detectado_em`.

**log_execucoes** — `etapa, inicio, fim, duracao_seg, linhas_lidas, linhas_validas, linhas_quarentena, taxa_quarentena, data_mais_recente, status, detalhe`.

**Ouro**
- `gold_preco_semanal_uf`: preço médio, mínimo, máximo e nº de coletas por UF/produto/semana + variação semana a semana.
- `gold_bandeira_vs_branca`: diferença percentual entre postos com bandeira e de bandeira branca, por UF/produto/mês.
- `gold_ranking_municipios`: municípios mais caros e mais baratos por produto no último mês.

---

## 6. Como trabalhar

**Marcadores dos passos:**
- **[PC]** = no seu Windows, no terminal PowerShell do VS Code, dentro da pasta do projeto.
- **[Databricks]** = no workspace, pelo navegador.
- **[GitHub]** = no site do GitHub.

**Fluxo de Git (vale para todas as fases a partir da 1):**
- A `main` só recebe código por pull request. A partir da Fase 5, só com CI verde.
- Cada fase tem uma branch própria, criada a partir da `main` atualizada:
  ```powershell
  git checkout main; git pull; git checkout -b feat/nome-da-fase
  ```
- Mensagens de commit no padrão Conventional Commits: `feat: ingestão incremental na bronze`, `fix: caminho do volume`, `test: regras de quarentena`, `docs: dicionário de dados`.
- Edite os módulos de `src/` e os testes no VS Code e os notebooks no Databricks. Não edite o mesmo arquivo nos dois lugares sem dar pull antes.
- No fim de cada fase: commit, push, PR, merge e pull dos dois lados (PC e Databricks).

**Por que `.venv\Scripts\python` em vez de ativar a `.venv`:** em máquina corporativa, a execução de scripts `.ps1` (como o `Activate.ps1`) costuma estar bloqueada. Chamar o Python da `.venv` diretamente funciona sempre.

**"Pronto quando"** é o critério de saída de cada fase. Se ele não passar, a fase não acabou.

---

## 7. Passo a passo

### Fase 0 — Preparação no Windows (≈2 h)

**Resultado:** repositório no GitHub com a estrutura de pastas, os arquivos-base e um primeiro commit. É a única fase com commit direto na `main`.

**Passo 0.1 — [PC] Confira as ferramentas.**
```powershell
git --version
python --version   # 3.11 ou 3.12; se "não encontrado", use py --version
```

**Passo 0.2 — [GitHub] Crie o repositório vazio.** Em github.com/new, use o nome `lakehouse-combustiveis-anp` e deixe público. **Não** marque README, .gitignore nem licença.

**Passo 0.3 — [PC] Crie a estrutura de pastas.** Na pasta onde ficam seus projetos (ex.: `C:\Users\bmendes\Documents`):
```powershell
mkdir lakehouse-combustiveis-anp; cd lakehouse-combustiveis-anp
git init -b main
"docs/prints","notebooks","scripts","src/anp_lakehouse","tests","data/sample","jobs",".github/workflows" |
  ForEach-Object { New-Item -ItemType Directory -Force $_ | Out-Null }
New-Item -ItemType File src/anp_lakehouse/__init__.py, docs/prints/.gitkeep, jobs/.gitkeep, data/sample/.gitkeep
code .
```
> `.gitkeep` é um arquivo vazio só para o Git guardar a pasta (o Git não versiona pastas vazias).

**Passo 0.4 — [VS Code] Crie os arquivos-base na raiz do projeto.**

`.gitignore` — o que nunca vai para o Git:
```
__pycache__/
.pytest_cache/
.ruff_cache/
.venv/
.databricks/
spark-warehouse/
metastore_db/
*.zip
data/raw/
```

`requirements-dev.txt` — pacotes do ambiente de desenvolvimento:
```
pyspark
pytest
ruff
databricks-sdk   # só para o VS Code reconhecer spark, dbutils e display
```

`pytest.ini` — diz ao pytest que o código fica em `src/` e os testes em `tests/` (sem ele, todo teste dá `ModuleNotFoundError`):
```ini
[pytest]
pythonpath = src
testpaths = tests
```

`ruff.toml` — fixa as regras do lint (sem ele, o ruff 0.16 liga regras extras e o CI fica vermelho):
```toml
line-length = 120

[lint]
select = ["E", "F", "I"]   # erros, nomes indefinidos e ordem de imports
```

`README.md` — por enquanto só o título e uma frase; ele é reescrito na Fase 10.

`docs/PLANO.md` — cópia deste documento.

**Passo 0.5 — [PC] Crie o ambiente virtual e instale os pacotes.**
```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
```
Depois, no VS Code: **Ctrl+Shift+P → Python: Select Interpreter → `.venv`**. Os dois comandos só criam a pasta `.venv` dentro do projeto e instalam pacotes do PyPI; não mexem no Python do sistema.

**Passo 0.6 — [PC] Primeiro commit e envio.** Se o computador é o da Vigodent, configure o seu e-mail pessoal só neste repositório:
```powershell
git config user.email "seu-email-pessoal@..."
git add .
git commit -m "chore: estrutura inicial do projeto"
git remote add origin https://github.com/Bmendes7/lakehouse-combustiveis-anp.git
git push -u origin main
```

**Pronto quando:** o repositório no GitHub mostra as pastas e o `docs/PLANO.md`, e `git status` responde "nothing to commit".

---

### Fase 1 — Databricks Free Edition (≈2 h) · branch `feat/setup`

**Resultado:** workspace ligado ao GitHub, com o schema `workspace.anp` e o Volume `raw` criados por um notebook versionado.

**Passo 1.1 — Crie a conta** no Databricks Free Edition com e-mail pessoal ou login Google, nunca a conta da Vigodent.

**Passo 1.2 — [Databricks] Reconheça o ambiente.** Abra **Catalog** e confirme que existe o catálogo `workspace`, com o schema `default`. Se o nome for outro, troque `CATALOGO` no `config.py` e nos SQLs.

**Passo 1.3 — [GitHub] Gere um token.** Em **Settings → Developer settings → Personal access tokens → Fine-grained tokens**: acesso só ao repositório `lakehouse-combustiveis-anp`, permissão **Contents: Read and write**, validade de 90 dias.

**Passo 1.4 — [Databricks] Ligue o GitHub.** **Avatar → Settings → Linked accounts → Add Git credential** → GitHub → usuário `Bmendes7` → cole o token.

**Passo 1.5 — [Databricks] Crie a Git folder.** **Workspace → Home → Create → Git folder** → cole a URL do repositório. Aparece uma pasta com o mesmo conteúdo do GitHub.

**Passo 1.6 — [Databricks] Crie a branch.** Clique no nome da branch (`main`) no topo da Git folder → **Create branch** → `feat/setup`.

**Passo 1.7 — [Databricks] Crie o notebook `notebooks/00_setup`** (Python), conecte ao compute **Serverless** e cole a primeira célula:

```python
# Databricks notebook source
from databricks.sdk.runtime import dbutils, display, spark  # só para o VS Code reconhecer os nomes

CATALOGO, SCHEMA = "workspace", "anp"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.raw")

for sub in ("zip", "csv"):
    dbutils.fs.mkdirs(f"/Volumes/{CATALOGO}/{SCHEMA}/raw/{sub}")

display(dbutils.fs.ls(f"/Volumes/{CATALOGO}/{SCHEMA}/raw"))
```

Linha a linha:
- `# Databricks notebook source` — cabeçalho que faz o Databricks tratar o arquivo como notebook.
- `from databricks.sdk.runtime import ...` — no Databricks, `spark`, `dbutils` e `display` já existem; o import só tira os avisos do VS Code.
- `CREATE SCHEMA` — cria o schema `workspace.anp` (a "pasta" das tabelas). `IF NOT EXISTS` permite rodar de novo sem erro.
- `CREATE VOLUME` — cria o Volume `raw`, o espaço para **arquivos** (ZIP/CSV); tabelas guardam **dados**.
- `dbutils.fs.mkdirs` — cria as subpastas `zip/` (o que você sobe) e `csv/` (o que é extraído). **Atenção:** em caminho de arquivo o separador é `/` (`/Volumes/workspace/anp/raw`); o ponto só vale em nomes SQL (`workspace.anp`).
- `display(dbutils.fs.ls(...))` — lista o Volume para você conferir as duas pastas.

**Passo 1.8 — [Databricks] Commit pelo workspace.** No diálogo da Git folder, marque o `00_setup`, escreva `feat: cria schema e volume do projeto` → **Commit & Push**.

**Passo 1.9 — [GitHub] PR e merge.** Abra o PR `feat/setup → main` e faça o merge (ainda não há CI).

**Passo 1.10 — Atualize os dois lados.** Databricks: volte a Git folder para `main` → **Pull**. PC: `git checkout main; git pull`.

**Pronto quando:** o Catalog mostra `workspace.anp` com o Volume `raw` e as subpastas `zip/` e `csv/`, e o primeiro PR está mergeado.

---

### Fase 2 — Dados da ANP (≈3 h) · branch `feat/dados`

**Resultado:** quatro semestres extraídos no Volume, o formato real dos arquivos anotado e o extrator testado.

**Passo 2.1 — [PC] Crie a branch.** `git checkout main; git pull; git checkout -b feat/dados`

**Passo 2.2 — [PC] Crie `scripts/baixar_anp.ps1` e rode.** O script só baixa quatro arquivos públicos da ANP para `data/raw/` (ignorada pelo Git); não executa nada nem altera o sistema.
```powershell
$base = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca"
$semestres = "2024-02","2025-01","2025-02","2026-01"
New-Item -ItemType Directory -Force data/raw | Out-Null
foreach ($s in $semestres) {
    $arq = "ca-$s.zip"
    Invoke-WebRequest "$base/$arq" -OutFile "data/raw/$arq"
    "{0}: {1:N1} MB" -f $arq, ((Get-Item "data/raw/$arq").Length / 1MB)
}
```
> A lista usa **vírgulas**. Com `;`, o PowerShell encerra o comando e a lista fica só com `"2024-02"`, sem dar erro.

Se a execução de `.ps1` estiver bloqueada, cole o conteúdo direto no terminal. Ao final devem aparecer 4 linhas com o tamanho de cada ZIP.

**Passo 2.3 — [PC] Inspecione um arquivo antes de subir.**
```powershell
Expand-Archive data/raw/ca-2026-01.zip -DestinationPath data/raw/inspecao -Force
Get-ChildItem data/raw/inspecao
Get-Content (Get-ChildItem data/raw/inspecao/*.csv)[0] -TotalCount 3 -Encoding UTF8
```
Anote em `docs/decisoes.md`:
- (a) se há um só CSV dentro do ZIP;
- (b) o separador (esperado: `;`);
- (c) os nomes exatos das colunas;
- (d) a codificação: se os acentos aparecem certos com `-Encoding UTF8`, é UTF-8; se aparece `Munic�pio`, é Latin-1 e o `config.py` precisará de `ENCODING_CSV = "ISO-8859-1"`.

**Passo 2.4 — [PC] Gere a amostra para os testes** (200 linhas, que vão para o Git):
```powershell
Get-Content (Get-ChildItem data/raw/inspecao/*.csv)[0] -TotalCount 201 |
  Set-Content data/sample/amostra_ca.csv -Encoding utf8
```

**Passo 2.5 — [Databricks] Suba os ZIPs.** **Catalog → workspace → anp → raw → pasta `zip/` → Upload to this volume** → arraste os quatro ZIPs.

**Passo 2.6 — [PC] Crie `src/anp_lakehouse/config.py`** — o lugar único de nomes, caminhos e mapeamento de colunas:
```python
CATALOGO = "workspace"
SCHEMA = "anp"
VOLUME_RAW = f"/Volumes/{CATALOGO}/{SCHEMA}/raw"
PASTA_ZIP = f"{VOLUME_RAW}/zip"
PASTA_CSV = f"{VOLUME_RAW}/csv"
ENCODING_CSV = "UTF-8"   # troque para "ISO-8859-1" se a Etapa 2 mostrou acentos quebrados


def tabela(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


# Cabeçalho original da ANP -> nome padronizado (confira com o printSchema da Etapa 2)
MAPA_COLUNAS = {
    "Regiao - Sigla": "regiao",
    "Estado - Sigla": "uf",
    "Municipio": "municipio",
    "Revenda": "revenda",
    "CNPJ da Revenda": "cnpj_revenda",
    "Bairro": "bairro",
    "Produto": "produto",
    "Data da Coleta": "data_coleta",
    "Valor de Venda": "valor_venda",
    "Unidade de Medida": "unidade_medida",
    "Bandeira": "bandeira",
}
```
Confira o `MAPA_COLUNAS` contra os nomes anotados no passo 2.3 e corrija o que for diferente.

**Passo 2.7 — [PC] Crie `src/anp_lakehouse/arquivos.py`** — extrai o CSV de cada ZIP ainda não extraído:
```python
import shutil
import zipfile
from pathlib import Path


def extrair_zips(pasta_zip: str, pasta_csv: str) -> list[str]:
    """Extrai o CSV de cada .zip ainda não extraído. Devolve os CSVs novos."""
    destino = Path(pasta_csv)
    destino.mkdir(parents=True, exist_ok=True)
    novos = []
    for zip_path in sorted(Path(pasta_zip).glob("*.zip")):
        # nome final = nome do zip, para rastrear o semestre (ca-2025-02.csv)
        alvo = destino / f"{zip_path.stem}.csv"
        if alvo.exists():
            continue
        with zipfile.ZipFile(zip_path) as zf:
            csvs = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise ValueError(f"{zip_path.name}: esperava 1 CSV, achei {len(csvs)}")
            # grava num .tmp e só renomeia no fim: uma falha no meio não deixa CSV truncado
            temporario = alvo.with_suffix(".csv.tmp")
            with zf.open(csvs[0]) as origem, open(temporario, "wb") as saida:
                shutil.copyfileobj(origem, saida, length=1024 * 1024)  # 1 MB por vez
            temporario.replace(alvo)
        novos.append(str(alvo))
    return novos
```
Como funciona:
- `glob("*.zip")` lista só os ZIPs; `sorted` mantém a ordem cronológica (`ca-AAAA-SS`).
- O CSV de saída recebe o nome do ZIP (`ca-2026-01.csv`), o que guarda o semestre na linhagem.
- `if alvo.exists(): continue` torna a função idempotente: rodar de novo não refaz nada.
- O nome dentro do ZIP é filtrado com `endswith(".csv")` — **sem asterisco**; o `*` só funciona no `glob`.
- A cópia vai para um `.tmp` e só vira `.csv` no fim (`replace`): se cair no meio, a próxima execução refaz o arquivo.
- `copyfileobj` copia em blocos de 1 MB, sem carregar o semestre inteiro na memória.

**Passo 2.8 — [PC] Crie `tests/test_arquivos.py`:**
```python
import zipfile

from anp_lakehouse.arquivos import extrair_zips


def test_extrai_uma_vez_so(tmp_path):
    pasta_zip, pasta_csv = tmp_path / "zip", tmp_path / "csv"
    pasta_zip.mkdir()
    with zipfile.ZipFile(pasta_zip / "ca-2025-02.zip", "w") as zf:
        zf.writestr("Preços semestrais - AUTOMOTIVOS_2025.02.csv", "Regiao - Sigla;Valor de Venda\nSE;6,19\n")

    primeira = extrair_zips(str(pasta_zip), str(pasta_csv))
    segunda = extrair_zips(str(pasta_zip), str(pasta_csv))

    assert [p.replace("\\", "/").split("/")[-1] for p in primeira] == ["ca-2025-02.csv"]
    assert segunda == []
    assert "6,19" in (pasta_csv / "ca-2025-02.csv").read_text()
```
O teste segue o padrão Arrange/Act/Assert: monta um ZIP falso numa pasta temporária (`tmp_path`, criada pelo pytest), chama a função duas vezes e confere o nome, a idempotência e o conteúdo. Rode:
```powershell
.venv\Scripts\python -m pytest tests/test_arquivos.py -v
```
Esperado: `1 passed`.

**Passo 2.9 — [PC] Envie a branch.** Commit e push. No Databricks, troque a Git folder para `feat/dados` → **Pull**.

**Passo 2.10 — [Databricks] Complete o `00_setup` com mais duas células:**
```python
# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from anp_lakehouse.arquivos import extrair_zips
from anp_lakehouse.config import PASTA_CSV, PASTA_ZIP

print(extrair_zips(PASTA_ZIP, PASTA_CSV))

# COMMAND ----------

df = spark.read.option("header", True).option("sep", ";").csv(f"{PASTA_CSV}/")
df.printSchema()
print("linhas:", df.count())
display(df.groupBy("Produto").count())
```
Linha a linha:
- `# COMMAND ----------` divide as células: rode esta só depois de subir os ZIPs.
- `sys.path.append(os.path.abspath("../src"))` — o notebook está em `notebooks/`; `../src` sobe um nível e entra em `src/`, para o Python achar o pacote `anp_lakehouse`.
- `print(extrair_zips(...))` — na primeira vez lista 4 CSVs; numa segunda execução, `[]`.
- `spark.read...csv(f"{PASTA_CSV}/")` — lê todos os CSVs de uma vez; `header` diz que a 1ª linha tem nomes; `sep=";"` é o separador da ANP. Tudo chega como texto.
- `printSchema()` — mostra as colunas, para conferir o `MAPA_COLUNAS`.
- `count()` — **anote o número**: é a referência para validar a bronze na Fase 3.
- `groupBy("Produto").count()` — mostra os nomes reais dos produtos.

**Passo 2.11 — Registre, commit, PR e merge.** Complete o `docs/decisoes.md` com o `printSchema`, as linhas por semestre e os produtos.

**Pronto quando:** 4 CSVs em `raw/csv/`, `pytest` passando, formato anotado no `decisoes.md` e `MAPA_COLUNAS` igual ao cabeçalho real.

---

### Fase 3 — Bronze (≈4 h) · branch `feat/bronze`

**Resultado:** a tabela `bronze_precos` com o dado bruto e a linhagem até o arquivo; a segunda execução não duplica nada.

**Passo 3.1 — [PC] Crie a branch.** `git checkout main; git pull; git checkout -b feat/bronze`

**Passo 3.2 — [PC] Crie `src/anp_lakehouse/ingestao.py`:**
```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from anp_lakehouse.config import ENCODING_CSV


def normalizar_caminho(caminho: str) -> str:
    """dbutils.fs.ls devolve 'dbfs:/Volumes/...'; deixa todo caminho no formato '/Volumes/...'."""
    return caminho.removeprefix("dbfs:")


def arquivos_novos(todos: list[str], ja_processados: set[str]) -> list[str]:
    processados = {normalizar_caminho(c) for c in ja_processados}
    return sorted({normalizar_caminho(c) for c in todos} - processados)


def ler_csvs(spark: SparkSession, caminhos: list[str]) -> DataFrame:
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("encoding", ENCODING_CSV)
            .csv(caminhos)
            .withColumn("_arquivo_origem",
                        F.regexp_replace(F.col("_metadata.file_path"), "^(dbfs:|file:)", ""))
            .withColumn("_ingerido_em", F.current_timestamp()))
```
Como funciona:
- `normalizar_caminho` — o `dbutils.fs.ls` devolve `dbfs:/Volumes/...` e o Spark devolve `/Volumes/...`; sem padronizar, os arquivos já processados nunca "batem" e a bronze duplica.
- `arquivos_novos` — diferença de conjuntos: todos os CSVs menos os já processados.
- `ler_csvs` — lê só a lista recebida; `_metadata.file_path` é uma coluna oculta do Spark com o caminho de cada linha, guardada como `_arquivo_origem` (linhagem).

**Passo 3.3 — [PC] Crie `tests/conftest.py`** — a fixture `spark` usada pelos testes de PySpark:
```python
import pytest


@pytest.fixture(scope="session")
def spark():
    # import aqui dentro: os testes que não usam Spark rodam mesmo sem Java instalado
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[1]").appName("testes-anp").getOrCreate()
```

**Passo 3.4 — [PC] Crie `tests/test_ingestao.py`:**
```python
from anp_lakehouse.ingestao import arquivos_novos, ler_csvs


def test_so_arquivos_novos_e_ignora_prefixo_dbfs():
    todos = ["dbfs:/Volumes/workspace/anp/raw/csv/ca-2025-02.csv",
             "dbfs:/Volumes/workspace/anp/raw/csv/ca-2026-01.csv"]
    processados = {"/Volumes/workspace/anp/raw/csv/ca-2025-02.csv"}

    assert arquivos_novos(todos, processados) == ["/Volumes/workspace/anp/raw/csv/ca-2026-01.csv"]


def test_ler_csvs_guarda_linhagem(spark, tmp_path):
    arq = tmp_path / "ca-2026-01.csv"
    arq.write_text("Produto;Valor de Venda\nGASOLINA;6,19\n", encoding="utf-8")

    linha = ler_csvs(spark, [str(arq)]).first()

    assert linha["Valor de Venda"] == "6,19"
    assert linha["_arquivo_origem"].endswith("ca-2026-01.csv")
    assert not linha["_arquivo_origem"].startswith("file:")
```
No Windows, rode só os testes sem Spark (os de Spark precisam de Java e rodam no CI a partir da Fase 5):
```powershell
.venv\Scripts\python -m pytest tests -k "prefixo or extrai" -v
```

**Passo 3.5 — [PC] Envie a branch.** Commit e push. No Databricks, troque para `feat/bronze` → **Pull**.

**Passo 3.6 — [Databricks] Crie `notebooks/01_bronze_ingestao`:**
```python
# Databricks notebook source
from databricks.sdk.runtime import dbutils, spark

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")  # serverless usa UTC

import os
import sys

sys.path.append(os.path.abspath("../src"))
from anp_lakehouse.config import PASTA_CSV, tabela
from anp_lakehouse.ingestao import arquivos_novos, ler_csvs

todos = [f.path for f in dbutils.fs.ls(PASTA_CSV) if f.path.endswith(".csv")]

if spark.catalog.tableExists(tabela("bronze_precos")):
    processados = {r[0] for r in spark.table(tabela("bronze_precos"))
                                    .select("_arquivo_origem").distinct().collect()}
else:
    processados = set()

novos = arquivos_novos(todos, processados)
print(f"{len(novos)} arquivo(s) novo(s):", novos)

if novos:
    (ler_csvs(spark, novos)
       .write.mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(tabela("bronze_precos")))
```
- `mode("append")` acrescenta sem apagar o que já existe.
- `mergeSchema` aceita uma coluna nova caso a ANP mude o arquivo.

**Passo 3.7 — [Databricks] Rode duas vezes seguidas.** A primeira imprime 4 arquivos; a segunda, `0 arquivo(s) novo(s)`. Confira:
```sql
SELECT _arquivo_origem, COUNT(*) AS linhas
FROM workspace.anp.bronze_precos
GROUP BY ALL ORDER BY 1;
```
O total tem que bater com o `count()` da Fase 2.

**Passo 3.8 — Commit, PR e merge.**

**Pronto quando:** 4 linhas no `GROUP BY`, a soma igual à da Fase 2 e a segunda execução sem efeito.

---

### Fase 4 — Prata (≈5 h) · branch `feat/silver`

**Resultado:** `silver_precos` tipada, sem duplicatas na chave e gravada com MERGE (reprocessar não muda nada).

> **Modo ANSI:** no serverless, `to_date` ou `cast` com valor inválido **derruba o job** em vez de devolver nulo (testado: `31/02/2026` gera `DateTimeException`). Por isso a tipagem usa `try_to_date` e `try_cast`: o valor inválido vira nulo e vai para a quarentena na Fase 5.

**Passo 4.1 — [PC] Crie a branch.** `git checkout main; git pull; git checkout -b feat/silver`

**Passo 4.2 — [PC] Crie `src/anp_lakehouse/transformacoes.py`:**
```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from anp_lakehouse.config import MAPA_COLUNAS

CHAVE = ["cnpj_revenda", "produto", "data_coleta"]


def padronizar_colunas(df: DataFrame) -> DataFrame:
    for original, novo in MAPA_COLUNAS.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, novo)
    return df.select(*MAPA_COLUNAS.values(), "_arquivo_origem")


def tipar(df: DataFrame) -> DataFrame:
    # try_*: valor inválido vira NULL (e vai para a quarentena) em vez de derrubar o job no modo ANSI
    return (df
        .withColumn("municipio", F.initcap(F.trim("municipio")))
        .withColumn("produto", F.upper(F.trim("produto")))
        .withColumn("bandeira", F.upper(F.trim("bandeira")))
        .withColumn("data_coleta", F.expr("try_to_date(trim(data_coleta), 'dd/MM/yyyy')"))
        .withColumn("valor_venda",
                    F.expr("try_cast(replace(trim(valor_venda), ',', '.') AS DECIMAL(10,3))")))


def deduplicar(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(CHAVE)
```
Como funciona:
- `padronizar_colunas` — troca os nomes da ANP pelos nomes do projeto e descarta colunas não mapeadas.
- `tipar` — limpa espaços, padroniza maiúsculas, converte a data `dd/MM/yyyy` e o preço com vírgula decimal (`6,19` → `6.190`).
- `deduplicar` — mantém uma linha por chave de negócio.

**Passo 4.3 — [PC] Crie `tests/test_transformacoes.py`:**
```python
from anp_lakehouse.transformacoes import deduplicar, tipar

COLUNAS = ["municipio", "produto", "bandeira", "data_coleta", "valor_venda"]


def test_tipar_converte_virgula_e_data(spark):
    df = spark.createDataFrame(
        [(" rio de janeiro ", "gasolina", "ipiranga", "05/01/2026", "6,19")], COLUNAS)
    linha = tipar(df).first()
    assert str(linha.valor_venda) == "6.190"
    assert str(linha.data_coleta) == "2026-01-05"
    assert linha.municipio == "Rio De Janeiro"
    assert linha.produto == "GASOLINA"


def test_tipar_valor_invalido_vira_nulo_sem_quebrar(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")   # como no serverless do Databricks
    df = spark.createDataFrame([("Niteroi", "ETANOL", "BRANCA", "31/02/2026", "abc")], COLUNAS)
    linha = tipar(df).first()
    assert linha.data_coleta is None
    assert linha.valor_venda is None


def test_deduplicar_pela_chave(spark):
    df = spark.createDataFrame([("1", "GASOLINA", "2026-01-05", 1), ("1", "GASOLINA", "2026-01-05", 2)],
                               ["cnpj_revenda", "produto", "data_coleta", "x"])
    assert deduplicar(df).count() == 1
```

**Passo 4.4 — [PC] Envie a branch.** Commit e push. No Databricks, troque para `feat/silver` → **Pull**.

**Passo 4.5 — [Databricks] Crie `notebooks/02_silver_transformacao` (primeira versão, sem quarentena):**
```python
# Databricks notebook source
from databricks.sdk.runtime import spark

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

import os
import sys

sys.path.append(os.path.abspath("../src"))
from delta.tables import DeltaTable

from anp_lakehouse.config import tabela
from anp_lakehouse.transformacoes import CHAVE, deduplicar, padronizar_colunas, tipar

tratado = deduplicar(tipar(padronizar_colunas(spark.table(tabela("bronze_precos")))))

if not spark.catalog.tableExists(tabela("silver_precos")):
    tratado.write.saveAsTable(tabela("silver_precos"))
else:
    condicao = " AND ".join(f"alvo.{c} = origem.{c}" for c in CHAVE)
    (DeltaTable.forName(spark, tabela("silver_precos")).alias("alvo")
        .merge(tratado.alias("origem"), condicao)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
```
- Na primeira execução, a tabela é criada.
- Nas seguintes, o **MERGE** compara pela chave: o que já existe é atualizado, o que é novo é inserido. Por isso reprocessar não duplica.

**Passo 4.6 — [Databricks] Valide:**
```sql
-- 1) duplicatas na chave: tem que voltar vazio
SELECT cnpj_revenda, produto, data_coleta, COUNT(*) FROM workspace.anp.silver_precos
GROUP BY ALL HAVING COUNT(*) > 1;

-- 2) tipos: data_coleta DATE e valor_venda DECIMAL(10,3)
DESCRIBE TABLE workspace.anp.silver_precos;

-- 3) nulos gerados pela tipagem (vão para a quarentena na Fase 5)
SELECT COUNT_IF(data_coleta IS NULL) AS datas_nulas, COUNT_IF(valor_venda IS NULL) AS precos_nulos
FROM workspace.anp.silver_precos;
```

**Passo 4.7 — [Databricks] Teste a idempotência.** Anote o `COUNT(*)` da prata, rode o notebook de novo e confira que não mudou. Em **Catalog → silver_precos → History**, a segunda versão é um `MERGE` com `numTargetRowsInserted = 0`.

**Passo 4.8 — Commit, PR e merge.**

**Pronto quando:** a consulta 1 volta vazia, os tipos estão certos e a segunda execução não insere linhas.

---

### Fase 5 — Qualidade, testes e CI (≈4 h) · branch `feat/qualidade`

**Resultado:** registros ruins vão para `quarentena_precos` com o motivo, a prata ganha constraints e todo PR roda os testes no GitHub Actions. A partir daqui, a `main` só aceita merge com CI verde.

**Regras de qualidade** (registre em `docs/decisoes.md`):

| Regra | Motivo gravado na quarentena |
|---|---|
| `data_coleta` não nula e válida | `data_invalida` |
| `uf` e `cnpj_revenda` preenchidos | `campo_obrigatorio_nulo` |
| `valor_venda` entre R$ 0,50 e R$ 30,00 | `preco_fora_da_faixa` |

> A quarentena não é enfeite: uma `data_coleta` nula na chave nunca casa no MERGE, e a linha seria reinserida a cada execução.

**Passo 5.1 — [PC] Crie a branch.** `git checkout main; git pull; git checkout -b feat/qualidade`

**Passo 5.2 — [PC] Crie `src/anp_lakehouse/qualidade.py`:**
```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PRECO_MIN, PRECO_MAX = 0.5, 30.0


def _motivo() -> F.Column:
    return (F.when(F.col("data_coleta").isNull(), "data_invalida")
             .when(F.col("uf").isNull() | F.col("cnpj_revenda").isNull(),
                   "campo_obrigatorio_nulo")
             .when(F.col("valor_venda").isNull()
                   | ~F.col("valor_venda").between(PRECO_MIN, PRECO_MAX),
                   "preco_fora_da_faixa"))


def separar_validos(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    marcado = df.withColumn("_motivo", _motivo())
    validos = marcado.filter(F.col("_motivo").isNull()).drop("_motivo")
    invalidos = (marcado.filter(F.col("_motivo").isNotNull())
                        .withColumn("_detectado_em", F.current_timestamp()))
    return validos, invalidos
```
- `_motivo` devolve o primeiro motivo que se aplica à linha, ou nulo se ela está boa.
- `separar_validos` divide o DataFrame em dois: válidos (sem motivo) e inválidos (com motivo e horário de detecção).

**Passo 5.3 — [PC] Crie `tests/test_qualidade.py`:**
```python
from datetime import date
from decimal import Decimal

from anp_lakehouse.qualidade import separar_validos

COLUNAS = ["uf", "cnpj_revenda", "produto", "data_coleta", "valor_venda"]


def test_preco_absurdo_vai_para_quarentena(spark):
    df = spark.createDataFrame([
        ("RJ", "111", "GASOLINA", date(2026, 1, 5), Decimal("6.190")),
        ("RJ", "222", "GASOLINA", date(2026, 1, 5), Decimal("619.000")),
        ("RJ", None, "GASOLINA", date(2026, 1, 5), Decimal("6.190")),
    ], COLUNAS)
    validos, invalidos = separar_validos(df)
    assert validos.count() == 1
    assert sorted(r["_motivo"] for r in invalidos.collect()) == ["campo_obrigatorio_nulo", "preco_fora_da_faixa"]
```

**Passo 5.4 — [PC] Crie o CI em `.github/workflows/ci.yml`:**
```yaml
name: CI
on:
  pull_request:
  push:
    branches: [main]

jobs:
  testes:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - run: pip install -r requirements-dev.txt
      - run: ruff check src tests
      - run: pytest -v
```
O CI roda numa máquina Linux do GitHub com Java 17, que o PySpark precisa, então os testes de Spark rodam lá mesmo sem Java no seu PC.

**Passo 5.5 — [PC] Crie `.github/pull_request_template.md`:**
```markdown
## O que muda
-

## Como testei
- [ ] CI verde (ruff + pytest)
- [ ] Rodei o notebook no Databricks

## Tabelas afetadas
-
```

**Passo 5.6 — [PC] Rode o lint, envie e abra o PR.**
```powershell
.venv\Scripts\python -m ruff check --fix src tests
git add .; git commit -m "feat: regras de qualidade e CI"; git push -u origin feat/qualidade
```
Abra o PR e acompanhe a aba **Actions**: todos os testes devem passar. Ainda não faça o merge.

**Passo 5.7 — [Databricks] Pull na `feat/qualidade` e troque o `02_silver_transformacao` por esta versão:**
```python
# Databricks notebook source
from databricks.sdk.runtime import spark

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

import os
import sys

sys.path.append(os.path.abspath("../src"))
from delta.tables import DeltaTable

from anp_lakehouse.config import tabela
from anp_lakehouse.qualidade import separar_validos
from anp_lakehouse.transformacoes import CHAVE, deduplicar, padronizar_colunas, tipar

tratado = deduplicar(tipar(padronizar_colunas(spark.table(tabela("bronze_precos")))))
validos, invalidos = separar_validos(tratado)

if not spark.catalog.tableExists(tabela("silver_precos")):
    validos.write.saveAsTable(tabela("silver_precos"))
    spark.sql(f"ALTER TABLE {tabela('silver_precos')} ALTER COLUMN data_coleta SET NOT NULL")
    spark.sql(f"ALTER TABLE {tabela('silver_precos')} ADD CONSTRAINT preco_positivo CHECK (valor_venda > 0)")
else:
    condicao = " AND ".join(f"alvo.{c} = origem.{c}" for c in CHAVE)
    (DeltaTable.forName(spark, tabela("silver_precos")).alias("alvo")
        .merge(validos.alias("origem"), condicao)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

(invalidos.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(tabela("quarentena_precos")))
print("válidos:", validos.count(), "| quarentena:", invalidos.count())
```
- As constraints (`NOT NULL` e `CHECK`) são criadas junto com a tabela, para o setup ser reproduzível. A partir daí, o Delta recusa qualquer gravação que as viole.
- A quarentena é sobrescrita a cada execução: ela reflete o estado atual da bronze.

**Passo 5.8 — [Databricks] Recrie a prata uma vez** (ela pode ter linhas com data nula da Fase 4; como é derivada da bronze, recriar é seguro):
```sql
DROP TABLE IF EXISTS workspace.anp.silver_precos;
```
Rode o notebook duas vezes e confira:
```sql
SELECT _motivo, COUNT(*) FROM workspace.anp.quarentena_precos GROUP BY ALL;
SHOW TBLPROPERTIES workspace.anp.silver_precos;   -- procure delta.constraints.preco_positivo
```

**Passo 5.9 — Commit do notebook (pelo Databricks, na mesma branch), CI verde e merge.**

**Passo 5.10 — [GitHub] Proteja a `main`.** **Settings → Branches → Add classic branch protection rule** → padrão `main` → marque **Require a pull request before merging** (0 aprovações) e **Require status checks to pass** → selecione `testes`.

**Pronto quando:** CI verde, `main` protegida, quarentena com motivos legíveis e a contagem da prata estável entre execuções.

> Quarentena acima de ~5% quase sempre é problema de formato (padrão de data, `MAPA_COLUNAS`), não de dado ruim.

---

### Fase 6 — Observabilidade (≈3 h) · branch `feat/observabilidade`

**Resultado:** cada execução da prata grava uma linha em `log_execucoes` com status `OK` ou `ALERTA`.

| Sinal | Regra de alerta | O que indica |
|---|---|---|
| Volume | `linhas_lidas = 0` | A ingestão não trouxe nada |
| Qualidade | `taxa_quarentena > 5%` | A ANP mudou o formato ou os dados vieram ruins |
| Atualização | `data_mais_recente` com mais de 200 dias | Arquivos novos não estão chegando |

**Passo 6.1 — [PC] Crie a branch.** `git checkout main; git pull; git checkout -b feat/observabilidade`

**Passo 6.2 — [PC] Crie `src/anp_lakehouse/observabilidade.py`:**
```python
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession

from anp_lakehouse.config import tabela

LIMITE_QUARENTENA = 0.05
LIMITE_DIAS_SEM_DADO_NOVO = 200

# Schema explícito: sem ele, uma execução OK (detalhe = None) quebra o createDataFrame
SCHEMA_LOG = ("etapa STRING, inicio TIMESTAMP, fim TIMESTAMP, duracao_seg DOUBLE, "
              "linhas_lidas BIGINT, linhas_validas BIGINT, linhas_quarentena BIGINT, "
              "taxa_quarentena DOUBLE, data_mais_recente DATE, status STRING, detalhe STRING")


def montar_registro(etapa: str, lidas: int, validas: int, quarentena: int,
                    data_mais_recente: date | None,
                    inicio: datetime, fim: datetime,
                    hoje: date | None = None) -> dict:
    """Função pura: só calcula, não toca no Spark."""
    hoje = hoje or date.today()
    taxa = round(quarentena / lidas, 4) if lidas else 0.0

    alertas = []
    if lidas == 0:
        alertas.append("nenhuma linha lida")
    if taxa > LIMITE_QUARENTENA:
        alertas.append(f"taxa de quarentena {taxa:.1%} acima de {LIMITE_QUARENTENA:.0%}")
    if data_mais_recente is None or (hoje - data_mais_recente).days > LIMITE_DIAS_SEM_DADO_NOVO:
        alertas.append("dados desatualizados")

    return {
        "etapa": etapa, "inicio": inicio, "fim": fim,
        "duracao_seg": round((fim - inicio).total_seconds(), 1),
        "linhas_lidas": lidas, "linhas_validas": validas, "linhas_quarentena": quarentena,
        "taxa_quarentena": float(taxa), "data_mais_recente": data_mais_recente,
        "status": "ALERTA" if alertas else "OK",
        "detalhe": "; ".join(alertas) or None,
    }


def registro_para_df(spark: SparkSession, registro: dict) -> DataFrame:
    return spark.createDataFrame([registro], schema=SCHEMA_LOG)


def registrar_execucao(spark: SparkSession, registro: dict) -> None:
    registro_para_df(spark, registro).write.mode("append").saveAsTable(tabela("log_execucoes"))
```
- `montar_registro` é uma função pura: só calcula, sem Spark, então é fácil de testar.
- O `SCHEMA_LOG` explícito é obrigatório: numa execução OK, `detalhe` é `None`, e sem schema o Spark não consegue deduzir o tipo (`CANNOT_DETERMINE_TYPE`, erro reproduzido nos testes).

**Passo 6.3 — [PC] Crie `tests/test_observabilidade.py`:**
```python
from datetime import date, datetime

from anp_lakehouse.observabilidade import montar_registro, registro_para_df

INICIO = datetime(2026, 11, 16, 6, 0, 0)
FIM = datetime(2026, 11, 16, 6, 3, 30)
HOJE = date(2026, 11, 16)


def test_execucao_saudavel_fica_ok():
    r = montar_registro("silver", 1000, 990, 10, date(2026, 10, 1), INICIO, FIM, HOJE)
    assert r["status"] == "OK"
    assert r["duracao_seg"] == 210.0


def test_quarentena_alta_gera_alerta():
    r = montar_registro("silver", 1000, 900, 100, date(2026, 10, 1), INICIO, FIM, HOJE)
    assert r["status"] == "ALERTA"
    assert "quarentena" in r["detalhe"]


def test_dado_antigo_gera_alerta():
    r = montar_registro("silver", 1000, 990, 10, date(2025, 1, 1), INICIO, FIM, HOJE)
    assert "desatualizados" in r["detalhe"]


def test_registro_ok_vira_dataframe_mesmo_com_detalhe_nulo(spark):
    r = montar_registro("silver", 1000, 990, 10, date(2026, 10, 1), INICIO, FIM, HOJE)
    linha = registro_para_df(spark, r).first()
    assert linha.status == "OK"
    assert linha.detalhe is None
```
Rode `ruff check --fix`, faça commit e push. O CI deve mostrar **11 testes passando**.

**Passo 6.4 — [Databricks] Pull e troque o `02_silver_transformacao` pela versão final:**
```python
# Databricks notebook source
from databricks.sdk.runtime import spark

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.append(os.path.abspath("../src"))


def agora():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)


inicio = agora()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from anp_lakehouse.config import tabela
from anp_lakehouse.observabilidade import montar_registro, registrar_execucao
from anp_lakehouse.qualidade import separar_validos
from anp_lakehouse.transformacoes import CHAVE, deduplicar, padronizar_colunas, tipar

tratado = deduplicar(tipar(padronizar_colunas(spark.table(tabela("bronze_precos")))))
validos, invalidos = separar_validos(tratado)

if not spark.catalog.tableExists(tabela("silver_precos")):
    validos.write.saveAsTable(tabela("silver_precos"))
    spark.sql(f"ALTER TABLE {tabela('silver_precos')} ALTER COLUMN data_coleta SET NOT NULL")
    spark.sql(f"ALTER TABLE {tabela('silver_precos')} ADD CONSTRAINT preco_positivo CHECK (valor_venda > 0)")
else:
    condicao = " AND ".join(f"alvo.{c} = origem.{c}" for c in CHAVE)
    (DeltaTable.forName(spark, tabela("silver_precos")).alias("alvo")
        .merge(validos.alias("origem"), condicao)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

(invalidos.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(tabela("quarentena_precos")))

# COMMAND ----------

lidas, n_validos, n_quarentena = tratado.count(), validos.count(), invalidos.count()
data_max = validos.agg(F.max("data_coleta")).first()[0]

registro = montar_registro("silver", lidas, n_validos, n_quarentena, data_max, inicio, agora())
registrar_execucao(spark, registro)
print(registro["status"], registro["detalhe"] or "")
```
O `agora()` usa o fuso de São Paulo porque o `datetime.now()` do serverless está em UTC.

**Passo 6.5 — [Databricks] Crie `notebooks/05_monitoramento` (SQL):**
```sql
-- Databricks notebook source
-- Últimas execuções
SELECT etapa, inicio, duracao_seg, linhas_lidas, taxa_quarentena, status, detalhe
FROM workspace.anp.log_execucoes
ORDER BY inicio DESC
LIMIT 10;

-- COMMAND ----------
-- Volume fora do padrão: execução com menos da metade da média das 5 anteriores
WITH hist AS (
  SELECT inicio, linhas_lidas,
         AVG(linhas_lidas) OVER (ORDER BY inicio ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS media_anterior
  FROM workspace.anp.log_execucoes
  WHERE etapa = 'silver'
)
SELECT * FROM hist
WHERE media_anterior IS NOT NULL AND linhas_lidas < 0.5 * media_anterior;

-- COMMAND ----------
-- Motivos de quarentena mais frequentes
SELECT _motivo, COUNT(*) AS qtd
FROM workspace.anp.quarentena_precos
GROUP BY _motivo
ORDER BY qtd DESC;
```

**Passo 6.6 — [Databricks] Rode a prata duas vezes.** As duas devem gravar `OK` (os dados vão até junho/2026, dentro do limite de 200 dias).

**Passo 6.7 — [Databricks] Prove que o alerta funciona** (numa célula avulsa, depois apague):
```python
r = montar_registro("teste", 1000, 800, 200, data_max, inicio, agora())
registrar_execucao(spark, r)
display(spark.sql("SELECT etapa, status, detalhe FROM workspace.anp.log_execucoes WHERE etapa = 'teste'"))
spark.sql("DELETE FROM workspace.anp.log_execucoes WHERE etapa = 'teste'")
```
Esperado: `ALERTA` com o detalhe `taxa de quarentena 20.0% acima de 5%`.

**Passo 6.8 — Commit, PR com CI verde e merge.**

**Pronto quando:** 11 testes verdes no CI, pelo menos duas linhas `OK` no log e o alerta de teste comprovado.

---

### Fase 7 — Ouro (≈5 h) · branch `feat/gold`

**Resultado:** três tabelas analíticas em SQL e de 3 a 4 achados com números para o README. As três consultas foram executadas com Spark local (ANSI ligado) sobre dados de exemplo.

**Passo 7.1 — [Databricks] Crie a branch `feat/gold`** a partir da `main` atualizada.

**Passo 7.2 — [Databricks] Confira os nomes reais dos produtos:**
```sql
SELECT produto, unidade_medida, COUNT(*) FROM workspace.anp.silver_precos GROUP BY ALL ORDER BY 3 DESC;
```

**Passo 7.3 — [Databricks] Crie `notebooks/03_gold_agregacoes` (SQL):**
```sql
-- Databricks notebook source
-- 1) Preço semanal por UF e produto, com variação semana a semana (função de janela)
CREATE OR REPLACE TABLE workspace.anp.gold_preco_semanal_uf AS
WITH semanal AS (
  SELECT uf, produto,
         CAST(date_trunc('WEEK', data_coleta) AS DATE) AS semana,
         ROUND(AVG(valor_venda), 3)                    AS preco_medio,
         MIN(valor_venda)                              AS preco_min,
         MAX(valor_venda)                              AS preco_max,
         COUNT(*)                                      AS qtd_coletas
  FROM workspace.anp.silver_precos
  GROUP BY ALL
)
SELECT *,
       ROUND(preco_medio / LAG(preco_medio)
             OVER (PARTITION BY uf, produto ORDER BY semana) - 1, 4) AS variacao_semanal
FROM semanal;

-- COMMAND ----------
-- 2) Bandeira vs. bandeira branca, por UF/produto/mês (auto-join)
CREATE OR REPLACE TABLE workspace.anp.gold_bandeira_vs_branca AS
WITH base AS (
  SELECT uf, produto,
         CAST(date_trunc('MONTH', data_coleta) AS DATE) AS mes,
         CASE WHEN bandeira = 'BRANCA' THEN 'BRANCA' ELSE 'COM_BANDEIRA' END AS tipo_posto,
         AVG(valor_venda) AS preco_medio
  FROM workspace.anp.silver_precos
  GROUP BY ALL
)
SELECT b.uf, b.produto, b.mes,
       ROUND(c.preco_medio, 3) AS preco_com_bandeira,
       ROUND(b.preco_medio, 3) AS preco_branca,
       ROUND(c.preco_medio / b.preco_medio - 1, 4) AS diferenca_pct
FROM base b
JOIN base c
  ON  c.uf = b.uf AND c.produto = b.produto AND c.mes = b.mes
  AND c.tipo_posto = 'COM_BANDEIRA'
WHERE b.tipo_posto = 'BRANCA';

-- COMMAND ----------
-- 3) Municípios mais caros e mais baratos no último mês disponível (subconsulta)
CREATE OR REPLACE TABLE workspace.anp.gold_ranking_municipios AS
WITH ultimo_mes AS (
  SELECT MAX(date_trunc('MONTH', data_coleta)) AS mes FROM workspace.anp.silver_precos
),
media AS (
  SELECT s.uf, s.municipio, s.produto, ROUND(AVG(s.valor_venda), 3) AS preco_medio
  FROM workspace.anp.silver_precos s
  WHERE date_trunc('MONTH', s.data_coleta) = (SELECT mes FROM ultimo_mes)
  GROUP BY s.uf, s.municipio, s.produto
)
SELECT *,
       DENSE_RANK() OVER (PARTITION BY produto ORDER BY preco_medio DESC) AS rank_mais_caro,
       DENSE_RANK() OVER (PARTITION BY produto ORDER BY preco_medio ASC)  AS rank_mais_barato
FROM media;
```
- **Bloco 1:** `LAG` pega o preço da semana anterior da mesma UF/produto; a variação é `atual / anterior − 1`. A primeira semana fica nula, porque não tem anterior.
- **Bloco 2:** a CTE `base` é unida com ela mesma (auto-join), para colocar o preço com bandeira e o da bandeira branca na mesma linha.
- **Bloco 3:** a subconsulta acha o último mês; `DENSE_RANK` numera do mais caro ao mais barato, sem pular posições em empates.

**Passo 7.4 — [Databricks] Valide:**
```sql
-- semanal: a 1ª semana de cada UF/produto tem variacao_semanal NULL
SELECT COUNT(*), COUNT(variacao_semanal) FROM workspace.anp.gold_preco_semanal_uf;

-- bandeira x branca: diferenças acima de 30% indicam erro
SELECT * FROM workspace.anp.gold_bandeira_vs_branca ORDER BY ABS(diferenca_pct) DESC LIMIT 10;

-- ranking: cada produto tem um rank_mais_caro = 1
SELECT produto, COUNT_IF(rank_mais_caro = 1) FROM workspace.anp.gold_ranking_municipios GROUP BY ALL;
```

**Passo 7.5 — [Databricks] Extraia os achados** (ajuste `'GASOLINA'` ao nome real do passo 7.2) e anote os números no `docs/decisoes.md`:
```sql
-- UFs com maior alta da gasolina no período (primeira x última semana)
WITH ext AS (
  SELECT uf, MIN_BY(preco_medio, semana) AS inicio, MAX_BY(preco_medio, semana) AS fim
  FROM workspace.anp.gold_preco_semanal_uf WHERE produto = 'GASOLINA' GROUP BY uf)
SELECT uf, inicio, fim, ROUND(fim / inicio - 1, 4) AS variacao FROM ext ORDER BY variacao DESC LIMIT 5;

-- Quanto o posto com bandeira cobra a mais, em média
SELECT produto, ROUND(AVG(diferenca_pct), 4) AS diferenca_media
FROM workspace.anp.gold_bandeira_vs_branca GROUP BY produto ORDER BY 2 DESC;

-- Rio e Niterói no ranking do último mês
SELECT * FROM workspace.anp.gold_ranking_municipios
WHERE municipio IN ('Rio De Janeiro', 'Niteroi', 'Niterói') ORDER BY produto;
```

**Passo 7.6 — Commit, PR e merge.**

**Pronto quando:** as três tabelas existem, as validações não mostram nada estranho e há pelo menos 3 achados com números.

> Ressalva para o README: a ANP coleta postos diferentes a cada semana; a média semanal é de uma amostra.

---

### Fase 8 — Job e carga incremental (≈4 h) · branch `feat/job`

**Resultado:** pipeline rodando sozinho toda semana, com e-mail quando falha, e a ingestão incremental comprovada com um arquivo mensal real.

**Passo 8.1 — [Databricks] Crie a branch `feat/job`.**

**Passo 8.2 — [Databricks] Atualize o `01_bronze_ingestao`** para extrair ZIPs novos antes de ingerir (assim o próprio Job processa um ZIP que você suba):
```python
# Databricks notebook source
from databricks.sdk.runtime import dbutils, spark

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

import os
import sys

sys.path.append(os.path.abspath("../src"))
from anp_lakehouse.arquivos import extrair_zips
from anp_lakehouse.config import PASTA_CSV, PASTA_ZIP, tabela
from anp_lakehouse.ingestao import arquivos_novos, ler_csvs

print("extraídos:", extrair_zips(PASTA_ZIP, PASTA_CSV))

todos = [f.path for f in dbutils.fs.ls(PASTA_CSV) if f.path.endswith(".csv")]

if spark.catalog.tableExists(tabela("bronze_precos")):
    processados = {r[0] for r in spark.table(tabela("bronze_precos"))
                                    .select("_arquivo_origem").distinct().collect()}
else:
    processados = set()

novos = arquivos_novos(todos, processados)
print(f"{len(novos)} arquivo(s) novo(s):", novos)

if novos:
    (ler_csvs(spark, novos)
       .write.mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(tabela("bronze_precos")))
```

**Passo 8.3 — [Databricks] Crie `notebooks/06_verificar_alerta`** — a última tarefa do Job, que falha de propósito se a última execução estiver em alerta (e a falha dispara o e-mail):
```python
# Databricks notebook source
from databricks.sdk.runtime import spark

ultimo = spark.sql("""
    SELECT status, detalhe FROM workspace.anp.log_execucoes
    WHERE etapa = 'silver' ORDER BY inicio DESC LIMIT 1""").first()

if ultimo is None or ultimo.status == "ALERTA":
    raise Exception(f"Pipeline em ALERTA: {ultimo.detalhe if ultimo else 'sem registro'}")
print("Pipeline OK")
```

**Passo 8.4 — Commit, PR com CI verde e merge.** O Job vai rodar a partir da `main`.

**Passo 8.5 — [Databricks] Crie uma Git folder só para produção.** Repita **Create → Git folder** com o mesmo repositório, numa pasta chamada `prod`, e deixe-a sempre na `main`. O Job roda dela; sua pasta de trabalho pode ficar em qualquer branch. **Depois de cada merge, Pull na `prod`.**

**Passo 8.6 — [Databricks] Crie o Job.** **Jobs & Pipelines → Create → Job** → nome `pipeline_anp_semanal`. Tarefas do tipo **Notebook**, origem **Workspace** (caminhos dentro de `prod/`), compute **Serverless**:

| Tarefa | Notebook | Depende de |
|---|---|---|
| `bronze` | `notebooks/01_bronze_ingestao` | — |
| `silver` | `notebooks/02_silver_transformacao` | `bronze` |
| `gold` | `notebooks/03_gold_agregacoes` | `silver` |
| `monitoramento` | `notebooks/06_verificar_alerta` | `gold` (passa a depender de `governanca` na Fase 9) |

**Passo 8.7 — [Databricks] Agendamento e alerta.** **Schedules & Triggers → Scheduled** → semanal, segunda 06:00, fuso **America/Sao_Paulo**. **Job notifications → Failure** → seu e-mail.

**Passo 8.8 — [Databricks] Rode manualmente.** **Run now** → abra a execução → confira o grafo com as tarefas verdes. Salve um print em `docs/prints/job_grafo.png`.

**Passo 8.9 — Teste a carga incremental com dados reais.** O último semestre fechado termina em junho/2026, mas a ANP já publica os mensais até outubro. No PC:
```powershell
$b = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2026"
Invoke-WebRequest "$b/10-gasolina-etanol.csv" -OutFile data/raw/mensal-2026-10-gasolina-etanol.csv
Invoke-WebRequest "$b/10-diesel-gnv.csv"      -OutFile data/raw/mensal-2026-10-diesel-gnv.csv
Get-Content data/raw/mensal-2026-10-gasolina-etanol.csv -TotalCount 2 -Encoding UTF8
```
- Compare o cabeçalho com o dos semestrais. Se mudou, ajuste o `MAPA_COLUNAS` antes, num PR próprio.
- Suba os dois CSVs direto em `raw/csv/` pelo Catalog e rode o Job de novo.
- Esperado: a tarefa `bronze` imprime `2 arquivo(s) novo(s)` e o `data_mais_recente` do log pula para outubro/2026.

**Passo 8.10 — [PC] Exporte a definição do Job.** Na página do Job: **⋮ → View as code** (YAML; o nome do menu pode variar). Salve em `jobs/job_pipeline_anp.yml`, faça commit com o print e abra o PR.

**Pronto quando:** duas execuções verdes, a segunda trazendo só os arquivos de outubro; YAML e print no repositório.

> Ponte com o D2: tarefa do Job = task do Airflow, "Depende de" = `>>`, Job = DAG, notificação de falha = `on_failure_callback`.

---

### Fase 9 — Governança no Unity Catalog (≈3 h) · branch `feat/governanca`

**Resultado:** tabelas e colunas descritas, tags por camada, leitura liberada só na ouro e o print da linhagem. Como as tabelas ouro usam `CREATE OR REPLACE TABLE` toda semana (o que pode apagar comentários e tags), a governança vira uma tarefa do Job. Todos os comandos são idempotentes.

**Passo 9.1 — [Databricks] Crie a branch `feat/governanca`.**

**Passo 9.2 — [Databricks] Crie `notebooks/04_governanca` (SQL):**
```sql
-- Databricks notebook source
-- Metadados: descrição de tabelas e colunas
COMMENT ON TABLE workspace.anp.silver_precos
  IS 'Preços de revenda limpos e tipados. Fonte: ANP, série histórica por revenda.';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN regiao COMMENT 'Sigla da região';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN uf COMMENT 'Sigla do estado';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN municipio COMMENT 'Município (capitalizado)';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN revenda COMMENT 'Nome do posto';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN cnpj_revenda COMMENT 'CNPJ do posto';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN bairro COMMENT 'Bairro';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN produto COMMENT 'Combustível (ex.: GASOLINA, ETANOL)';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN data_coleta COMMENT 'Data da coleta de preço pela ANP';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN valor_venda COMMENT 'Preço ao consumidor (R$/L; R$/m³ para GNV)';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN unidade_medida COMMENT 'Unidade informada pela ANP';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN bandeira COMMENT 'Distribuidora do posto ou BRANCA';
ALTER TABLE workspace.anp.silver_precos ALTER COLUMN _arquivo_origem COMMENT 'CSV de origem (linhagem)';
COMMENT ON TABLE workspace.anp.gold_preco_semanal_uf
  IS 'Preço médio semanal por UF e produto, com variação semana a semana.';
COMMENT ON TABLE workspace.anp.gold_bandeira_vs_branca
  IS 'Diferença de preço entre postos com bandeira e bandeira branca, por UF/produto/mês.';
COMMENT ON TABLE workspace.anp.gold_ranking_municipios
  IS 'Municípios mais caros e mais baratos por produto no último mês.';

-- COMMAND ----------
-- Classificação por camada (tags)
ALTER TABLE workspace.anp.bronze_precos           SET TAGS ('camada' = 'bronze');
ALTER TABLE workspace.anp.silver_precos           SET TAGS ('camada' = 'prata');
ALTER TABLE workspace.anp.quarentena_precos       SET TAGS ('camada' = 'prata', 'tipo' = 'quarentena');
ALTER TABLE workspace.anp.log_execucoes           SET TAGS ('tipo' = 'observabilidade');
ALTER TABLE workspace.anp.gold_preco_semanal_uf   SET TAGS ('camada' = 'ouro', 'consumo' = 'analytics');
ALTER TABLE workspace.anp.gold_bandeira_vs_branca SET TAGS ('camada' = 'ouro', 'consumo' = 'analytics');
ALTER TABLE workspace.anp.gold_ranking_municipios SET TAGS ('camada' = 'ouro', 'consumo' = 'analytics');

-- COMMAND ----------
-- Controle de acesso: consumidores leem só a camada ouro
GRANT USE SCHEMA ON SCHEMA workspace.anp TO `account users`;
GRANT SELECT ON TABLE workspace.anp.gold_preco_semanal_uf   TO `account users`;
GRANT SELECT ON TABLE workspace.anp.gold_bandeira_vs_branca TO `account users`;
GRANT SELECT ON TABLE workspace.anp.gold_ranking_municipios TO `account users`;
```

**Passo 9.3 — [Databricks] Rode e verifique:**
```sql
SHOW GRANTS ON TABLE workspace.anp.gold_preco_semanal_uf;

SELECT table_name, tag_name, tag_value
FROM workspace.information_schema.table_tags WHERE schema_name = 'anp';

SELECT column_name, comment
FROM workspace.information_schema.columns
WHERE table_schema = 'anp' AND table_name = 'silver_precos';
```
Se algum comando de tag ou GRANT for recusado no Free Edition, comente a linha e registre a limitação (com a mensagem de erro) no `decisoes.md`.

**Passo 9.4 — [Databricks] Salve a linhagem.** **Catalog → gold_preco_semanal_uf → Lineage → See lineage graph**. Deve aparecer bronze → prata → ouro. Salve em `docs/prints/linhagem.png`.

**Passo 9.5 — Commit, PR e merge. Depois, Pull na `prod`.**

**Passo 9.6 — [Databricks] Inclua a governança no Job.** Crie a tarefa `governanca` (`notebooks/04_governanca`) dependente de `gold`, e mude `monitoramento` para depender de `governanca`. A cadeia final fica:

`bronze → silver → gold → governanca → monitoramento`

Rode o Job, repita as consultas do passo 9.3 (comentários e tags da ouro têm que continuar lá) e reexporte o YAML para `jobs/`.

**Pronto quando:** permissões, tags e comentários aparecem depois de uma execução completa do Job, e o print da linhagem está no repositório.

---

### Fase 10 — Documentação, README e LinkedIn (≈5 h) · branch `docs/readme`

**Resultado:** um repositório que se explica em 2 minutos para um recrutador e em 10 para um engenheiro de dados, mais o post no LinkedIn.

**Passo 10.1 — [PC] Diagrama.** No draw.io ou Excalidraw, desenhe a arquitetura da seção 3 e exporte para `docs/arquitetura.png`.

**Passo 10.2 — [PC] `docs/dicionario_de_dados.md`.** Uma tabela por camada (coluna, tipo, descrição). A base sai da consulta ao `information_schema.columns` da Fase 9.

**Passo 10.3 — [PC] Feche o `docs/decisoes.md`.** Registre principalmente os problemas resolvidos, que são o melhor material de entrevista:
- upload manual porque o Free Edition restringe a internet de saída;
- formato ZIP da ANP e o extrator idempotente com arquivo temporário;
- `try_to_date`/`try_cast` por causa do modo ANSI;
- schema explícito no `log_execucoes`;
- pasta `prod` separada para o Job;
- governança reaplicada a cada execução;
- o que não funcionou no Free Edition, se algo falhou.

**Passo 10.4 — [PC] Escreva o `README.md`** seguindo o roteiro da seção 10. Coloque o badge do CI logo abaixo do título:
```markdown
![CI](https://github.com/Bmendes7/lakehouse-combustiveis-anp/actions/workflows/ci.yml/badge.svg)
```

**Passo 10.5 — [GitHub] Vitrine.** Em **About**, descrição de uma linha e topics `databricks`, `pyspark`, `delta-lake`, `lakehouse`, `unity-catalog`, `data-engineering`. Fixe o repositório no perfil (**Customize your pins**), ao lado do `pipeline-dados-bcb`.

**Passo 10.6 — PR final e merge.** Leia o README no GitHub como um recrutador: links, imagens e badge precisam funcionar.

**Passo 10.7 — [LinkedIn] Post**, na mesma linha do post do D1:
- **Gancho:** "No dia a dia monitoro preços com Python e SQL; quis fazer o mesmo em escala, com Databricks e arquitetura Lakehouse."
- **O que construí:** bronze → prata → ouro com dados públicos da ANP, quarentena, testes no CI, Job agendado e governança no Unity Catalog.
- **Dois achados com número** da Fase 7.
- **Um aprendizado técnico**, como o do modo ANSI: o código que funcionava local quebraria em produção, e o teste pegou antes.
- **Link do repositório e imagem** (diagrama ou print da linhagem).
- Nenhum dado nem print de sistema da Vigodent.

**Passo 10.8 — Atualize o plano de estudos.** Na `plano-estudos-backend.xlsx`, marque o D3 como concluído e registre as horas reais.

**Pronto quando:** README com diagrama, badge verde, achados e instruções de reprodução; repositório fixado; post publicado.

---

## 8. Problemas comuns

| Fase | Sintoma | Causa provável | Solução |
|---|---|---|---|
| 0 | Todo teste dá `ModuleNotFoundError: anp_lakehouse` | `pytest.ini` vazio ou ausente | Conteúdo do passo 0.4 |
| 0 | `python` não encontrado | Atalho do Windows | Use `py -m venv .venv` |
| 0 | VS Code sublinha `spark`, `dbutils`, `display` | Nomes que só existem no Databricks | `databricks-sdk` instalado + import do passo 1.7 |
| 1 | Push recusado com "protected branch" | Você está na `main` | Crie uma branch e abra o PR |
| 1 | Git folder pede credencial de novo | Token expirou (90 dias) | Gere outro e atualize em Linked accounts |
| 1 | Caminho `/Volumes/workspace.anp/...` não existe | Ponto em vez de barra no caminho | `/Volumes/{CATALOGO}/{SCHEMA}/raw` |
| 1–3 | `ModuleNotFoundError: databricks.sdk` no Databricks | SDK ausente no ambiente | Apague a linha `from databricks.sdk.runtime import ...` (ela só serve ao VS Code) |
| 2 | Script baixa só um semestre | `;` em vez de `,` na lista | `"2024-02","2025-01",...` |
| 2 | `esperava 1 csv, encontrei 0` | `endswith("*.csv")` com asterisco | `endswith(".csv")` |
| 2 | Download pelo notebook falha | Internet de saída restrita no Free Edition | Baixe no PC e suba pelo Catalog |
| 2 | `Munic�pio` com caractere estranho | Arquivo em Latin-1 | `ENCODING_CSV = "ISO-8859-1"` |
| 2 | Todas as colunas grudadas numa só | Separador diferente de `;` | Confira a 1ª linha e ajuste o `sep` |
| 3 | `ModuleNotFoundError: anp_lakehouse` no notebook | Notebook fora de `notebooks/` | `print(os.getcwd())` e ajuste o `../src` |
| 3 | Segunda execução duplica a bronze | Caminhos com e sem `dbfs:` | `normalizar_caminho` (passo 3.2) |
| 4 | `DateTimeException` / `CAST_INVALID_INPUT` | Modo ANSI | `try_to_date` / `try_cast` |
| 5 | A prata cresce a cada execução | Chave com data nula nunca casa no MERGE | Quarentena + recriar a prata (passo 5.8) |
| 5 | `ADD CONSTRAINT` falha | Linhas que violam a regra | Recriar a prata depois da quarentena |
| 5 | CI vermelho no ruff (`I001`, `E501`) | Regras do ruff 0.16 | `ruff.toml` + `ruff check --fix` |
| 5 | CI vermelho com `JAVA_GATEWAY_EXITED` | Falta Java no runner | Passo `setup-java` no `ci.yml` |
| 6 | `CANNOT_DETERMINE_TYPE` ao gravar o log | `detalhe` nulo sem schema | `SCHEMA_LOG` explícito |
| 6 | Horários do log 3 h adiantados | Serverless em UTC | Função `agora()` com America/Sao_Paulo |
| 8 | O Job roda código antigo | Pasta `prod` sem Pull | Pull na `prod` após cada merge |
| 8 | Compute desligado pelo resto do dia | Cota do Free Edition | Espere o reset; se repetir, use 2 semestres |
| 9 | Comentários e tags da ouro sumiram | `CREATE OR REPLACE TABLE` | Tarefa `governanca` no Job |

---

## 9. Checklist e cronograma

| Fase | Entrega | Horas |
|---|---|---|
| 0. Preparação | Repositório, arquivos-base, `.venv`, primeiro push | 2 |
| 1. Databricks | Git folder, schema e Volume | 2 |
| 2. Dados | 4 semestres extraídos, formato anotado, extrator testado | 3 |
| 3. Bronze | Ingestão incremental idempotente | 4 |
| 4. Prata | Tipagem com try_*, deduplicação, MERGE | 5 |
| 5. Qualidade e CI | Quarentena, constraints, CI, `main` protegida | 4 |
| 6. Observabilidade | `log_execucoes` OK/ALERTA, 11 testes | 3 |
| 7. Ouro | 3 tabelas + achados | 5 |
| 8. Job | Pipeline agendado + carga incremental real | 4 |
| 9. Governança | Comentários, tags, GRANTs, linhagem | 3 |
| 10. Documentação | README, diagrama, post | 5 |
| **Total** | | **40** |

- [ ] Fase 0 — repositório no GitHub com estrutura, `pytest.ini`, `ruff.toml` e `docs/PLANO.md`
- [ ] Fase 1 — Git folder ligada; `workspace.anp` com Volume `raw/zip` e `raw/csv`
- [ ] Fase 2 — 4 CSVs extraídos, `test_arquivos` passando, formato anotado, `MAPA_COLUNAS` conferido
- [ ] Fase 3 — bronze incremental; rodar 2× não duplica
- [ ] Fase 4 — prata tipada com MERGE e zero duplicatas na chave
- [ ] Fase 5 — quarentena com motivo, constraints, CI verde, `main` protegida
- [ ] Fase 6 — `log_execucoes` com execuções OK e alerta testado; 11 testes no CI
- [ ] Fase 7 — 3 tabelas ouro e pelo menos 3 achados com números
- [ ] Fase 8 — pasta `prod`, Job agendado e carga de outubro/2026 processada
- [ ] Fase 9 — comentários, tags, GRANTs, linhagem e tarefa no Job
- [ ] Fase 10 — README completo, repositório fixado e post publicado

---

## 10. Material para README, entrevista e próximos passos

**Roteiro do README**
1. Título, uma frase e badge do CI.
2. Arquitetura (diagrama).
3. Stack: Databricks, PySpark, Delta Lake, Unity Catalog, GitHub Actions.
4. O que os dados mostram: 3 ou 4 achados com números + ressalva da amostra.
5. Decisões técnicas: por que Spark, por que MERGE, por que quarentena, por que `try_*`.
6. Qualidade, observabilidade e governança: regras, testes, `log_execucoes`, permissões, linhagem.
7. Como reproduzir, a partir de uma conta do Free Edition.
8. Equivalência com Azure Data Factory e ponte Job × Airflow.
9. Próximos passos.

**Equivalência com Azure Data Factory** (estude pelos módulos gratuitos do Microsoft Learn)

| Neste projeto | Com Azure Data Factory |
|---|---|
| Download dos ZIPs + upload no Volume | Copy Activity (HTTP → ADLS Gen2) |
| Volume `raw` | Container no ADLS Gen2 |
| Job do Databricks agendado | Pipeline do ADF com Schedule Trigger chamando Notebook Activities |
| Notificação de falha do Job | Alerta no Azure Monitor / atividade de falha no pipeline |

**Pontos para falar em entrevista**
- Por que o pipeline é idempotente (extração e ingestão incrementais + MERGE) e por que isso importa em produção.
- Diferença entre bronze, prata e ouro e quem consome cada camada.
- Por que registros inválidos vão para quarentena em vez de serem descartados (e o caso da chave nula no MERGE).
- Modo ANSI: o código que funcionava local quebraria em produção, e o teste pegou antes.
- Diferença entre qualidade (barrar dado ruim) e observabilidade (saber se o pipeline está saudável).
- Como a lógica em `src/` permite testar sem o Databricks.
- Job do Databricks × DAG do Airflow (ponte com o D2).
- Ligação com o trabalho: monitoramento de preços com Python e SQL.

**Evoluções opcionais (depois de concluído)**
- **Auto Loader** (`cloudFiles`) com `trigger(availableNow=True)` no lugar da detecção manual de arquivos novos.
- **Databricks Asset Bundles** (`databricks.yml`): Job 100% como código, substituindo a pasta `prod`.
- Dashboard SQL do Databricks em cima do `05_monitoramento` e das tabelas ouro.

---

## 11. Fontes

- [ANP — Série histórica de preços de combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) (consultada em 24/11/2026)
- [Databricks — Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (consultada em 24/11/2026)
