# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze – Ingestão de Dados Brutos
# MAGIC %md
# MAGIC # Bronze – Ingestão de Dados Brutos
# MAGIC
# MAGIC **Projeto:** uemg_compras – Portal de Compras do Estado de Minas Gerais
# MAGIC
# MAGIC **Objetivo:** ler todos os arquivos CSV da pasta `/Volumes/uemg_compras/bronze/dados_brutos_governomg/` e gravar como tabelas Delta na camada Bronze do Unity Catalog.
# MAGIC
# MAGIC **Regras de ingestão:**
# MAGIC - Todas as colunas lidas como `STRING` (sem `inferSchema`) — a tipagem fica para a Silver.
# MAGIC - Separador `;`, cabeçalho na 1ª linha, encoding UTF-8 com BOM, CRLF, `quote='"'`, `escape='"'`.
# MAGIC - Colunas técnicas: `_arquivo_origem`, `_dt_modificacao_arquivo`, `_dt_ingestao`.
# MAGIC - Escrita `overwrite` com `overwriteSchema=True` (idempotente).

# COMMAND ----------

# DBTITLE 1,1. Criar catálogo e schemas
# ── 1. Criar catálogo e schemas (IF NOT EXISTS) ───────────────────
# O volume dados_brutos_governomg já existe em uemg_compras.bronze.
spark.sql("CREATE CATALOG IF NOT EXISTS uemg_compras")
spark.sql("CREATE SCHEMA IF NOT EXISTS uemg_compras.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS uemg_compras.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS uemg_compras.gold")

print("Catálogo e schemas verificados/criados com sucesso.")
spark.sql("SHOW SCHEMAS IN uemg_compras").display()

# COMMAND ----------

# DBTITLE 1,2. Função reutilizável ingest_csv
# ── 2. Função reutilizável: ingest_csv(caminho_arquivo) ───────────────
# Lê um CSV da pasta de dados brutos e grava como tabela Delta na Bronze.
# - Todas as colunas como STRING (sem inferSchema)
# - Remove BOM e espaços dos nomes; nomes em minúsculas
# - Adiciona colunas técnicas (_arquivo_origem, _dt_modificacao_arquivo, _dt_ingestao)
# - Escrita overwrite com overwriteSchema=True (idempotente)
# - COMMENT específico para dm_contratado (LGPD)

from pyspark.sql.functions import col, current_timestamp, trim, regexp_replace
from pyspark.sql.types import StringType

# Pasta base dos arquivos CSV
PASTA_DADOS = "/Volumes/uemg_compras/bronze/dados_brutos_governomg/"
CATALOGO = "uemg_compras"
SCHEMA_BRONZE = "bronze"

def _normalizar_nomes_colunas(df):
    """Remove BOM (\ufeff) e espaços dos nomes das colunas; nomes em minúsculas."""
    colunas_originais = df.columns  # uma única chamada de Analyze RPC
    novos_nomes = []
    for c in colunas_originais:
        nome = c.replace("\ufeff", "").strip().replace(" ", "_").lower()
        novos_nomes.append(nome)
    # Usa select com alias (evita withColumnRenamed em loop)
    return df.select(*[col(c).alias(novos_nomes[i]) for i, c in enumerate(colunas_originais)])

def ingest_csv(caminho_arquivo):
    """
    Ingest um arquivo CSV para uma tabela Delta na camada Bronze.

    Parâmetros:
        caminho_arquivo (str): caminho completo do arquivo CSV.

    Retorno:
        dict com tabela, qtd_linhas, qtd_colunas e status.
    """
    # Extrai o nome do arquivo sem extensão para usar como nome da tabela
    nome_arquivo = caminho_arquivo.split("/")[-1]
    nome_tabela = nome_arquivo.replace(".csv", "")
    tabela_full = f"{CATALOGO}.{SCHEMA_BRONZE}.{nome_tabela}"

    # Ler o CSV — todas as colunas como STRING (sem inferSchema)
    # O Spark lê todas as colunas como StringType por padrão quando header=true
    # e nenhum schema é fornecido.
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("lineSep", "\r\n")
        .load(caminho_arquivo)
    )

    # Normalizar nomes das colunas (remove BOM, espaços; minúsculas)
    df = _normalizar_nomes_colunas(df)

    # Cast explícito de todas as colunas para STRING + colunas técnicas (withColumns evita loop)
    colunas_atuais = df.columns  # uma única chamada de Analyze RPC
    colunas_expr = {c: col(c).cast(StringType()) for c in colunas_atuais}
    colunas_expr.update({
        "_arquivo_origem": col("_metadata.file_path").cast(StringType()),
        "_dt_modificacao_arquivo": col("_metadata.file_modification_time").cast("timestamp"),
        "_dt_ingestao": current_timestamp(),
    })
    df = df.withColumns(colunas_expr)

    # Escrever em modo overwrite com overwriteSchema=True (idempotente)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_full)
    )

    # Definir COMMENT na tabela
    comentario_base = (
        f"Bronze - dados brutos de {nome_arquivo}, "
        f"Portal de Compras do Estado de Minas Gerais"
    )
    if nome_tabela == "dm_contratado":
        comentario_base += (
            " - Contém dados pessoais anonimizados (CPF mascarado) - uso sujeito à LGPD"
        )

    spark.sql(f"ALTER TABLE {tabela_full} SET TBLPROPERTIES ('comment' = '{comentario_base}')")

    # Coletar metadados para retorno
    qtd_linhas = df.count()
    qtd_colunas = len(df.columns)

    print(f"✓ {tabela_full}: {qtd_linhas} linhas, {qtd_colunas} colunas")

    return {
        "tabela": tabela_full,
        "qtd_linhas": qtd_linhas,
        "qtd_colunas": qtd_colunas,
        "status": "OK",
    }

# COMMAND ----------

# DBTITLE 1,3. Listar arquivos CSV e ingerir
# ── 3. Listar arquivos CSV e executar ingestão (try/except por arquivo) ─
# Lista a pasta com dbutils.fs.ls e filtra por .csv
# Novos arquivos ft_*/fl_*/dm_* colocados na pasta são ingeridos automaticamente.

arquivos_csv = [
    f.path
    for f in dbutils.fs.ls(PASTA_DADOS)
    if f.path.lower().endswith(".csv")
]

print(f"Arquivos CSV encontrados ({len(arquivos_csv)}):")
for a in sorted(arquivos_csv):
    print(f"  • {a}")
print()

# Lista de resultados para validação posterior
resultados_ingestao = []

for arquivo in sorted(arquivos_csv):
    try:
        resultado = ingest_csv(arquivo)
        resultados_ingestao.append(resultado)
    except Exception as e:
        nome_arquivo = arquivo.split("/")[-1]
        tabela_full = f"{CATALOGO}.{SCHEMA_BRONZE}.{nome_arquivo.replace('.csv', '')}"
        print(f"✗ ERRO ao ingerir {arquivo}: {e}")
        resultados_ingestao.append({
            "tabela": tabela_full,
            "qtd_linhas": 0,
            "qtd_colunas": 0,
            "status": f"ERRO: {e}",
        })

print("\nIngestão concluída.")

# COMMAND ----------

# DBTITLE 1,4. Validação final
# ── 4. Validação final ──────────────────────────────────────────────
# DataFrame resumo com schema explícito e colunas de negócio, ordenado por tabela.
# NÃO reexecuta a ingestão — apenas lê as tabelas já criadas na Bronze.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# Schema explícito do resumo
schema_resumo = StructType([
    StructField("tabela", StringType(), True),
    StructField("qtd_linhas", LongType(), True),
    StructField("qtd_colunas", IntegerType(), True),
    StructField("qtd_colunas_negocio", IntegerType(), True),
    StructField("linhas_totalmente_nulas", LongType(), True),
    StructField("status", StringType(), True),
])

linhas_resumo = []

for r in resultados_ingestao:
    tabela = r["tabela"]
    status = r["status"]

    if status != "OK":
        linhas_resumo.append({
            "tabela": tabela,
            "qtd_linhas": 0,
            "qtd_colunas": 0,
            "qtd_colunas_negocio": 0,
            "linhas_totalmente_nulas": 0,
            "status": status,
        })
        continue

    # Lê a tabela Delta e identifica as colunas de negócio (não começam com "_")
    df_tabela = spark.table(tabela)
    todas_colunas = df_tabela.columns
    colunas_negocio = [c for c in todas_colunas if not c.startswith("_")]

    # Conta linhas onde TODAS as colunas de negócio são NULL
    if colunas_negocio:
        condicao_nula = None
        for c_name in colunas_negocio:
            if condicao_nula is None:
                condicao_nula = F.col(c_name).isNull()
            else:
                condicao_nula = condicao_nula & F.col(c_name).isNull()
        qtd_todas_nulas = df_tabela.filter(condicao_nula).count()
    else:
        qtd_todas_nulas = 0

    linhas_resumo.append({
        "tabela": tabela,
        "qtd_linhas": r["qtd_linhas"],
        "qtd_colunas": r["qtd_colunas"],
        "qtd_colunas_negocio": len(colunas_negocio),
        "linhas_totalmente_nulas": qtd_todas_nulas,
        "status": "OK",
    })

# Criar DataFrame com schema explícito e ordenar por tabela
df_resumo = spark.createDataFrame(linhas_resumo, schema_resumo).orderBy("tabela")

print("═" * 70)
print("RESUMO DA INGESTÃO")
print("═" * 70)
df_resumo.display()

# Resumo final
qtd_ok = sum(1 for r in resultados_ingestao if r["status"] == "OK")
qtd_erro = sum(1 for r in resultados_ingestao if r["status"] != "OK")
qtd_alerta = sum(1 for linha in linhas_resumo if linha["linhas_totalmente_nulas"] > 0)

print(f"\n{'═' * 70}")
print(f"Resumo final: {qtd_ok} tabela(s) OK, {qtd_erro} erro(s), {qtd_alerta} alerta(s) de parsing")
print(f"{'═' * 70}")