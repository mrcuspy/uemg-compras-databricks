# Databricks notebook source
# DBTITLE 1,Silver — Fatos
# MAGIC %md
# MAGIC # Silver — Fatos
# MAGIC
# MAGIC **Projeto:** uemg_compras – Portal de Compras do Estado de Minas Gerais
# MAGIC
# MAGIC **Objetivo:** ler as tabelas de fato da camada Bronze (todas STRING), aplicar tipagem, limpeza e
# MAGIC regras de negócio, e gravar tabelas Delta tipadas em `uemg_compras.silver`.
# MAGIC
# MAGIC **Execução:** IDEMPOTENTE (`overwrite` + `overwriteSchema`). Mantém o ESTADO INTEIRO
# MAGIC (a UEMG é identificada por flag, não por filtro).
# MAGIC
# MAGIC **Fatos criados:**
# MAGIC * `silver.fato_compras_item` (origem: `bronze.ft_compras`; 1 linha = 1 item homologado)
# MAGIC * `silver.fato_compras_contrato` (origem: `bronze.ft_compras_contrato`)
# MAGIC * `silver.fato_compras_empenho` (origem: `bronze.fl_compras_empenho`)

# COMMAND ----------

# DBTITLE 1,0. Imports e funções reutilizáveis
# ── 0. Imports, constantes e funções reutilizáveis ───────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
)
from pyspark.sql.window import Window

CATALOGO = "uemg_compras"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
RESPONSAVEL = "<responsavel>"
NOTEBOOK_NOME = "02_silver_fatos"

COLUNAS_TECNICAS_BRONZE = ["_arquivo_origem", "_dt_modificacao_arquivo", "_dt_ingestao"]

# ── Funções auxiliares ─────────────────────────────────────────────

def _esc(texto):
    """Escapa aspas simples para SQL string literals."""
    return texto.replace("'", "''")


def trim_null(col_expr):
    """trim() e converte string vazia para NULL."""
    c = F.trim(col_expr)
    return F.when(c == "", F.lit(None).cast("string")).otherwise(c)


def try_cast_bigint(col_expr):
    """Cast para BIGINT que nunca falha; valores como '1843.0' são convertidos via DOUBLE."""
    s = col_expr.cast("string")
    # Padrão inteiro puro
    is_int = s.rlike(r"^-?\d+$")
    # Padrão decimal com parte fracionária zero (ex.: 1843.0, 1843.00)
    is_decimal_int = s.rlike(r"^-?\d+\.[0]+$")
    return (
        F.when(is_int, s.cast("bigint"))
        .when(is_decimal_int, s.cast("double").cast("bigint"))
        .otherwise(F.lit(None).cast("bigint"))
    )


def try_cast_decimal(col_expr, precision, scale):
    """Cast para DECIMAL(p,s) que nunca falha; retorna NULL para inválidos."""
    s = col_expr.cast("string")
    # Aceita números com ponto decimal (positivos ou negativos)
    is_numeric = s.rlike(r"^-?\d+(\.\d+)?$")
    return F.when(is_numeric, s.cast(f"decimal({precision},{scale})")).otherwise(F.lit(None).cast(f"decimal({precision},{scale})"))


def try_cast_date(col_expr):
    """Cast para DATE que nunca falha; retorna NULL para inválidos."""
    s = trim_null(col_expr)
    return F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), s.cast("date")).otherwise(F.lit(None).cast("date"))


def _comentarios_bronze(tabela_bronze):
    """Busca comentários de coluna da Bronze no information_schema."""
    df = spark.sql(
        f"SELECT column_name, comment "
        f"FROM {CATALOGO}.information_schema.columns "
        f"WHERE table_schema = '{SCHEMA_BRONZE}' AND table_name = '{tabela_bronze}' "
        f"  AND comment IS NOT NULL"
    )
    return {row["column_name"]: row["comment"] for row in df.collect()}


def aplicar_governanca_silver(tabela_silver, comentario_tabela, comentarios_colunas, colunas_pii=None):
    """Aplica COMMENT ON TABLE, COMMENT em colunas, SET TAGS e TBLPROPERTIES."""
    tabela_full = f"{CATALOGO}.{SCHEMA_SILVER}.{tabela_silver}"
    colunas_pii = colunas_pii or []

    spark.sql(f"COMMENT ON TABLE {tabela_full} IS '{_esc(comentario_tabela)}'")

    for col_name, col_comment in comentarios_colunas.items():
        spark.sql(f"ALTER TABLE {tabela_full} ALTER COLUMN {col_name} COMMENT '{_esc(col_comment)}'")

    spark.sql(
        f"ALTER TABLE {tabela_full} SET TAGS ("
        f"'camada'='silver', 'fonte'='portal_compras_mg', "
        f"'dominio'='compras_publicas', 'tipo'='fato'"
        f")"
    )

    for col_pii in colunas_pii:
        spark.sql(
            f"ALTER TABLE {tabela_full} ALTER COLUMN {col_pii} "
            f"SET TAGS ('pii'='true', 'lgpd'='dado_pessoal_anonimizado')"
        )

    spark.sql(
        f"ALTER TABLE {tabela_full} SET TBLPROPERTIES ("
        f"'fonte_dados'='Portal de Compras MG - dados abertos', "
        f"'frequencia_atualizacao'='manual', "
        f"'responsavel'='{RESPONSAVEL}', 'projeto'='uemg_compras'"
        f")"
    )
    print(f"  Governança aplicada em {tabela_full}")


def escrever_tabela(df, tabela_full, partition_cols=None):
    """Escreve em overwrite + overwriteSchema, com particionamento opcional."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(tabela_full)


def calcular_falhas_cast(df_br, colunas_bronze, df_silver, colunas_silver):
    """Calcula falhas de cast: não-nulo na Bronze → NULL na Silver."""
    total_falhas = 0
    for br_col, silver_col in zip(colunas_bronze, colunas_silver):
        br_non_null = df_br.filter(F.col(br_col).isNotNull() & (F.trim(F.col(br_col)) != "")).select(br_col).distinct().count()
        silver_non_null = df_silver.filter(F.col(silver_col).isNotNull()).select(silver_col).distinct().count()
        total_falhas += max(br_non_null - silver_non_null, 0)
    return total_falhas


print("Funções utilitárias carregadas.")
resultados_fatos = []

# Permitir que casts DECIMAL fora de faixa retornem NULL em vez de falhar
spark.sql("SET spark.sql.ansi.enabled = false")

# COMMAND ----------

# DBTITLE 1,1. silver.fato_compras_item
# ── 1. silver.fato_compras_item (origem: bronze.ft_compras) ─────────
# 1 linha = 1 item homologado. NÃO remover duplicatas — apenas marcar.
# Particionado por ano_particao.

TABELA = "fato_compras_item"
ORIGEM = "ft_compras"
TABELA_FULL = f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
linhas_bronze = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

# Colunas de negócio da Bronze (sem técnicas e sem id_tipo_licitacao que será removida)
COLUNAS_NEGOCIO = [
    "id_tempo", "id_procedimento", "id_orgao_demanda", "id_orgao_contrato",
    "id_situacao_proc", "id_situacao_cont", "id_municipio", "id_contratado",
    "id_grupo_matserv", "id_classe_matserv", "id_material_servico", "id_item_matserv",
    "id_processo", "id_contrato", "id_unidade_medida", "id_linha_fornec",
    "id_item", "id_unidade_orc", "ano_particao", "dt_item_homologa",
    "qt_item_pedido", "vr_un_referencia", "vr_referencia",
    "vr_un_homologado", "vr_homologado", "vr_atualizado",
]

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    # REMOVER id_tipo_licitacao (100% nula)
    .drop("id_tipo_licitacao")
    # ── Tipagem: IDs → BIGINT ──
    .withColumn("id_tempo", try_cast_bigint(F.col("id_tempo")))
    .withColumn("id_procedimento", try_cast_bigint(F.col("id_procedimento")))
    .withColumn("id_orgao_demanda", try_cast_bigint(F.col("id_orgao_demanda")))
    .withColumn("id_orgao_contrato", try_cast_bigint(F.col("id_orgao_contrato")))
    .withColumn("id_situacao_proc", try_cast_bigint(F.col("id_situacao_proc")))
    .withColumn("id_situacao_cont", try_cast_bigint(F.col("id_situacao_cont")))
    .withColumn("id_municipio", try_cast_bigint(F.col("id_municipio")))
    .withColumn("id_contratado", try_cast_bigint(F.col("id_contratado")))
    .withColumn("id_grupo_matserv", try_cast_bigint(F.col("id_grupo_matserv")))
    .withColumn("id_classe_matserv", try_cast_bigint(F.col("id_classe_matserv")))
    .withColumn("id_material_servico", try_cast_bigint(F.col("id_material_servico")))
    .withColumn("id_item_matserv", try_cast_bigint(F.col("id_item_matserv")))
    .withColumn("id_processo", try_cast_bigint(F.col("id_processo")))
    .withColumn("id_contrato", try_cast_bigint(F.col("id_contrato")))
    .withColumn("id_unidade_medida", try_cast_bigint(F.col("id_unidade_medida")))
    .withColumn("id_linha_fornec", try_cast_bigint(F.col("id_linha_fornec")))
    .withColumn("id_item", try_cast_bigint(F.col("id_item")))
    .withColumn("id_unidade_orc", try_cast_bigint(F.col("id_unidade_orc")))
    .withColumn("ano_particao", try_cast_bigint(F.col("ano_particao")))
    # ── Data: datas anteriores a 2000-01-01 viram NULL ──
    .withColumn("dt_item_homologa", try_cast_date(F.col("dt_item_homologa")))
    .withColumn(
        "dt_item_homologa",
        F.when(F.col("dt_item_homologa") < F.lit("2000-01-01").cast("date"), F.lit(None).cast("date"))
        .otherwise(F.col("dt_item_homologa"))
    )
    # ── Quantidades e valores unitários → DECIMAL(18,4) ──
    .withColumn("qt_item_pedido", try_cast_decimal(F.col("qt_item_pedido"), 18, 4))
    .withColumn("vr_un_referencia", try_cast_decimal(F.col("vr_un_referencia"), 18, 4))
    .withColumn("vr_un_homologado", try_cast_decimal(F.col("vr_un_homologado"), 18, 4))
    # ── Valores totais → DECIMAL(18,2) ──
    .withColumn("vr_referencia", try_cast_decimal(F.col("vr_referencia"), 18, 2))
    .withColumn("vr_homologado", try_cast_decimal(F.col("vr_homologado"), 18, 2))
    .withColumn("vr_atualizado", try_cast_decimal(F.col("vr_atualizado"), 18, 2))
    # ── Colunas derivadas ──
    .withColumn("ano_homologacao", F.year(F.col("dt_item_homologa")).cast("int"))
    .withColumn("mes_homologacao", F.month(F.col("dt_item_homologa")).cast("int"))
    .withColumn(
        "fl_uemg",
        F.when(F.col("id_orgao_demanda") == F.lit(1831030), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "fl_possui_contrato",
        F.when(F.col("id_contrato") != F.lit(46296), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "fl_valor_zerado",
        F.when(F.col("vr_homologado") == F.lit(0).cast("decimal(18,2)"), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "vr_economia",
        (F.col("vr_referencia") - F.col("vr_homologado")).cast("decimal(18,2)")
    )
    .withColumn(
        "pc_economia",
        F.when(F.col("vr_referencia") != F.lit(0).cast("decimal(18,2)"),
               (F.col("vr_economia").cast("double") / F.col("vr_referencia").cast("double")).cast("decimal(9,4)"))
        .otherwise(F.lit(None).cast("decimal(9,4)"))
    )
    .withColumn(
        "vr_variacao_aditivo",
        (F.col("vr_atualizado") - F.col("vr_homologado")).cast("decimal(18,2)")
    )
    .withColumn(
        "fl_homologado_acima_referencia",
        F.when(F.col("vr_homologado") > F.col("vr_referencia"), F.lit(True)).otherwise(F.lit(False))
    )
)

# ── Detecção de linhas duplicadas (NÃO remover; apenas marcar) ──
w_dups = Window.partitionBy(*COLUNAS_NEGOCIO).orderBy(F.lit(1))
df = (
    df
    .withColumn("nr_ocorrencia", F.row_number().over(w_dups))
    .withColumn("fl_linha_duplicada", F.when(F.col("nr_ocorrencia") > 1, F.lit(True)).otherwise(F.lit(False)))
)

# ── sk_item_compra: hash estável da linha ──
df = df.withColumn(
    "sk_item_compra",
    F.sha2(F.concat_ws("|", *COLUNAS_NEGOCIO, F.col("nr_ocorrencia")), 256)
)

# Coluna técnica Silver
df = df.withColumn("_dt_processamento_silver", F.current_timestamp())

# Reordenar colunas
df = df.select(
    "sk_item_compra",
    "id_tempo", "id_procedimento", "id_orgao_demanda", "id_orgao_contrato",
    "id_situacao_proc", "id_situacao_cont", "id_municipio", "id_contratado",
    "id_grupo_matserv", "id_classe_matserv", "id_material_servico", "id_item_matserv",
    "id_processo", "id_contrato", "id_unidade_medida", "id_linha_fornec",
    "id_item", "id_unidade_orc", "ano_particao",
    "dt_item_homologa", "ano_homologacao", "mes_homologacao",
    "qt_item_pedido", "vr_un_referencia", "vr_referencia",
    "vr_un_homologado", "vr_homologado", "vr_atualizado",
    "fl_uemg", "fl_possui_contrato", "fl_valor_zerado",
    "vr_economia", "pc_economia", "vr_variacao_aditivo",
    "fl_homologado_acima_referencia",
    "nr_ocorrencia", "fl_linha_duplicada",
    "_dt_processamento_silver",
)

linhas_silver = df.count()
linhas_duplicadas = df.filter(F.col("fl_linha_duplicada")).count()

# Escrever (particionado por ano_particao)
escrever_tabela(df, TABELA_FULL, partition_cols=["ano_particao"])
print(f"  {TABELA_FULL}: {linhas_bronze} → {linhas_silver} linhas ({linhas_duplicadas} duplicatas marcadas)")

# Verificar PK única (sk_item_compra)
pk_duplicadas = spark.table(TABELA_FULL).groupBy("sk_item_compra").count().filter("count > 1").count()

# Falhas de cast (exceto dt_item_homologa 1900 esperada)
falhas = 0
for col_br, col_sv in [
    ("id_tempo", "id_tempo"), ("id_orgao_demanda", "id_orgao_demanda"),
    ("id_contratado", "id_contratado"), ("id_processo", "id_processo"),
    ("vr_homologado", "vr_homologado"), ("vr_referencia", "vr_referencia"),
    ("vr_atualizado", "vr_atualizado"), ("qt_item_pedido", "qt_item_pedido"),
]:
    br_nn = df_br.filter(F.col(col_br).isNotNull() & (F.trim(F.col(col_br)) != "")).select(col_br).distinct().count()
    sv_nn = df.filter(F.col(col_sv).isNotNull()).select(col_sv).distinct().count()
    falhas += max(br_nn - sv_nn, 0)

# ── Governança ──
coment_colunas = {}
for c in COLUNAS_NEGOCIO:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "sk_item_compra": "Chave única e estável da linha (SHA-256 de todas as colunas de negócio + nr_ocorrencia)",
    "ano_homologacao": "Ano da data de homologação do item",
    "mes_homologacao": "Mês da data de homologação do item",
    "fl_uemg": "True se o órgão demandante é a UEMG (id_orgao_demanda = 1831030)",
    "fl_possui_contrato": "True se há contrato formal (id_contrato != 46296). 46296 = compra sem contrato (situação 8)",
    "fl_valor_zerado": "True se vr_homologado = 0",
    "vr_economia": "Economia: vr_referencia - vr_homologado, em R$",
    "pc_economia": "Percentual de economia: vr_economia / vr_referencia (NULL se vr_referencia = 0)",
    "vr_variacao_aditivo": "Variação por aditivo: vr_atualizado - vr_homologado, em R$",
    "fl_homologado_acima_referencia": "True se vr_homologado > vr_referencia",
    "nr_ocorrencia": "Número da ocorrência dentro do grupo de linhas idênticas (1 = primeira)",
    "fl_linha_duplicada": "True se a linha é idêntica a outra (nr_ocorrencia > 1). Não é removida — decisão fica para a Gold",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Fato de compras do Estado de MG: itens homologados em processos de compra. "
    "Granularidade: 1 linha = 1 item homologado. "
    "Chave: sk_item_compra (SHA-256). "
    "Origem: bronze.ft_compras (Portal de Compras MG). "
    "fl_possui_contrato: id_contrato 46296 = compra sem contrato formal (situação 8). "
    "fl_linha_duplicada: linha idêntica a outra; não removida — decisão de deduplicação fica para a Gold.",
    coment_colunas,
)

resultados_fatos.append({
    "tabela": TABELA_FULL,
    "linhas_bronze": linhas_bronze,
    "linhas_silver": linhas_silver,
    "duplicatas_removidas": 0,
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,2. silver.fato_compras_contrato
# ── 2. silver.fato_compras_contrato (origem: bronze.ft_compras_contrato) ──
# PK: id_contrato + id_processo. fl_uemg derivado de fato_compras_item.

TABELA = "fato_compras_contrato"
ORIGEM = "ft_compras_contrato"
TABELA_FULL = f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
linhas_bronze = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

# Processos UEMG a partir de fato_compras_item (já criada)
df_processos_uemg = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_item")
    .filter(F.col("fl_uemg"))
    .select(F.col("id_processo").alias("proc_uemg")).distinct()
)

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    # IDs → BIGINT
    .withColumn("id_tempo", try_cast_bigint(F.col("id_tempo")))
    .withColumn("id_processo", try_cast_bigint(F.col("id_processo")))
    .withColumn("id_orgao_contrato", try_cast_bigint(F.col("id_orgao_contrato")))
    .withColumn("id_contrato", try_cast_bigint(F.col("id_contrato")))
    .withColumn("id_contratado", try_cast_bigint(F.col("id_contratado")))
    .withColumn("id_situacao_cont", try_cast_bigint(F.col("id_situacao_cont")))
    .withColumn("ano_particao", try_cast_bigint(F.col("ano_particao")))
    # Valores totais → DECIMAL(18,2)
    .withColumn("vr_homologado", try_cast_decimal(F.col("vr_homologado"), 18, 2))
    .withColumn("vr_atualizado", try_cast_decimal(F.col("vr_atualizado"), 18, 2))
)

# fl_uemg: processo existe em fato_compras_item com fl_uemg = true
df = df.join(df_processos_uemg, df["id_processo"] == df_processos_uemg["proc_uemg"], "left")
df = (
    df
    .withColumn("fl_uemg", F.when(F.col("proc_uemg").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    .drop("proc_uemg")
    .withColumn("vr_variacao_aditivo", (F.col("vr_atualizado") - F.col("vr_homologado")).cast("decimal(18,2)"))
    .withColumn(
        "pc_variacao_aditivo",
        F.when(F.col("vr_homologado") != F.lit(0).cast("decimal(18,2)"),
               (F.col("vr_variacao_aditivo").cast("double") / F.col("vr_homologado").cast("double")).cast("decimal(9,4)"))
        .otherwise(F.lit(None).cast("decimal(9,4)"))
    )
    .withColumn("_dt_processamento_silver", F.current_timestamp())
)

df = df.select(
    "id_tempo", "id_processo", "id_orgao_contrato", "id_contrato", "id_contratado",
    "id_situacao_cont", "ano_particao",
    "vr_homologado", "vr_atualizado",
    "fl_uemg", "vr_variacao_aditivo", "pc_variacao_aditivo",
    "_dt_processamento_silver",
)

linhas_silver = df.count()
escrever_tabela(df, TABELA_FULL)
print(f"  {TABELA_FULL}: {linhas_bronze} → {linhas_silver} linhas")

# PK: id_contrato + id_processo
pk_duplicadas = spark.table(TABELA_FULL).groupBy("id_contrato", "id_processo").count().filter("count > 1").count()

# Falhas de cast
falhas = 0
for col_br, col_sv in [("id_processo", "id_processo"), ("id_contrato", "id_contrato"), ("vr_homologado", "vr_homologado"), ("vr_atualizado", "vr_atualizado")]:
    br_nn = df_br.filter(F.col(col_br).isNotNull() & (F.trim(F.col(col_br)) != "")).select(col_br).distinct().count()
    sv_nn = df.filter(F.col(col_sv).isNotNull()).select(col_sv).distinct().count()
    falhas += max(br_nn - sv_nn, 0)

coment_colunas = {}
for c in ["id_tempo", "id_processo", "id_orgao_contrato", "id_contrato", "id_contratado", "id_situacao_cont", "ano_particao", "vr_homologado", "vr_atualizado"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "fl_uemg": "True se o processo possui itens de compra da UEMG em fato_compras_item",
    "vr_variacao_aditivo": "Variação por aditivo: vr_atualizado - vr_homologado, em R$",
    "pc_variacao_aditivo": "Percentual da variação: vr_variacao_aditivo / vr_homologado (NULL se vr_homologado = 0)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Fato de contratos do Estado de MG. Granularidade: 1 linha = 1 contrato. "
    "Chave: (id_contrato, id_processo). Origem: bronze.ft_compras_contrato (Portal de Compras MG).",
    coment_colunas,
)

resultados_fatos.append({
    "tabela": TABELA_FULL,
    "linhas_bronze": linhas_bronze,
    "linhas_silver": linhas_silver,
    "duplicatas_removidas": 0,
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,3. silver.fato_compras_empenho
# ── 3. silver.fato_compras_empenho (origem: bronze.fl_compras_empenho) ─
# PK: id_processo + id_empenho. NÃO agregar. Quebrar dotacao_orcamentaria via split.

TABELA = "fato_compras_empenho"
ORIGEM = "fl_compras_empenho"
TABELA_FULL = f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
linhas_bronze = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    # IDs → BIGINT
    .withColumn("id_processo", try_cast_bigint(F.col("id_processo")))
    .withColumn("id_empenho", try_cast_bigint(F.col("id_empenho")))
    .withColumn("id_programa", try_cast_bigint(F.col("id_programa")))
    .withColumn("id_acao", try_cast_bigint(F.col("id_acao")))
    .withColumn("id_elemento", try_cast_bigint(F.col("id_elemento")))
    .withColumn("id_item", try_cast_bigint(F.col("id_item")))
    .withColumn("dotacao_orcamentaria", trim_null(F.col("dotacao_orcamentaria")))
)

# ── Quebrar dotacao_orcamentaria via split ──
# partes = split(dotacao, ' ') → 4 partes
# partes[0] = cd_unidade_orcamentaria
# partes[1] = funcional → split('.') → cd_funcao, cd_subfuncao, cd_programa, cd_acao, cd_identificador
# partes[2] = natureza → split('.') → cd_categoria_economica, cd_grupo_despesa, cd_modalidade_aplicacao, cd_elemento_despesa, cd_subitem_despesa
# partes[3] = cd_fonte_recurso
partes = F.split(F.col("dotacao_orcamentaria"), " ")
funcional_parts = F.split(partes.getItem(1), "\\.")
natureza_parts = F.split(partes.getItem(2), "\\.")

df = (
    df
    .withColumn("cd_unidade_orcamentaria", partes.getItem(0))
    .withColumn("cd_funcao", funcional_parts.getItem(0))
    .withColumn("cd_subfuncao", funcional_parts.getItem(1))
    .withColumn("cd_programa", funcional_parts.getItem(2))
    .withColumn("cd_acao", funcional_parts.getItem(3))
    .withColumn("cd_identificador", funcional_parts.getItem(4))
    .withColumn("cd_categoria_economica", natureza_parts.getItem(0))
    .withColumn("cd_grupo_despesa", natureza_parts.getItem(1))
    .withColumn("cd_modalidade_aplicacao", natureza_parts.getItem(2))
    .withColumn("cd_elemento_despesa", natureza_parts.getItem(3))
    .withColumn("cd_subitem_despesa", natureza_parts.getItem(4))
    .withColumn("cd_fonte_recurso", partes.getItem(3))
    # cd_natureza_despesa = "categoria.grupo.modalidade.elemento"
    .withColumn(
        "cd_natureza_despesa",
        F.concat_ws(".",
            F.col("cd_categoria_economica"),
            F.col("cd_grupo_despesa"),
            F.col("cd_modalidade_aplicacao"),
            F.col("cd_elemento_despesa")
        )
    )
)

# ── Descrições derivadas ──
df = (
    df
    .withColumn(
        "ds_categoria_economica",
        F.when(F.col("cd_categoria_economica") == "3", F.lit("DESPESAS CORRENTES"))
        .when(F.col("cd_categoria_economica") == "4", F.lit("DESPESAS DE CAPITAL"))
        .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "ds_grupo_despesa",
        F.when(F.col("cd_grupo_despesa") == "1", F.lit("PESSOAL E ENCARGOS SOCIAIS"))
        .when(F.col("cd_grupo_despesa") == "2", F.lit("JUROS E ENCARGOS DA DIVIDA"))
        .when(F.col("cd_grupo_despesa") == "3", F.lit("OUTRAS DESPESAS CORRENTES"))
        .when(F.col("cd_grupo_despesa") == "4", F.lit("INVESTIMENTOS"))
        .when(F.col("cd_grupo_despesa") == "5", F.lit("INVERSOES FINANCEIRAS"))
        .when(F.col("cd_grupo_despesa") == "6", F.lit("AMORTIZACAO DA DIVIDA"))
        .otherwise(F.lit(None).cast("string"))
    )
)

# ── Validação da dotação ──
num_parts = F.size(F.split(F.col("dotacao_orcamentaria"), " "))
funcional_size = F.size(F.split(partes.getItem(1), "\\."))
natureza_size = F.size(F.split(partes.getItem(2), "\\."))
df = df.withColumn(
    "fl_dotacao_valida",
    F.when(
        (num_parts == 4) & (funcional_size == 5) & (natureza_size == 5),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# ── fl_uemg ──
df = df.withColumn(
    "fl_uemg",
    F.when(F.col("cd_unidade_orcamentaria") == "2350", F.lit(True)).otherwise(F.lit(False))
)

df = df.withColumn("_dt_processamento_silver", F.current_timestamp())

df = df.select(
    "id_processo", "id_empenho", "id_programa", "id_acao", "id_elemento", "id_item",
    "dotacao_orcamentaria",
    "cd_unidade_orcamentaria",
    "cd_funcao", "cd_subfuncao", "cd_programa", "cd_acao", "cd_identificador",
    "cd_categoria_economica", "cd_grupo_despesa", "cd_modalidade_aplicacao",
    "cd_elemento_despesa", "cd_subitem_despesa",
    "cd_fonte_recurso", "cd_natureza_despesa",
    "ds_categoria_economica", "ds_grupo_despesa",
    "fl_dotacao_valida", "fl_uemg",
    "_dt_processamento_silver",
)

linhas_silver = df.count()
escrever_tabela(df, TABELA_FULL)
print(f"  {TABELA_FULL}: {linhas_bronze} → {linhas_silver} linhas")

# PK: id_processo + id_empenho
pk_duplicadas = spark.table(TABELA_FULL).groupBy("id_processo", "id_empenho").count().filter("count > 1").count()

# Falhas de cast
falhas = 0
for col_br, col_sv in [("id_processo", "id_processo"), ("id_empenho", "id_empenho"), ("id_programa", "id_programa"), ("id_acao", "id_acao")]:
    br_nn = df_br.filter(F.col(col_br).isNotNull() & (F.trim(F.col(col_br)) != "")).select(col_br).distinct().count()
    sv_nn = df.filter(F.col(col_sv).isNotNull()).select(col_sv).distinct().count()
    falhas += max(br_nn - sv_nn, 0)

# Governança
coment_colunas = {}
for c in ["id_processo", "id_empenho", "id_programa", "id_acao", "id_elemento", "id_item", "dotacao_orcamentaria"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "cd_unidade_orcamentaria": "Código da unidade orçamentária (extraído da dotação)",
    "cd_funcao": "Código da função (extraído da dotação)",
    "cd_subfuncao": "Código da subfunção (extraído da dotação)",
    "cd_programa": "Código do programa orçamentário (extraído da dotação)",
    "cd_acao": "Código da ação orçamentária (extraído da dotação)",
    "cd_identificador": "Identificador da dotação (extraído da dotação)",
    "cd_categoria_economica": "Código da categoria econômica (3=correntes, 4=capital)",
    "cd_grupo_despesa": "Código do grupo de despesa (1-6)",
    "cd_modalidade_aplicacao": "Código da modalidade de aplicação (extraído da dotação)",
    "cd_elemento_despesa": "Código do elemento de despesa (extraído da dotação)",
    "cd_subitem_despesa": "Código do subitem de despesa (extraído da dotação)",
    "cd_fonte_recurso": "Código da fonte de recurso (extraído da dotação)",
    "cd_natureza_despesa": "Natureza da despesa no formato categoria.grupo.modalidade.elemento (ex.: 4.4.90.52)",
    "ds_categoria_economica": "Descrição da categoria econômica: DESPESAS CORRENTES ou DESPESAS DE CAPITAL",
    "ds_grupo_despesa": "Descrição do grupo de despesa (PESSOAL, JUROS, OUTRAS, INVESTIMENTOS etc.)",
    "fl_dotacao_valida": "True se o split gerou 4 partes com 5 subpartes na funcional e na natureza",
    "fl_uemg": "True se a unidade orçamentária é 2350 (UEMG)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Tabela ponte entre processos de compra e empenhos orçamentários. "
    "Granularidade: 1 linha = 1 empenho vinculado a um processo/item. "
    "Chave: (id_processo, id_empenho). NÃO agregar antes de juntar com fato_compras_item. "
    "Origem: bronze.fl_compras_empenho (Portal de Compras MG).",
    coment_colunas,
)

resultados_fatos.append({
    "tabela": TABELA_FULL,
    "linhas_bronze": linhas_bronze,
    "linhas_silver": linhas_silver,
    "duplicatas_removidas": 0,
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,4. Validação final e DQ
# ── 4. Validação final e gravação em dq_resultados ──────────────────
# Inclui integridade referencial e soma de vr_homologado da UEMG.

schema_dq = StructType([
    StructField("tabela", StringType(), True),
    StructField("linhas_bronze", LongType(), True),
    StructField("linhas_silver", LongType(), True),
    StructField("duplicatas_removidas", LongType(), True),
    StructField("pk_unica", BooleanType(), True),
    StructField("falhas_de_cast", LongType(), True),
    StructField("status", StringType(), True),
    StructField("notebook", StringType(), True),
    StructField("dt_execucao", LongType(), True),
])

# DataFrame de validação principal (StructType explícito para alinhamento correto)
schema_validacao = StructType([
    StructField("tabela", StringType(), True),
    StructField("linhas_bronze", LongType(), True),
    StructField("linhas_silver", LongType(), True),
    StructField("duplicatas_removidas", LongType(), True),
    StructField("pk_unica", BooleanType(), True),
    StructField("falhas_de_cast", LongType(), True),
    StructField("status", StringType(), True),
])

df_validacao = spark.createDataFrame(resultados_fatos, schema_validacao).orderBy("tabela")

print("═" * 70)
print("VALIDAÇÃO DAS FATOS SILVER")
print("═" * 70)
df_validacao.display()

# ── Integridade referencial: % de chaves de fato_compras_item nas dimensões ──
print("\n── Integridade Referencial (fato_compras_item → dimensões) ──")

df_fato = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_item")
total_fato = df_fato.count()

ri_results = []
for dim_table, fk_col, dim_pk in [
    ("dim_orgao", "id_orgao_demanda", "id_orgao_demanda"),
    ("dim_municipio", "id_municipio", "id_municipio"),
    ("dim_contratado", "id_contratado", "id_contratado"),
    ("dim_material_servico", "id_material_servico", "id_material_servico"),
    ("dim_item_matserv", "id_item_matserv", "id_item_matserv"),
]:
    total_fk = df_fato.filter(F.col(fk_col).isNotNull()).count()
    matched = (
        df_fato.filter(F.col(fk_col).isNotNull())
        .join(
            spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{dim_table}").select(F.col(dim_pk).alias("_dim_pk")).distinct(),
            F.col(fk_col) == F.col("_dim_pk"),
            "inner"
        )
        .count()
    )
    pct = round(100.0 * matched / total_fk, 2) if total_fk > 0 else 0.0
    ri_results.append({"dimensao": dim_table, "fk": fk_col, "total_fk": total_fk, "encontrados": matched, "pct": pct})
    print(f"  {dim_table:25s} {pct:6.2f}%  ({matched:,}/{total_fk:,})")

# ── Soma de vr_homologado da UEMG ──
soma_uemg = df_fato.filter(F.col("fl_uemg")).agg(F.sum("vr_homologado").alias("soma")).collect()[0]["soma"]
print(f"\n── Soma vr_homologado UEMG (fl_uemg=true): R$ {soma_uemg:,.2f} ──")

# ── Gravar em dq_resultados (append) ──
df_dq_append = (
    df_validacao
    .withColumn("notebook", F.lit(NOTEBOOK_NOME))
    .withColumn("dt_execucao", F.current_timestamp())
)

(
    df_dq_append.write.format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.dq_resultados")
)
print(f"\n✓ Resultados de DQ gravados em {CATALOGO}.{SCHEMA_SILVER}.dq_resultados (append)")

# Resumo final
qtd_ok = sum(1 for r in resultados_fatos if r["status"] == "OK")
todas_linhas_iguais = all(r["linhas_bronze"] == r["linhas_silver"] for r in resultados_fatos)
todas_pk_unica = all(r["pk_unica"] for r in resultados_fatos)

print(f"\n{'═' * 70}")
print(f"Resumo: {qtd_ok}/{len(resultados_fatos)} fatos OK, "
      f"linhas Bronze = Silver: {'Sim' if todas_linhas_iguais else 'NÃO'}, "
      f"PK única em todas: {'Sim' if todas_pk_unica else 'NÃO'}")
print(f"{'═' * 70}")