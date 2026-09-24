# Databricks notebook source
# DBTITLE 1,Silver — Dimensões
# MAGIC %md
# MAGIC # Silver — Dimensões
# MAGIC
# MAGIC **Projeto:** uemg_compras – Portal de Compras do Estado de Minas Gerais
# MAGIC
# MAGIC **Objetivo:** ler as dimensões da camada Bronze (todas STRING), aplicar tipagem, limpeza e
# MAGIC regras de negócio, e gravar tabelas Delta tipadas em `uemg_compras.silver`.
# MAGIC
# MAGIC **Execução:** IDEMPOTENTE (`overwrite` + `overwriteSchema`). Rodar após a ingestão Bronze
# MAGIC e a governança Bronze (comentários de coluna são copiados da Bronze).
# MAGIC
# MAGIC **Dimensões criadas:**
# MAGIC * `silver.dim_orgao` (origem: `bronze.dm_orgao_demanda`)
# MAGIC * `silver.dim_municipio` (origem: `bronze.dm_municipio`)
# MAGIC * `silver.dim_material_servico` (origem: `bronze.dm_material_servico`)
# MAGIC * `silver.dim_item_matserv` (origem: `bronze.dm_item_matserv`)
# MAGIC * `silver.dim_contratado` (origem: `bronze.dm_contratado`)

# COMMAND ----------

# DBTITLE 1,0. Imports e funções reutilizáveis
# ── 0. Imports, constantes e funções reutilizáveis ───────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
)
from pyspark.sql.column import Column

CATALOGO = "uemg_compras"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
RESPONSAVEL = "<responsavel>"
NOTEBOOK_NOME = "01_silver_dimensoes"

COLUNAS_TECNICAS_BRONZE = ["_arquivo_origem", "_dt_modificacao_arquivo", "_dt_ingestao"]

# ── Funções auxiliares ──────────────────────────────────────────────

def _esc(texto):
    """Escapa aspas simples para SQL string literals."""
    return texto.replace("'", "''")


def trim_null(col_expr):
    """trim() e converte string vazia para NULL."""
    c = F.trim(col_expr)
    return F.when(c == "", F.lit(None).cast("string")).otherwise(c)


def try_cast_bigint(col_expr):
    """Cast para BIGINT que nunca falha; retorna NULL para valores inválidos."""
    s = col_expr.cast("string")
    return F.when(s.rlike(r"^-?\d+$"), s.cast("bigint")).otherwise(F.lit(None).cast("bigint"))


def _comentarios_bronze(tabela_bronze):
    """Busca comentários de coluna da Bronze no information_schema."""
    df = spark.sql(
        f"SELECT column_name, comment "
        f"FROM {CATALOGO}.information_schema.columns "
        f"WHERE table_schema = '{SCHEMA_BRONZE}' AND table_name = '{tabela_bronze}' "
        f"  AND comment IS NOT NULL"
    )
    return {row["column_name"]: row["comment"] for row in df.collect()}


def aplicar_governanca_silver(
    tabela_silver,
    comentario_tabela,
    comentarios_colunas,
    colunas_pii=None
):
    """
    Aplica governança a uma tabela Silver: COMMENT ON TABLE, COMMENT em colunas,
    SET TAGS, e tags PII se houver.
    """
    tabela_full = f"{CATALOGO}.{SCHEMA_SILVER}.{tabela_silver}"
    colunas_pii = colunas_pii or []

    # COMMENT ON TABLE
    spark.sql(f"COMMENT ON TABLE {tabela_full} IS '{_esc(comentario_tabela)}'")

    # COMMENT em cada coluna
    for col_name, col_comment in comentarios_colunas.items():
        spark.sql(
            f"ALTER TABLE {tabela_full} "
            f"ALTER COLUMN {col_name} COMMENT '{_esc(col_comment)}'"
        )

    # SET TAGS na tabela
    spark.sql(
        f"ALTER TABLE {tabela_full} "
        f"SET TAGS ("
        f"'camada'='silver', 'fonte'='portal_compras_mg', "
        f"'dominio'='compras_publicas', 'tipo'='dimensao'"
        f")"
    )

    # SET TAGS PII nas colunas
    for col_pii in colunas_pii:
        spark.sql(
            f"ALTER TABLE {tabela_full} "
            f"ALTER COLUMN {col_pii} "
            f"SET TAGS ('pii'='true', 'lgpd'='dado_pessoal_anonimizado')"
        )

    # TBLPROPERTIES
    spark.sql(
        f"ALTER TABLE {tabela_full} "
        f"SET TBLPROPERTIES ("
        f"'fonte_dados'='Portal de Compras MG - dados abertos', "
        f"'frequencia_atualizacao'='manual', "
        f"'responsavel'='{RESPONSAVEL}', 'projeto'='uemg_compras'"
        f")"
    )
    print(f"  Governança aplicada em {tabela_full}")


def dedup_and_write(df, tabela_full, pk_col):
    """
    Deduplica por PK (manter 1 linha), escreve em overwrite+overwriteSchema,
    retorna dict com metadados da operação.
    """
    linhas_antes = df.count()
    df_dedup = df.dropDuplicates([pk_col])
    linhas_depois = df_dedup.count()
    duplicatas = linhas_antes - linhas_depois

    (
        df_dedup.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_full)
    )

    print(f"  {tabela_full}: {linhas_antes} → {linhas_depois} linhas "
          f"({duplicatas} duplicata(s) removida(s))")

    return {
        "linhas_bronze": linhas_antes,
        "linhas_silver": linhas_depois,
        "duplicatas_removidas": duplicatas,
    }


print("Funções utilitárias carregadas.")

# COMMAND ----------

# DBTITLE 1,1. silver.dim_orgao
# ── 1. silver.dim_orgao (origem: bronze.dm_orgao_demanda) ──────────
TABELA = "dim_orgao"
ORIGEM = "dm_orgao_demanda"
PK = "id_orgao_demanda"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
df_br_linhas = df_br.count()

# Copiar comentários da Bronze para colunas com mesmo nome
coment_bronze = _comentarios_bronze(ORIGEM)

df = (
    df_br
    # Remover colunas técnicas da Bronze
    .drop(*COLUNAS_TECNICAS_BRONZE)
    # Tipagem: chaves e códigos numéricos → BIGINT
    .withColumn(PK, try_cast_bigint(F.col(PK)))
    .withColumn("cd_orgao_demanda", try_cast_bigint(F.col("cd_orgao_demanda")))
    # Limpeza de texto
    .withColumn("nome", trim_null(F.col("nome")))
    # fl_registro_especial: código negativo
    .withColumn(
        "fl_registro_especial",
        F.when(F.col("cd_orgao_demanda") < 0, F.lit(True)).otherwise(F.lit(False))
    )
    # fl_inativo: nome contém INATIVO
    .withColumn(
        "fl_inativo",
        F.when(F.col("nome").rlike("(?i)INATIVO"), F.lit(True)).otherwise(F.lit(False))
    )
    # nome_original = nome como veio da origem
    .withColumn("nome_original", F.col("nome"))
    # nome_orgao = nome sem o sufixo de inativo
    .withColumn(
        "nome_orgao",
        trim_null(
            F.regexp_replace(
                F.col("nome"),
                r"\s*[-_/]*\s*INATIVO\.?\s*$",
                ""
            )
        )
    )
    # fl_uemg = (cd_orgao_demanda = 2350)
    .withColumn(
        "fl_uemg",
        F.when(F.col("cd_orgao_demanda") == 2350, F.lit(True)).otherwise(F.lit(False))
    )
    # Coluna técnica Silver
    .withColumn("_dt_processamento_silver", F.current_timestamp())
)

# Reordenar colunas
df = df.select(
    PK,
    "cd_orgao_demanda",
    "nome_orgao",
    "nome_original",
    "fl_inativo",
    "fl_registro_especial",
    "fl_uemg",
    "_dt_processamento_silver",
)

# Deduplicar e escrever
stats = dedup_and_write(df, f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}", PK)

# Verificar PK única
pk_duplicadas = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}")
    .groupBy(PK).count().filter("count > 1").count()
)

# Falhas de cast: valores não nulos na Bronze que viraram NULL na Silver
falhas_id = df_br.filter(F.col(PK).isNotNull() & F.col(PK).rlike("^[^0]+$")) \
    .select(PK).distinct().count() - df.filter(F.col(PK).isNotNull()).select(PK).distinct().count()
falhas_cd = df_br.filter(F.col("cd_orgao_demanda").isNotNull() & F.col("cd_orgao_demanda").rlike("^[^0]+$")) \
    .select("cd_orgao_demanda").distinct().count() - df.filter(F.col("cd_orgao_demanda").isNotNull()).select("cd_orgao_demanda").distinct().count()
falhas_cast = max(falhas_id, 0) + max(falhas_cd, 0)

# Governança
coment_colunas = {}
# Copiar da Bronze (colunas com mesmo nome)
for c in [PK, "cd_orgao_demanda"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
# Colunas derivadas
coment_colunas.update({
    "nome_orgao": "Nome do órgão sem sufixo de inativo",
    "nome_original": "Nome do órgão como veio da origem Bronze",
    "fl_inativo": "True se o órgão está inativo (nome continha INATIVO)",
    "fl_registro_especial": "True se o código é negativo (NÃO INFORMADO, INVÁLIDO etc.)",
    "fl_uemg": "True se o órgão é a UEMG (cd_orgao_demanda = 2350)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Dimensão de órgãos do Estado de MG que demandam compras. "
    "Granularidade: 1 linha = 1 órgão. "
    "Origem: bronze.dm_orgao_demanda (Portal de Compras MG).",
    coment_colunas,
)

resultados_dim = []
resultados_dim.append({
    "tabela": f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}",
    "linhas_bronze": stats["linhas_bronze"],
    "linhas_silver": stats["linhas_silver"],
    "duplicatas_removidas": stats["duplicatas_removidas"],
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas_cast,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,2. silver.dim_municipio
# ── 2. silver.dim_municipio (origem: bronze.dm_municipio) ─────────
TABELA = "dim_municipio"
ORIGEM = "dm_municipio"
PK = "id_municipio"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
df_br_linhas = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

# Mapeamento UF por prefixo IBGE
UF_MAP = {
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "43": "RS", "53": "DF",
}

# Construir expressão CASE para UF
uf_case = F.when(F.lit(False).isNotNull(), F.lit(None).cast("string"))
for prefix, uf in UF_MAP.items():
    uf_case = uf_case.when(
        (F.length(F.col("cd_ibge_str")) == 7)
        & (F.substring(F.col("cd_ibge_str"), 1, 2) == prefix),
        F.lit(uf)
    )

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    # Tipagem
    .withColumn(PK, try_cast_bigint(F.col(PK)))
    # cd_municipio_ibge como STRING (preservar) e numérica BIGINT
    .withColumn("cd_municipio_ibge", trim_null(F.col("cd_municipio_ibge")))
    .withColumn("cd_municipio_ibge_num", try_cast_bigint(F.col("cd_municipio_ibge")))
    .withColumn("nome", trim_null(F.col("nome")))
    # Alias temporário para uso nas derivadas
    .withColumn("cd_ibge_str", F.col("cd_municipio_ibge"))
    # fl_registro_especial
    .withColumn(
        "fl_registro_especial",
        F.when(F.col("cd_municipio_ibge_num") < 0, F.lit(True)).otherwise(F.lit(False))
    )
    # tipo_localidade
    .withColumn(
        "tipo_localidade",
        F.when(F.col("cd_municipio_ibge_num") < 0, F.lit("REGISTRO_ESPECIAL"))
        .when(
            (F.length(F.col("cd_ibge_str")) == 7)
            & (F.substring(F.col("cd_ibge_str"), 1, 2) == "31"),
            F.lit("MUNICIPIO_MG")
        )
        .when(
            (F.length(F.col("cd_ibge_str")) == 7)
            & (F.col("cd_municipio_ibge_num") >= 0),
            F.lit("MUNICIPIO_OUTRA_UF")
        )
        .otherwise(F.lit("REGIAO_OU_MARCADOR"))
    )
    # uf
    .withColumn("uf", uf_case)
    # fl_municipio_real
    .withColumn(
        "fl_municipio_real",
        F.when(
            F.col("tipo_localidade").isin("MUNICIPIO_MG", "MUNICIPIO_OUTRA_UF"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn("_dt_processamento_silver", F.current_timestamp())
    .drop("cd_ibge_str")
)

df = df.select(
    PK,
    "cd_municipio_ibge",
    "cd_municipio_ibge_num",
    "nome",
    "tipo_localidade",
    "uf",
    "fl_municipio_real",
    "fl_registro_especial",
    "_dt_processamento_silver",
)

stats = dedup_and_write(df, f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}", PK)
pk_duplicadas = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}")
    .groupBy(PK).count().filter("count > 1").count()
)
falhas_id = df_br.filter(F.col(PK).isNotNull() & F.col(PK).rlike("^[^0]+$")) \
    .select(PK).distinct().count() - df.filter(F.col(PK).isNotNull()).select(PK).distinct().count()
falhas_cast = max(falhas_id, 0)

coment_colunas = {}
for c in [PK, "cd_municipio_ibge", "nome"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "cd_municipio_ibge_num": "Código IBGE como BIGINT (para filtros numéricos)",
    "tipo_localidade": "Tipo: MUNICIPIO_MG, MUNICIPIO_OUTRA_UF, REGISTRO_ESPECIAL ou REGIAO_OU_MARCADOR",
    "uf": "Unidade federativa derivada dos 2 primeiros dígitos do IBGE (quando município)",
    "fl_municipio_real": "True se o registro é um município real (MG ou outra UF)",
    "fl_registro_especial": "True se o código é negativo (NÃO INFORMADO, INVÁLIDO etc.)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Dimensão de localidades: municípios de MG, de outras UFs, territórios e marcadores. "
    "Granularidade: 1 linha = 1 localidade. "
    "Origem: bronze.dm_municipio (Portal de Compras MG).",
    coment_colunas,
)

resultados_dim.append({
    "tabela": f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}",
    "linhas_bronze": stats["linhas_bronze"],
    "linhas_silver": stats["linhas_silver"],
    "duplicatas_removidas": stats["duplicatas_removidas"],
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas_cast,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,3. silver.dim_material_servico
# ── 3. silver.dim_material_servico (origem: bronze.dm_material_servico) ─
TABELA = "dim_material_servico"
ORIGEM = "dm_material_servico"
PK = "id_material_servico"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
df_br_linhas = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    .withColumn(PK, try_cast_bigint(F.col(PK)))
    .withColumn("cd_material_servico", try_cast_bigint(F.col("cd_material_servico")))
    .withColumn("nome", trim_null(F.col("nome")))
    # nome_material_servico = nome sem o " -" final
    .withColumn(
        "nome_material_servico",
        trim_null(F.regexp_replace(F.col("nome"), r"\s*-\s*$", ""))
    )
    .withColumn(
        "fl_registro_especial",
        F.when(F.col("cd_material_servico") < 0, F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("_dt_processamento_silver", F.current_timestamp())
)

df = df.select(
    PK,
    "cd_material_servico",
    "nome_material_servico",
    "fl_registro_especial",
    "_dt_processamento_silver",
)

stats = dedup_and_write(df, f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}", PK)
pk_duplicadas = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}")
    .groupBy(PK).count().filter("count > 1").count()
)
falhas_id = df_br.filter(F.col(PK).isNotNull() & F.col(PK).rlike("^[^0]+$")) \
    .select(PK).distinct().count() - df.filter(F.col(PK).isNotNull()).select(PK).distinct().count()
falhas_cd = df_br.filter(F.col("cd_material_servico").isNotNull() & F.col("cd_material_servico").rlike("^[^0]+$")) \
    .select("cd_material_servico").distinct().count() - df.filter(F.col("cd_material_servico").isNotNull()).select("cd_material_servico").distinct().count()
falhas_cast = max(falhas_id, 0) + max(falhas_cd, 0)

coment_colunas = {}
for c in [PK, "cd_material_servico"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "nome_material_servico": "Descrição genérica do material/serviço sem o sufixo ' -' final",
    "fl_registro_especial": "True se o código é negativo (NÃO INFORMADO, INVÁLIDO etc.)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Dimensão de materiais e serviços (descrição genérica). "
    "Granularidade: 1 linha = 1 material/serviço genérico. "
    "Origem: bronze.dm_material_servico (Portal de Compras MG).",
    coment_colunas,
)

resultados_dim.append({
    "tabela": f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}",
    "linhas_bronze": stats["linhas_bronze"],
    "linhas_silver": stats["linhas_silver"],
    "duplicatas_removidas": stats["duplicatas_removidas"],
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas_cast,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,4. silver.dim_item_matserv
# ── 4. silver.dim_item_matserv (origem: bronze.dm_item_matserv) ──────
TABELA = "dim_item_matserv"
ORIGEM = "dm_item_matserv"
PK = "id_item_matserv"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
df_br_linhas = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    .withColumn(PK, try_cast_bigint(F.col(PK)))
    .withColumn("cd_item_matserv", try_cast_bigint(F.col("cd_item_matserv")))
    .withColumn("nome", trim_null(F.col("nome")))
    .withColumn("natureza_despesa", trim_null(F.col("natureza_despesa")))
    # descricao_item = nome
    .withColumn("descricao_item", F.col("nome"))
    # nome_resumido_item = texto antes do primeiro " - "
    .withColumn(
        "nome_resumido_item",
        trim_null(F.substring_index(F.col("nome"), " - ", 1))
    )
    .withColumn(
        "fl_registro_especial",
        F.when(F.col("cd_item_matserv") < 0, F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("_dt_processamento_silver", F.current_timestamp())
)

df = df.select(
    PK,
    "cd_item_matserv",
    "descricao_item",
    "nome_resumido_item",
    "natureza_despesa",
    "fl_registro_especial",
    "_dt_processamento_silver",
)

stats = dedup_and_write(df, f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}", PK)
pk_duplicadas = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}")
    .groupBy(PK).count().filter("count > 1").count()
)
falhas_id = df_br.filter(F.col(PK).isNotNull() & F.col(PK).rlike("^[^0]+$")) \
    .select(PK).distinct().count() - df.filter(F.col(PK).isNotNull()).select(PK).distinct().count()
falhas_cd = df_br.filter(F.col("cd_item_matserv").isNotNull() & F.col("cd_item_matserv").rlike("^[^0]+$")) \
    .select("cd_item_matserv").distinct().count() - df.filter(F.col("cd_item_matserv").isNotNull()).select("cd_item_matserv").distinct().count()
falhas_cast = max(falhas_id, 0) + max(falhas_cd, 0)

coment_colunas = {}
for c in [PK, "cd_item_matserv", "natureza_despesa"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "descricao_item": "Descrição detalhada/especificação técnica do item",
    "nome_resumido_item": "Texto antes do primeiro ' - ' (nome resumido do item)",
    "fl_registro_especial": "True se o código é negativo (NÃO INFORMADO, INVÁLIDO etc.)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Dimensão de itens de material/serviço (descrição detalhada com especificação). "
    "Granularidade: 1 linha = 1 item de material/serviço detalhado. "
    "Origem: bronze.dm_item_matserv (Portal de Compras MG).",
    coment_colunas,
)

resultados_dim.append({
    "tabela": f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}",
    "linhas_bronze": stats["linhas_bronze"],
    "linhas_silver": stats["linhas_silver"],
    "duplicatas_removidas": stats["duplicatas_removidas"],
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas_cast,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,5. silver.dim_contratado
# ── 5. silver.dim_contratado (origem: bronze.dm_contratado) ───────
TABELA = "dim_contratado"
ORIGEM = "dm_contratado"
PK = "id_contratado"

df_br = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{ORIGEM}")
df_br_linhas = df_br.count()
coment_bronze = _comentarios_bronze(ORIGEM)

df = (
    df_br
    .drop(*COLUNAS_TECNICAS_BRONZE)
    .withColumn(PK, try_cast_bigint(F.col(PK)))
    .withColumn("tp_documento", try_cast_bigint(F.col("tp_documento")))
    .withColumn("nr_doc_orig", trim_null(F.col("nr_documento_anonimizado")))
    .withColumn("nome_contratado", trim_null(F.col("nome_anonimizado")))
    # tipo_pessoa
    .withColumn(
        "tipo_pessoa",
        F.when(F.col("tp_documento") == 2, F.lit("PJ"))
        .when(F.col("tp_documento") == 1, F.lit("PF"))
        .otherwise(F.lit("NAO_INFORMADO"))
    )
    # nr_documento: PJ → lpad 14, PF → manter mascarado, demais → NULL
    .withColumn(
        "nr_doc_pj",
        F.lpad(F.col("nr_doc_orig"), 14, "0")
    )
    .withColumn(
        "nr_documento",
        F.when(F.col("tp_documento") == 2, F.col("nr_doc_pj"))
        .when(F.col("tp_documento") == 1, F.col("nr_doc_orig"))
        .otherwise(F.lit(None).cast("string"))
    )
    # fl_documento_ajustado: lpad alterou o documento
    .withColumn(
        "fl_documento_ajustado",
        F.when(
            (F.col("tp_documento") == 2)
            & (F.length(F.col("nr_doc_orig")) < 14),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    # cnpj_raiz: 8 primeiros dígitos (PJ apenas)
    .withColumn(
        "cnpj_raiz",
        F.when(
            F.col("tp_documento") == 2,
            F.substring(F.col("nr_doc_pj"), 1, 8).cast("bigint")
        ).otherwise(F.lit(None).cast("bigint"))
    )
    # cnpj_formatado: XX.XXX.XXX/XXXX-XX (PJ apenas)
    .withColumn(
        "cnpj_formatado",
        F.when(
            F.col("tp_documento") == 2,
            F.concat(
                F.substring(F.col("nr_doc_pj"), 1, 2), F.lit("."),
                F.substring(F.col("nr_doc_pj"), 3, 3), F.lit("."),
                F.substring(F.col("nr_doc_pj"), 6, 3), F.lit("/"),
                F.substring(F.col("nr_doc_pj"), 9, 4), F.lit("-"),
                F.substring(F.col("nr_doc_pj"), 13, 2)
            )
        ).otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "fl_registro_especial",
        F.when(F.col("tp_documento") == 0, F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("_dt_processamento_silver", F.current_timestamp())
    .drop("nr_doc_orig", "nr_doc_pj", "nr_documento_anonimizado", "nome_anonimizado")
)

df = df.select(
    PK,
    "tp_documento",
    "tipo_pessoa",
    "nr_documento",
    "cnpj_raiz",
    "cnpj_formatado",
    "nome_contratado",
    "fl_documento_ajustado",
    "fl_registro_especial",
    "_dt_processamento_silver",
)

stats = dedup_and_write(df, f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}", PK)
pk_duplicadas = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}")
    .groupBy(PK).count().filter("count > 1").count()
)
falhas_id = df_br.filter(F.col(PK).isNotNull() & F.col(PK).rlike("^[^0]+$")) \
    .select(PK).distinct().count() - df.filter(F.col(PK).isNotNull()).select(PK).distinct().count()
falhas_tp = df_br.filter(F.col("tp_documento").isNotNull() & F.col("tp_documento").rlike("^[^0]+$")) \
    .select("tp_documento").distinct().count() - df.filter(F.col("tp_documento").isNotNull()).select("tp_documento").distinct().count()
falhas_cast = max(falhas_id, 0) + max(falhas_tp, 0)

coment_colunas = {}
for c in [PK, "tp_documento"]:
    if c in coment_bronze:
        coment_colunas[c] = coment_bronze[c]
coment_colunas.update({
    "tipo_pessoa": "Tipo de pessoa: PJ (CNPJ), PF (CPF) ou NAO_INFORMADO",
    "nr_documento": "Número do documento: CNPJ com 14 dígitos (lpad) ou CPF mascarado",
    "cnpj_raiz": "8 primeiros dígitos do CNPJ (identifica o grupo empresarial, PJ apenas)",
    "cnpj_formatado": "CNPJ no formato XX.XXX.XXX/XXXX-XX (PJ apenas)",
    "nome_contratado": "Razão social ou nome do fornecedor",
    "fl_documento_ajustado": "True quando o lpad adicionou zeros à esquerda ao CNPJ",
    "fl_registro_especial": "True se o registro é especial (sem documento / NÃO INFORMADO etc.)",
    "_dt_processamento_silver": "Data/hora de processamento na camada Silver",
})

aplicar_governanca_silver(
    TABELA,
    "Dimensão de fornecedores contratados (pessoa jurídica e física). "
    "Contém dados pessoais anonimizados — uso sujeito à LGPD. "
    "Granularidade: 1 linha = 1 fornecedor. "
    "Origem: bronze.dm_contratado (Portal de Compras MG).",
    coment_colunas,
    colunas_pii=["nr_documento", "nome_contratado", "cnpj_formatado"],
)

resultados_dim.append({
    "tabela": f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA}",
    "linhas_bronze": stats["linhas_bronze"],
    "linhas_silver": stats["linhas_silver"],
    "duplicatas_removidas": stats["duplicatas_removidas"],
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": falhas_cast,
    "status": "OK",
})
print(f"✓ {TABELA} concluída")

# COMMAND ----------

# DBTITLE 1,6. Validação final e DQ
# ── 6. Validação final e gravação em dq_resultados ────────────────
# DataFrame com schema explícito, linhas montadas como dicionário.
# Grava em append na tabela uemg_compras.silver.dq_resultados (trilha de auditoria).

schema_dq = StructType([
    StructField("tabela", StringType(), True),
    StructField("linhas_bronze", LongType(), True),
    StructField("linhas_silver", LongType(), True),
    StructField("duplicatas_removidas", LongType(), True),
    StructField("pk_unica", BooleanType(), True),
    StructField("falhas_de_cast", LongType(), True),
    StructField("status", StringType(), True),
    StructField("notebook", StringType(), True),
    StructField("dt_execucao", LongType(), True),  # current_timestamp() no append
])

# Montar linhas com dt_execucao
dt_exec = F.current_timestamp()
linhas_dq = []
for r in resultados_dim:
    linha = dict(r)
    linha["notebook"] = NOTEBOOK_NOME
    linha["dt_execucao"] = None  # será preenchido pelo Spark
    linhas_dq.append(linha)

# Criar DataFrame de validação (sem dt_execucao, que será adicionada via withColumn)
schema_display = StructType([
    StructField("tabela", StringType(), True),
    StructField("linhas_bronze", LongType(), True),
    StructField("linhas_silver", LongType(), True),
    StructField("duplicatas_removidas", LongType(), True),
    StructField("pk_unica", BooleanType(), True),
    StructField("falhas_de_cast", LongType(), True),
    StructField("status", StringType(), True),
])

df_validacao = spark.createDataFrame(
    [{k: v for k, v in linha.items() if k != "dt_execucao"} for linha in linhas_dq],
    schema_display
).orderBy("tabela")

print("═" * 70)
print("VALIDAÇÃO DAS DIMENSÕES SILVER")
print("═" * 70)
df_validacao.display()

# Criar tabela dq_resultados se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}.dq_resultados (
        tabela STRING,
        linhas_bronze BIGINT,
        linhas_silver BIGINT,
        duplicatas_removidas BIGINT,
        pk_unica BOOLEAN,
        falhas_de_cast BIGINT,
        status STRING,
        notebook STRING,
        dt_execucao TIMESTAMP
    )
""")

# Adicionar dt_execucao e gravar em append
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
qtd_ok = sum(1 for r in resultados_dim if r["status"] == "OK")
qtd_erro = sum(1 for r in resultados_dim if r["status"] != "OK")
total_duplicatas = sum(r["duplicatas_removidas"] for r in resultados_dim)
total_falhas = sum(r["falhas_de_cast"] for r in resultados_dim)
todas_pk_unica = all(r["pk_unica"] for r in resultados_dim)

print(f"\n{'═' * 70}")
print(f"Resumo: {qtd_ok}/{len(resultados_dim)} dimensões OK, "
      f"{qtd_erro} erro(s), {total_duplicatas} duplicata(s) removida(s), "
      f"{total_falhas} falha(s) de cast, "
      f"PK única em todas: {'Sim' if todas_pk_unica else 'NÃO'}")
print(f"{'═' * 70}")