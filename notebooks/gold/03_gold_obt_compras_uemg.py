# Databricks notebook source
# DBTITLE 1,Gold — OBT Compras UEMG
# MAGIC %md
# MAGIC # Gold — OBT Compras UEMG
# MAGIC
# MAGIC **Projeto:** uemg_compras – Portal de Compras do Estado de Minas Gerais
# MAGIC
# MAGIC **Objetivo:** construir `uemg_compras.gold.obt_compras_uemg`, uma One Big Table (OBT)
# MAGIC desnormalizada para análise das compras da UEMG, consumida por dashboards e pelo Genie.
# MAGIC
# MAGIC **Granularidade:** 1 linha = 1 item homologado da UEMG (`silver.fato_compras_item WHERE fl_uemg = true`).
# MAGIC Sem particionamento (tabela pequena, ~13,5 mil linhas). Idempotente.
# MAGIC
# MAGIC **Joins:** dimensões (LEFT JOIN), empenho (agregado antes do join), benchmark de preço (estado inteiro).

# COMMAND ----------

# DBTITLE 1,0. Imports e funções reutilizáveis
# ── 0. Imports, constantes e funções reutilizáveis ───────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
)

CATALOGO = "uemg_compras"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
RESPONSAVEL = "<responsavel>"
NOTEBOOK_NOME = "03_gold_obt_compras_uemg"
TABELA_FULL = f"{CATALOGO}.{SCHEMA_GOLD}.obt_compras_uemg"

spark.sql("SET spark.sql.ansi.enabled = false")

def _esc(texto):
    """Escapa aspas simples para SQL string literals."""
    return texto.replace("'", "''")

def _comentarios_silver(tabela_silver):
    """Busca comentários de coluna da Silver no information_schema."""
    df = spark.sql(
        f"SELECT column_name, comment "
        f"FROM {CATALOGO}.information_schema.columns "
        f"WHERE table_schema = '{SCHEMA_SILVER}' AND table_name = '{tabela_silver}' "
        f"  AND comment IS NOT NULL"
    )
    return {row["column_name"]: row["comment"] for row in df.collect()}

def _comentarios_multiplas_tabelas(tabelas_silver):
    """Busca comentários de múltiplas tabelas Silver. Colunas repetidas usam a primeira ocorrência."""
    resultado = {}
    for t in tabelas_silver:
        resultado.update(_comentarios_silver(t))
    return resultado

def aplicar_comentarios_colunas(tabela_full, comentarios):
    """Aplica COMMENT em todas as colunas da tabela."""
    for col_name, col_comment in comentarios.items():
        spark.sql(f"ALTER TABLE {tabela_full} ALTER COLUMN {col_name} COMMENT '{_esc(col_comment)}'")
    print(f"  Comentários aplicados em {len(comentarios)} colunas de {tabela_full}")

print("Funções utilitárias carregadas.")

# COMMAND ----------

# DBTITLE 1,1. Base OBT + joins dimensões
# ── 1. Base OBT: filtro UEMG + joins dimensões 1-5 + derivações temporais ─
# Granularidade: 1 linha = 1 item homologado da UEMG. LEFT JOINs não duplicam linhas.

# Base: fato_compras_item filtrada para UEMG
df_fato = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_item").filter(F.col("fl_uemg"))

linhas_base = df_fato.count()
soma_base = df_fato.agg(F.sum("vr_homologado").alias("soma")).collect()[0]["soma"]
print(f"Base UEMG: {linhas_base} linhas, soma vr_homologado = R$ {soma_base:,.2f}")

# ── Join 1: dim_orgao ON id_orgao_demanda → nome_orgao_demanda ──
df_orgao = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_orgao").select(
    F.col("id_orgao_demanda"),
    F.col("nome_orgao").alias("nome_orgao_demanda"),
)

# ── Join 2: dim_municipio ON id_municipio ──
df_municipio = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_municipio").select(
    F.col("id_municipio"),
    F.col("nome").alias("nome_localidade"),
    F.col("cd_municipio_ibge"),
    F.col("tipo_localidade"),
    F.col("uf"),
    F.col("fl_municipio_real"),
)

# ── Join 3: dim_contratado ON id_contratado ──
df_contratado = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_contratado").select(
    F.col("id_contratado"),
    F.col("nome_contratado").alias("nome_fornecedor"),
    F.col("tipo_pessoa"),
    F.col("nr_documento").alias("nr_documento_fornecedor"),
    F.col("cnpj_raiz"),
    F.col("cnpj_formatado"),
)

# ── Join 4: dim_material_servico ON id_material_servico ──
df_mat_serv = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_material_servico").select(
    F.col("id_material_servico"),
    F.col("nome_material_servico"),
)

# ── Join 5: dim_item_matserv ON id_item_matserv ──
df_item = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_item_matserv").select(
    F.col("id_item_matserv"),
    F.col("descricao_item"),
    F.col("nome_resumido_item"),
    F.col("natureza_despesa").alias("natureza_despesa_item"),
)

# Aplicar todos os LEFT JOINs (aliasing para evitar ambiguidade)
df_obt = (
    df_fato.alias("f")
    .join(df_orgao.alias("d1"), F.col("f.id_orgao_demanda") == F.col("d1.id_orgao_demanda"), "left")
    .join(df_municipio.alias("d2"), F.col("f.id_municipio") == F.col("d2.id_municipio"), "left")
    .join(df_contratado.alias("d3"), F.col("f.id_contratado") == F.col("d3.id_contratado"), "left")
    .join(df_mat_serv.alias("d4"), F.col("f.id_material_servico") == F.col("d4.id_material_servico"), "left")
    .join(df_item.alias("d5"), F.col("f.id_item_matserv") == F.col("d5.id_item_matserv"), "left")
    # Selecionar colunas da fato e das dims (com prefixo f. e aliases das dims)
    .select(
        F.col("f.sk_item_compra"),
        F.col("f.id_processo"),
        F.col("f.id_contrato"),
        F.col("f.id_linha_fornec"),
        F.col("f.id_item"),
        # Tempo
        F.col("f.dt_item_homologa"),
        F.col("f.ano_particao"),
        F.col("f.ano_homologacao"),
        F.col("f.mes_homologacao"),
        F.col("f.id_orgao_demanda"),
        F.col("d1.nome_orgao_demanda"),
        F.col("f.id_orgao_contrato"),
        F.col("f.id_unidade_orc"),
        F.col("f.id_procedimento"),
        F.col("f.id_situacao_proc"),
        F.col("f.id_situacao_cont"),
        F.col("f.fl_possui_contrato"),
        F.col("f.id_municipio"),
        F.col("d2.nome_localidade"),
        F.col("d2.cd_municipio_ibge"),
        F.col("d2.tipo_localidade"),
        F.col("d2.uf"),
        F.col("d2.fl_municipio_real"),
        F.col("f.id_contratado"),
        F.col("d3.nome_fornecedor"),
        F.col("d3.tipo_pessoa"),
        F.col("d3.nr_documento_fornecedor"),
        F.col("d3.cnpj_raiz"),
        F.col("d3.cnpj_formatado"),
        F.col("f.id_grupo_matserv"),
        F.col("f.id_classe_matserv"),
        F.col("f.id_material_servico"),
        F.col("f.id_item_matserv"),
        F.col("f.id_unidade_medida"),
        F.col("d4.nome_material_servico"),
        F.col("d5.descricao_item"),
        F.col("d5.nome_resumido_item"),
        F.col("d5.natureza_despesa_item"),
        # Valores
        F.col("f.qt_item_pedido"),
        F.col("f.vr_un_referencia"),
        F.col("f.vr_referencia"),
        F.col("f.vr_un_homologado"),
        F.col("f.vr_homologado"),
        F.col("f.vr_atualizado"),
        F.col("f.vr_economia"),
        F.col("f.pc_economia"),
        F.col("f.vr_variacao_aditivo"),
        F.col("f.fl_homologado_acima_referencia"),
        F.col("f.fl_valor_zerado"),
        # Qualidade
        F.col("f.fl_linha_duplicada"),
    )
)

# ── Derivações temporais ──
nome_mes_expr = (
    F.when(F.col("mes_homologacao") == 1, F.lit("Janeiro"))
    .when(F.col("mes_homologacao") == 2, F.lit("Fevereiro"))
    .when(F.col("mes_homologacao") == 3, F.lit("Março"))
    .when(F.col("mes_homologacao") == 4, F.lit("Abril"))
    .when(F.col("mes_homologacao") == 5, F.lit("Maio"))
    .when(F.col("mes_homologacao") == 6, F.lit("Junho"))
    .when(F.col("mes_homologacao") == 7, F.lit("Julho"))
    .when(F.col("mes_homologacao") == 8, F.lit("Agosto"))
    .when(F.col("mes_homologacao") == 9, F.lit("Setembro"))
    .when(F.col("mes_homologacao") == 10, F.lit("Outubro"))
    .when(F.col("mes_homologacao") == 11, F.lit("Novembro"))
    .when(F.col("mes_homologacao") == 12, F.lit("Dezembro"))
    .otherwise(F.lit(None).cast("string"))
)

df_obt = (
    df_obt
    .withColumn("trimestre_homologacao", F.quarter(F.col("dt_item_homologa")).cast("int"))
    .withColumn("semestre_homologacao", F.when(F.col("mes_homologacao") <= 6, F.lit(1)).otherwise(F.lit(2)).cast("int"))
    .withColumn("nome_mes", nome_mes_expr)
    .withColumn("ano_mes", F.date_format(F.col("dt_item_homologa"), "yyyy-MM"))
    .withColumn(
        "fl_ano_incompleto",
        F.when((F.col("ano_homologacao") == 2025) | (F.col("ano_particao") == 2025), F.lit(True)).otherwise(F.lit(False))
    )
)

print(f"Após joins dimensões: {df_obt.count()} linhas (deve ser {linhas_base})")

# COMMAND ----------

# DBTITLE 1,2. Empenho agregado
# ── 2. Empenho: agregar ANTES do join para garantir 1 linha por chave ──
# Nivel item: agrupar por (id_processo, id_item). Fallback: nivel processo.
# origem_dotacao = 'ITEM' | 'PROCESSO' | 'SEM_EMPENHO'

df_emp_raw = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_empenho")

# Agregacões com chaves renomeadas para evitar ambiguidade no join
df_emp_item = (
    df_emp_raw
    .groupBy("id_processo", "id_item")
    .agg(
        F.count(F.lit(1)).alias("qtd_empenhos_item"),
        F.first("cd_natureza_despesa", ignorenulls=True).alias("cd_natureza_despesa_item"),
        F.first("ds_categoria_economica", ignorenulls=True).alias("ds_categoria_economica_item"),
        F.first("ds_grupo_despesa", ignorenulls=True).alias("ds_grupo_despesa_item"),
        F.first("cd_elemento_despesa", ignorenulls=True).alias("cd_elemento_despesa_item"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_fonte_recurso"))).alias("fontes_recurso_item"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_acao"))).alias("acoes_orcamentarias_item"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_programa"))).alias("programas_orcamentarios_item"),
        F.first("cd_funcao", ignorenulls=True).alias("cd_funcao_item"),
        F.first("cd_subfuncao", ignorenulls=True).alias("cd_subfuncao_item"),
    )
    .select(
        F.col("id_processo").alias("_emp_proc_item"),
        F.col("id_item").alias("_emp_item_item"),
        F.col("qtd_empenhos_item"),
        F.col("cd_natureza_despesa_item"),
        F.col("ds_categoria_economica_item"),
        F.col("ds_grupo_despesa_item"),
        F.col("cd_elemento_despesa_item"),
        F.col("fontes_recurso_item"),
        F.col("acoes_orcamentarias_item"),
        F.col("programas_orcamentarios_item"),
        F.col("cd_funcao_item"),
        F.col("cd_subfuncao_item"),
    )
)

df_emp_proc = (
    df_emp_raw
    .groupBy("id_processo")
    .agg(
        F.count(F.lit(1)).alias("qtd_empenhos_proc"),
        F.first("cd_natureza_despesa", ignorenulls=True).alias("cd_natureza_despesa_proc"),
        F.first("ds_categoria_economica", ignorenulls=True).alias("ds_categoria_economica_proc"),
        F.first("ds_grupo_despesa", ignorenulls=True).alias("ds_grupo_despesa_proc"),
        F.first("cd_elemento_despesa", ignorenulls=True).alias("cd_elemento_despesa_proc"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_fonte_recurso"))).alias("fontes_recurso_proc"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_acao"))).alias("acoes_orcamentarias_proc"),
        F.concat_ws(", ", F.array_sort(F.collect_set("cd_programa"))).alias("programas_orcamentarios_proc"),
        F.first("cd_funcao", ignorenulls=True).alias("cd_funcao_proc"),
        F.first("cd_subfuncao", ignorenulls=True).alias("cd_subfuncao_proc"),
    )
    .select(
        F.col("id_processo").alias("_emp_proc_proc"),
        F.col("qtd_empenhos_proc"),
        F.col("cd_natureza_despesa_proc"),
        F.col("ds_categoria_economica_proc"),
        F.col("ds_grupo_despesa_proc"),
        F.col("cd_elemento_despesa_proc"),
        F.col("fontes_recurso_proc"),
        F.col("acoes_orcamentarias_proc"),
        F.col("programas_orcamentarios_proc"),
        F.col("cd_funcao_proc"),
        F.col("cd_subfuncao_proc"),
    )
)

# Join nivel item (chaves renomeadas evitam ambiguidade)
df_obt = (
    df_obt
    .join(df_emp_item,
          (F.col("id_processo") == F.col("_emp_proc_item")) &
          (F.col("id_item") == F.col("_emp_item_item")),
          "left")
    .drop("_emp_proc_item", "_emp_item_item")
)

# Join nivel processo (fallback)
df_obt = (
    df_obt
    .join(df_emp_proc,
          F.col("id_processo") == F.col("_emp_proc_proc"),
          "left")
    .drop("_emp_proc_proc")
)

# Coalesce campo a campo: item primeiro, processo depois, NULL se nenhum
df_obt = (
    df_obt
    .withColumn("qtd_empenhos", F.coalesce(F.col("qtd_empenhos_item"), F.col("qtd_empenhos_proc")))
    .withColumn("cd_natureza_despesa", F.coalesce(F.col("cd_natureza_despesa_item"), F.col("cd_natureza_despesa_proc")))
    .withColumn("ds_categoria_economica", F.coalesce(F.col("ds_categoria_economica_item"), F.col("ds_categoria_economica_proc")))
    .withColumn("ds_grupo_despesa", F.coalesce(F.col("ds_grupo_despesa_item"), F.col("ds_grupo_despesa_proc")))
    .withColumn("cd_elemento_despesa", F.coalesce(F.col("cd_elemento_despesa_item"), F.col("cd_elemento_despesa_proc")))
    .withColumn("fontes_recurso", F.coalesce(F.col("fontes_recurso_item"), F.col("fontes_recurso_proc")))
    .withColumn("acoes_orcamentarias", F.coalesce(F.col("acoes_orcamentarias_item"), F.col("acoes_orcamentarias_proc")))
    .withColumn("programas_orcamentarios", F.coalesce(F.col("programas_orcamentarios_item"), F.col("programas_orcamentarios_proc")))
    .withColumn("cd_funcao", F.coalesce(F.col("cd_funcao_item"), F.col("cd_funcao_proc")))
    .withColumn("cd_subfuncao", F.coalesce(F.col("cd_subfuncao_item"), F.col("cd_subfuncao_proc")))
    .withColumn(
        "origem_dotacao",
        F.when(F.col("qtd_empenhos_item").isNotNull(), F.lit("ITEM"))
        .when(F.col("qtd_empenhos_proc").isNotNull(), F.lit("PROCESSO"))
        .otherwise(F.lit("SEM_EMPENHO"))
    )
    .drop(
        "qtd_empenhos_item", "qtd_empenhos_proc",
        "cd_natureza_despesa_item", "cd_natureza_despesa_proc",
        "ds_categoria_economica_item", "ds_categoria_economica_proc",
        "ds_grupo_despesa_item", "ds_grupo_despesa_proc",
        "cd_elemento_despesa_item", "cd_elemento_despesa_proc",
        "fontes_recurso_item", "fontes_recurso_proc",
        "acoes_orcamentarias_item", "acoes_orcamentarias_proc",
        "programas_orcamentarios_item", "programas_orcamentarios_proc",
        "cd_funcao_item", "cd_funcao_proc",
        "cd_subfuncao_item", "cd_subfuncao_proc",
    )
)

print(f"Após join empenho: {df_obt.count()} linhas")

# COMMAND ----------

# DBTITLE 1,3. Benchmark + write
# ── 3. Benchmark de preço (estado inteiro) + final + write ──────────
# Benchmark sobre silver.fato_compras_item do ESTADO INTEIRO,
# excluindo fl_valor_zerado e fl_linha_duplicada.

df_estado = (
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_item")
    .filter(~F.col("fl_valor_zerado") & ~F.col("fl_linha_duplicada"))
)

df_benchmark = (
    df_estado
    .groupBy("id_item_matserv", "id_unidade_medida", "ano_homologacao")
    .agg(
        F.percentile_approx("vr_un_homologado", 0.5).alias("vr_un_mediana_estado"),
        F.count(F.lit(1)).alias("qtd_compras_estado"),
        F.countDistinct("id_orgao_demanda").alias("qtd_orgaos_estado"),
    )
)

# Join benchmark na OBT (LEFT JOIN por 3 chaves)
df_obt = (
    df_obt.alias("o")
    .join(df_benchmark.alias("b"),
          (F.col("o.id_item_matserv") == F.col("b.id_item_matserv")) &
          (F.col("o.id_unidade_medida") == F.col("b.id_unidade_medida")) &
          (F.col("o.ano_homologacao") == F.col("b.ano_homologacao")),
          "left")
    .select(
        F.col("o.*"),
        F.col("b.vr_un_mediana_estado"),
        F.col("b.qtd_compras_estado"),
        F.col("b.qtd_orgaos_estado"),
    )
)

# Só considerar benchmark válido quando qtd_compras_estado >= 5
df_obt = (
    df_obt
    .withColumn(
        "vr_un_mediana_estado",
        F.when(F.col("qtd_compras_estado") >= 5, F.col("vr_un_mediana_estado")).otherwise(F.lit(None))
    )
    .withColumn(
        "qtd_compras_estado",
        F.when(F.col("qtd_compras_estado") >= 5, F.col("qtd_compras_estado")).otherwise(F.lit(None))
    )
    .withColumn(
        "qtd_orgaos_estado",
        F.when(F.col("qtd_orgaos_estado") >= 5, F.col("qtd_orgaos_estado")).otherwise(F.lit(None))
    )
    .withColumn(
        "pc_diferenca_mediana_estado",
        F.when(
            F.col("vr_un_mediana_estado").isNotNull() & (F.col("vr_un_mediana_estado") != 0),
            (F.col("vr_un_homologado").cast("double") / F.col("vr_un_mediana_estado").cast("double") - 1).cast("decimal(9,4)")
        ).otherwise(F.lit(None).cast("decimal(9,4)"))
    )
    .withColumn(
        "faixa_preco_vs_estado",
        F.when(F.col("pc_diferenca_mediana_estado").isNull(), F.lit("SEM_BENCHMARK"))
        .when(F.col("pc_diferenca_mediana_estado") <= F.lit(-0.10).cast("decimal(9,4)"), F.lit("ABAIXO"))
        .when(F.col("pc_diferenca_mediana_estado") > F.lit(0.10).cast("decimal(9,4)"), F.lit("ACIMA"))
        .otherwise(F.lit("NA_MEDIA"))
    )
    .withColumn("_dt_processamento_gold", F.current_timestamp())
)

# ── Ordem final das colunas ──
COLUNAS_FINAIS = [
    # Identificação
    "sk_item_compra", "id_processo", "id_contrato", "id_linha_fornec", "id_item",
    # Tempo
    "dt_item_homologa", "ano_particao", "ano_homologacao", "mes_homologacao",
    "trimestre_homologacao", "semestre_homologacao", "nome_mes", "ano_mes", "fl_ano_incompleto",
    # Órgão
    "id_orgao_demanda", "nome_orgao_demanda", "id_orgao_contrato", "id_unidade_orc",
    # Processo
    "id_procedimento", "id_situacao_proc", "id_situacao_cont", "fl_possui_contrato",
    # Local
    "id_municipio", "nome_localidade", "cd_municipio_ibge", "tipo_localidade", "uf", "fl_municipio_real",
    # Fornecedor
    "id_contratado", "nome_fornecedor", "tipo_pessoa", "nr_documento_fornecedor", "cnpj_raiz", "cnpj_formatado",
    # Objeto
    "id_grupo_matserv", "id_classe_matserv", "id_material_servico", "id_item_matserv", "id_unidade_medida",
    "nome_material_servico", "descricao_item", "nome_resumido_item", "natureza_despesa_item",
    # Orçamento
    "origem_dotacao", "qtd_empenhos", "cd_natureza_despesa", "ds_categoria_economica",
    "ds_grupo_despesa", "cd_elemento_despesa", "fontes_recurso", "acoes_orcamentarias",
    "programas_orcamentarios", "cd_funcao", "cd_subfuncao",
    # Valores
    "qt_item_pedido", "vr_un_referencia", "vr_referencia", "vr_un_homologado",
    "vr_homologado", "vr_atualizado", "vr_economia", "pc_economia",
    "vr_variacao_aditivo", "fl_homologado_acima_referencia", "fl_valor_zerado",
    # Benchmark
    "vr_un_mediana_estado", "qtd_compras_estado", "qtd_orgaos_estado",
    "pc_diferenca_mediana_estado", "faixa_preco_vs_estado",
    # Qualidade/técnicas
    "fl_linha_duplicada", "_dt_processamento_gold",
]

df_obt = df_obt.select(*COLUNAS_FINAIS)

linhas_obt = df_obt.count()
print(f"OBT final: {linhas_obt} linhas, {len(COLUNAS_FINAIS)} colunas")

# Escrever tabela (overwrite + overwriteSchema, sem particionamento)
(
    df_obt.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_FULL)
)
print(f"✓ Tabela {TABELA_FULL} escrita")

# COMMAND ----------

# DBTITLE 1,4. Governança
# ── 4. Governança: COMMENT ON TABLE, colunas, SET TAGS, PII ────────

# COMMENT ON TABLE
spark.sql(
    f"COMMENT ON TABLE {TABELA_FULL} IS '"
    f"One Big Table das compras da Universidade do Estado de Minas Gerais (UEMG) — "
    f"1 linha por item homologado, 2009 a 2025 (2025 parcial). Fonte: Portal de Compras MG. "
    f"Valores em R$. Use vr_homologado como valor principal de gasto.'"
)

# Buscar comentários das tabelas Silver de origem
dic_silver = _comentarios_multiplas_tabelas([
    "fato_compras_item", "dim_orgao", "dim_municipio", "dim_contratado",
    "dim_material_servico", "dim_item_matserv", "fato_compras_empenho",
])

# Mapear nomes de coluna Silver → OBT onde foram renomeados
rename_map = {
    "nome_orgao": "nome_orgao_demanda",
    "nome": "nome_localidade",
    "nome_contratado": "nome_fornecedor",
    "nr_documento": "nr_documento_fornecedor",
    "natureza_despesa": "natureza_despesa_item",
}

# Construir dicionário de comentários para todas as colunas da OBT
comentarios = {}
for col in COLUNAS_FINAIS:
    # Buscar comentário original (pelo nome OBT ou pelo nome Silver renomeado)
    silver_name = col
    for sn, obt in rename_map.items():
        if obt == col:
            silver_name = sn
            break
    if silver_name in dic_silver:
        comentarios[col] = dic_silver[silver_name]

# Comentários obrigatórios de regras de negócio (sobrescrevem se existirem)
comentarios.update({
    "vr_homologado": "Valor total do item após a licitação (R$). Métrica principal de gasto.",
    "vr_economia": "Diferença entre valor de referência e homologado; positivo = economia.",
    "pc_economia": "Percentual de economia: (vr_referencia - vr_homologado) / vr_referencia. Positivo = economia.",
    "vr_variacao_aditivo": "Variação do valor do item após aditivos/reajustes (atualizado - homologado).",
    "fl_possui_contrato": "false = compra sem contrato formal (ex.: empenho/autorização de fornecimento direta).",
    "fl_ano_incompleto": "Ano com dados parciais; não comparar com anos completos sem ressalva.",
    "tipo_localidade": "MUNICIPIO_MG, MUNICIPIO_OUTRA_UF, REGIAO_OU_MARCADOR (ex.: CONFORME EDITAL) ou REGISTRO_ESPECIAL.",
    "faixa_preco_vs_estado": "Preço unitário da UEMG comparado à mediana estadual do mesmo item, unidade e ano.",
    "fl_linha_duplicada": "Linha idêntica a outra na origem; mantida por não haver prova de erro.",
})

# Comentários para colunas novas (não presentes na Silver)
comentarios_novos = {
    "trimestre_homologacao": "Trimestre da data de homologação (1-4)",
    "semestre_homologacao": "Semestre da data de homologação (1 ou 2)",
    "nome_mes": "Nome do mês da homologação em português",
    "ano_mes": "Ano e mês da homologação no formato yyyy-MM",
    "nome_orgao_demanda": "Nome do órgão que demandou a compra (UEMG)",
    "nome_localidade": "Nome do município ou localidade da compra",
    "cd_municipio_ibge": "Código IBGE do município",
    "fl_municipio_real": "true se é um município real (não marcador ou registro especial)",
    "nome_fornecedor": "Nome do fornecedor contratado",
    "tipo_pessoa": "Tipo de pessoa: FISICA ou JURIDICA",
    "nr_documento_fornecedor": "Número do documento (CPF/CNPJ) do fornecedor — dado anonimizado",
    "cnpj_raiz": "Raiz do CNPJ (8 primeiros dígitos) quando aplicável",
    "cnpj_formatado": "CNPJ formatado (##.###.###/####-##) quando aplicável",
    "nome_material_servico": "Nome do material ou serviço genérico",
    "descricao_item": "Descrição detalhada do item de material/serviço",
    "nome_resumido_item": "Nome resumido do item",
    "natureza_despesa_item": "Natureza da despesa do item",
    "origem_dotacao": "Origem dos dados de dotação orçamentária: ITEM (empenho por item), PROCESSO (empenho por processo), SEM_EMPENHO",
    "qtd_empenhos": "Quantidade de empenhos vinculados ao item/processo",
    "cd_natureza_despesa": "Código da natureza da despesa (mais frequente nos empenhos)",
    "ds_categoria_economica": "Descrição da categoria econômica do empenho",
    "ds_grupo_despesa": "Descrição do grupo de despesa do empenho",
    "cd_elemento_despesa": "Código do elemento de despesa (do empenho mais frequente)",
    "fontes_recurso": "Fontes de recurso (lista única ordenada, separada por vírgula)",
    "acoes_orcamentarias": "Ações orçamentárias (lista única ordenada, separada por vírgula)",
    "programas_orcamentarios": "Programas orçamentários (lista única ordenada, separada por vírgula)",
    "cd_funcao": "Código da função (mais frequente nos empenhos)",
    "cd_subfuncao": "Código da subfunção (mais frequente nos empenhos)",
    "vr_un_mediana_estado": "Mediana do valor unitário homologado no estado inteiro para o mesmo item, unidade e ano",
    "qtd_compras_estado": "Número de compras no estado inteiro para o mesmo item, unidade e ano (mínimo 5)",
    "qtd_orgaos_estado": "Número de órgãos distintos no estado inteiro que compraram o mesmo item, unidade e ano",
    "pc_diferenca_mediana_estado": "Percentual de diferença: (vr_un_homologado / vr_un_mediana_estado) - 1",
    "_dt_processamento_gold": "Data/hora de processamento na camada Gold",
}
comentarios.update(comentarios_novos)

aplicar_comentarios_colunas(TABELA_FULL, comentarios)

# SET TAGS
spark.sql(
    f"ALTER TABLE {TABELA_FULL} SET TAGS ("
    f"'camada'='gold', 'fonte'='portal_compras_mg', "
    f"'dominio'='compras_publicas', 'tipo'='obt', 'consumo'='genie_dashboard'"
    f")"
)

# PII tags
for col_pii in ["nome_fornecedor", "nr_documento_fornecedor", "cnpj_formatado"]:
    spark.sql(
        f"ALTER TABLE {TABELA_FULL} ALTER COLUMN {col_pii} "
        f"SET TAGS ('pii'='true', 'lgpd'='dado_pessoal_anonimizado')"
    )

# TBLPROPERTIES
spark.sql(
    f"ALTER TABLE {TABELA_FULL} SET TBLPROPERTIES ("
    f"'fonte_dados'='Portal de Compras MG - dados abertos', "
    f"'frequencia_atualizacao'='manual', "
    f"'responsavel'='{RESPONSAVEL}', 'projeto'='uemg_compras'"
    f")"
)
print(f"✓ Governança aplicada em {TABELA_FULL}")

# COMMAND ----------

# DBTITLE 1,5. Validação e DQ
# ── 5. Validação final e gravação em dq_resultados ─────────────────

# Recarregar tabela para validação
df_obt_final = spark.table(TABELA_FULL)

linhas_obt = df_obt_final.count()
soma_obt = df_obt_final.agg(F.sum("vr_homologado").alias("soma")).collect()[0]["soma"]

# Verificações vs Silver
df_silver_uemg = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_compras_item").filter(F.col("fl_uemg"))
linhas_silver = df_silver_uemg.count()
soma_silver = df_silver_uemg.agg(F.sum("vr_homologado").alias("soma")).collect()[0]["soma"]

# sk_item_compra único
pk_duplicadas = df_obt_final.groupBy("sk_item_compra").count().filter("count > 1").count()

# % origem_dotacao
df_origem = df_obt_final.groupBy("origem_dotacao").agg(
    (F.count(F.lit(1)) / F.lit(linhas_obt) * 100).alias("pct"),
    F.count(F.lit(1)).alias("qtd"),
).orderBy("origem_dotacao")

# % benchmark válido
linhas_com_benchmark = df_obt_final.filter(F.col("vr_un_mediana_estado").isNotNull()).count()
pct_benchmark = round(100.0 * linhas_com_benchmark / linhas_obt, 2) if linhas_obt > 0 else 0.0

# % colunas com comentário
total_colunas = len(df_obt_final.columns)
cols_com_comentario = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {CATALOGO}.information_schema.columns "
    f"WHERE table_schema = '{SCHEMA_GOLD}' AND table_name = 'obt_compras_uemg' "
    f"  AND comment IS NOT NULL"
).collect()[0]["cnt"]
pct_comentarios = round(100.0 * cols_com_comentario / total_colunas, 2)

print("═" * 70)
print("VALIDAÇÃO DA OBT GOLD — obt_compras_uemg")
print("═" * 70)
print(f"linhas_obt = {linhas_obt} | linhas_silver = {linhas_silver} | iguais: {linhas_obt == linhas_silver}")
print(f"soma_vr_homologado_obt = R$ {soma_obt:,.2f} | soma_silver = R$ {soma_silver:,.2f} | iguais: {soma_obt == soma_silver}")
print(f"sk_item_compra único: {pk_duplicadas == 0} (duplicatas: {pk_duplicadas})")
print(f"% benchmark válido: {pct_benchmark}% ({linhas_com_benchmark}/{linhas_obt})")
print(f"% colunas com comentário: {pct_comentarios}% ({cols_com_comentario}/{total_colunas})")

print("\n── % linhas por origem_dotacao ──")
df_origem.display()

# ── Gravar em dq_resultados (append) ──
schema_dq = StructType([
    StructField("tabela", StringType(), True),
    StructField("linhas_bronze", LongType(), True),
    StructField("linhas_silver", LongType(), True),
    StructField("duplicatas_removidas", LongType(), True),
    StructField("pk_unica", BooleanType(), True),
    StructField("falhas_de_cast", LongType(), True),
    StructField("status", StringType(), True),
    StructField("notebook", StringType(), True),
    StructField("dt_execucao", StringType(), True),
])

resultados_dq = [{
    "tabela": TABELA_FULL,
    "linhas_bronze": linhas_silver,  # na Gold, a "bronze" é a Silver de origem
    "linhas_silver": linhas_obt,
    "duplicatas_removidas": 0,
    "pk_unica": pk_duplicadas == 0,
    "falhas_de_cast": 0,
    "status": "OK" if linhas_obt == linhas_silver and soma_obt == soma_silver and pk_duplicadas == 0 else "FALHA",
}]

df_dq = spark.createDataFrame(resultados_dq, schema_dq)
df_dq = df_dq.withColumn("notebook", F.lit(NOTEBOOK_NOME)).withColumn("dt_execucao", F.current_timestamp().cast("string"))

(
    df_dq.write.format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.dq_resultados")
)
print(f"\n✓ Resultados de DQ gravados em {CATALOGO}.{SCHEMA_SILVER}.dq_resultados")

# ── Conferências visuais ──
print("\n── Gasto por ano ──")
(
    df_obt_final
    .filter(~F.col("fl_ano_incompleto"))
    .groupBy("ano_homologacao")
    .agg(F.sum("vr_homologado").alias("vr_total"), F.count(F.lit(1)).alias("qtd_itens"))
    .orderBy("ano_homologacao")
).display()

print("\n── Top 10 fornecedores por vr_homologado ──")
(
    df_obt_final
    .groupBy("nome_fornecedor")
    .agg(F.sum("vr_homologado").alias("vr_total"), F.count(F.lit(1)).alias("qtd_itens"))
    .orderBy(F.desc("vr_total"))
    .limit(10)
).display()

print("\n── Gasto por natureza_despesa_item ──")
(
    df_obt_final
    .groupBy("natureza_despesa_item")
    .agg(F.sum("vr_homologado").alias("vr_total"), F.count(F.lit(1)).alias("qtd_itens"))
    .orderBy(F.desc("vr_total"))
    .limit(15)
).display()