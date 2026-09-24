# Databricks notebook source
# DBTITLE 1,Governança Bronze — Documentação e Tags
# MAGIC %md
# MAGIC # Governança Bronze — Documentação e Tags
# MAGIC
# MAGIC **Projeto:** uemg_compras – Portal de Compras do Estado de Minas Gerais
# MAGIC
# MAGIC **Objetivo:** aplicar comentários de tabela e coluna, tags de governança e TBLPROPERTIES
# MAGIC em todas as tabelas do schema `uemg_compras.bronze`.
# MAGIC
# MAGIC **Execução:** IDEMPOTENTE — deve ser rodado **após** a ingestão da Bronze,
# MAGIC pois o `overwrite` pode apagar comentários de coluna.
# MAGIC
# MAGIC **Fonte de dados:** dicionário de dados embutido no notebook (`DICIONARIO_DADOS`).

# COMMAND ----------

# DBTITLE 1,1. Dicionário de dados e constantes
# ── 1. Dicionário de dados e constantes de governança ────────────────
# Estrutura por tabela:
#   comentario_tabela  : texto descritivo
#   granularidade      : o que 1 linha representa
#   colunas            : {nome_coluna: comentário}
#   colunas_pii        : lista de colunas com dados pessoais

CATALOGO = "uemg_compras"
SCHEMA_BRONZE = "bronze"
RESPONSAVEL = "<responsavel>"

# Comentários fixos das colunas técnicas (presentes em todas as tabelas Bronze)
COLUNAS_TECNICAS_COMENTARIOS = {
    "_arquivo_origem": "Caminho do arquivo CSV de origem no Volume",
    "_dt_modificacao_arquivo": "Data de modificação do arquivo de origem",
    "_dt_ingestao": "Data/hora em que o registro foi carregado na Bronze",
}

DICIONARIO_DADOS = {
    "ft_compras": {
        "comentario_tabela": (
            "Fato de compras do Estado de MG: itens homologados em processos de compra, "
            "com valores de referência, homologado e atualizado. Dados de 2009 a 2025. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 item homologado de um processo de compra para um fornecedor",
        "colunas": {
            "id_tempo": "Chave da dimensão tempo",
            "id_procedimento": "Chave do procedimento/modalidade de compra (pregão, dispensa etc.)",
            "id_orgao_demanda": "Chave do órgão que demandou a compra (FK dm_orgao_demanda; UEMG = 1831030)",
            "id_orgao_contrato": "Chave do órgão responsável pelo contrato",
            "id_situacao_proc": "Chave da situação do processo de compra",
            "id_situacao_cont": "Chave da situação do contrato",
            "id_municipio": "Chave do município/localidade da compra (FK dm_municipio)",
            "id_contratado": "Chave do fornecedor contratado (FK dm_contratado)",
            "id_tipo_licitacao": "Chave do tipo de licitação (FK dm_tipo_licitacao)",
            "id_grupo_matserv": "Chave do grupo de material/serviço",
            "id_classe_matserv": "Chave da classe de material/serviço",
            "id_material_servico": "Chave do material/serviço genérico (FK dm_material_servico)",
            "id_item_matserv": "Chave do item de material/serviço detalhado (FK dm_item_matserv)",
            "id_processo": "Identificador do processo de compra",
            "id_contrato": "Identificador do contrato",
            "id_unidade_medida": "Chave da unidade de medida do item",
            "id_linha_fornec": "Identificador da linha de fornecimento",
            "id_item": "Identificador do item de despesa (também presente em fl_compras_empenho)",
            "id_unidade_orc": "Chave da unidade orçamentária",
            "ano_particao": "Ano de referência da compra",
            "dt_item_homologa": "Data de homologação do item (yyyy-MM-dd)",
            "qt_item_pedido": "Quantidade pedida do item",
            "vr_un_referencia": "Valor unitário de referência (estimado antes da licitação), em R$",
            "vr_referencia": "Valor total de referência (quantidade x valor unitário de referência), em R$",
            "vr_un_homologado": "Valor unitário homologado (após a licitação), em R$",
            "vr_homologado": "Valor total homologado, em R$",
            "vr_atualizado": "Valor total atualizado após aditivos/reajustes, em R$",
        },
        "colunas_pii": [],
    },
    "ft_compras_contrato": {
        "comentario_tabela": (
            "Fato de contratos do Estado de MG com valor homologado e atualizado. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 contrato",
        "colunas": {
            "id_tempo": "Chave da dimensão tempo",
            "id_processo": "Identificador do processo de compra",
            "id_orgao_contrato": "Chave do órgão responsável pelo contrato",
            "id_contrato": "Identificador do contrato",
            "id_contratado": "Chave do fornecedor contratado (FK dm_contratado)",
            "id_situacao_cont": "Chave da situação do contrato",
            "ano_particao": "Ano de referência da compra",
            "vr_homologado": "Valor homologado do contrato, em R$",
            "vr_atualizado": "Valor do contrato atualizado após aditivos, em R$",
        },
        "colunas_pii": [],
    },
    "fl_compras_empenho": {
        "comentario_tabela": (
            "Tabela ponte entre processos de compra e empenhos orçamentários. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": (
            "1 linha = 1 empenho vinculado a um processo/item. "
            "ATENÇÃO: várias linhas por processo — agregar antes de juntar com ft_compras "
            "para evitar duplicação de valores."
        ),
        "colunas": {
            "id_processo": "Identificador do processo de compra (FK ft_compras)",
            "id_empenho": "Identificador do empenho",
            "id_programa": "Chave do programa orçamentário",
            "id_acao": "Chave da ação orçamentária",
            "id_elemento": "Chave do elemento de despesa",
            "id_item": "Identificador do item de despesa",
            "dotacao_orcamentaria": (
                "Dotação completa no formato "
                "\"unidade_orc funcao.subfuncao.programa.acao.x categoria.grupo."
                "modalidade.elemento.item fonte\" "
                "(ex.: 2350 12.364.021.4065.1 4.4.90.52.14 0.10.8)"
            ),
        },
        "colunas_pii": [],
    },
    "dm_orgao_demanda": {
        "comentario_tabela": (
            "Dimensão de órgãos do Estado de MG que demandam compras. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 órgão",
        "colunas": {
            "id_orgao_demanda": "Chave substituta do órgão",
            "cd_orgao_demanda": "Código oficial do órgão (UEMG = 2350; negativos = registros especiais)",
            "nome": "Nome do órgão",
        },
        "colunas_pii": [],
    },
    "dm_municipio": {
        "comentario_tabela": (
            "Dimensão de localidades: municípios de MG, de outras UFs, territórios "
            "e marcadores (ex.: CONFORME EDITAL, MINAS GERAIS). "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 localidade",
        "colunas": {
            "id_municipio": "Chave substituta",
            "cd_municipio_ibge": "Código IBGE (7 dígitos = município; outros valores = região/marcador)",
            "nome": "Nome da localidade",
        },
        "colunas_pii": [],
    },
    "dm_material_servico": {
        "comentario_tabela": (
            "Dimensão de materiais e serviços (descrição genérica). "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 material/serviço genérico",
        "colunas": {
            "id_material_servico": "Chave substituta",
            "cd_material_servico": "Código do material/serviço no catálogo do Estado",
            "nome": "Descrição genérica",
        },
        "colunas_pii": [],
    },
    "dm_item_matserv": {
        "comentario_tabela": (
            "Dimensão de itens de material/serviço (descrição detalhada com especificação). "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 item de material/serviço detalhado",
        "colunas": {
            "id_item_matserv": "Chave substituta",
            "cd_item_matserv": "Código do item no catálogo do Estado",
            "nome": "Descrição detalhada/especificação técnica do item",
            "natureza_despesa": "Classificação (ex.: MATERIAL CONSUMO, MATERIAL PERMANENTE)",
        },
        "colunas_pii": [],
    },
    "dm_tipo_licitacao": {
        "comentario_tabela": (
            "Dimensão de tipos de licitação. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 tipo de licitação",
        "colunas": {
            "id_tipo_licitacao": "Chave substituta",
            "cd_tipo_licitacao": "Código (1 menor preço, 2 melhor técnica, 3 técnica e preço; negativos = especiais)",
            "nome": "Descrição",
        },
        "colunas_pii": [],
    },
    "dm_contratado": {
        "comentario_tabela": (
            "Dimensão de fornecedores contratados (pessoa jurídica e física). "
            "Contém dados pessoais anonimizados — uso sujeito à LGPD. "
            "Todas as colunas como STRING (camada Bronze)."
        ),
        "granularidade": "1 linha = 1 fornecedor",
        "colunas": {
            "id_contratado": "Chave substituta do fornecedor",
            "tp_documento": "Tipo de documento (2 = CNPJ, 1 = CPF, 0 = sem documento)",
            "nr_documento_anonimizado": "CNPJ (pode estar sem zeros à esquerda) ou CPF mascarado",
            "nome_anonimizado": "Razão social ou nome do fornecedor",
        },
        "colunas_pii": ["nr_documento_anonimizado", "nome_anonimizado"],
    },
}

print(f"Dicionário carregado: {len(DICIONARIO_DADOS)} tabelas documentadas.")

# COMMAND ----------

# DBTITLE 1,2. Função aplicar_governanca
# ── 2. Função aplicar_governanca(tabela, meta) ───────────────────────
# Aplica comentários de tabela/coluna, tags e TBLPROPERTIES de forma idempotente.


def _esc(texto):
    """Escapa aspas simples para uso em SQL string literals."""
    return texto.replace("'", "''")


def aplicar_governanca(tabela, meta):
    """
    Aplica governança a uma tabela Bronze.

    Parâmetros:
        tabela (str): nome curto da tabela (ex.: "ft_compras")
        meta  (dict): entrada de DICIONARIO_DADOS para essa tabela

    Retorno:
        dict com tabela, status e detalhes das operações aplicadas.
    """
    tabela_full = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    operacoes = []

    # (a) COMMENT ON TABLE — comentário + granularidade + origem
    comentario_completo = (
        f"{meta['comentario_tabela']} "
        f"| Granularidade: {meta['granularidade']} "
        f"| Origem: Portal de Compras MG - dados abertos"
    )
    spark.sql(f"COMMENT ON TABLE {tabela_full} IS '{_esc(comentario_completo)}'")
    operacoes.append("comment_on_table")

    # (b) ALTER COLUMN COMMENT para cada coluna do dicionário
    for col_name, col_comment in meta["colunas"].items():
        spark.sql(
            f"ALTER TABLE {tabela_full} "
            f"ALTER COLUMN {col_name} COMMENT '{_esc(col_comment)}'"
        )
    operacoes.append(f"comment_colunas:{len(meta['colunas'])}")

    # (f) Comentários fixos das colunas técnicas
    for col_name, col_comment in COLUNAS_TECNICAS_COMENTARIOS.items():
        spark.sql(
            f"ALTER TABLE {tabela_full} "
            f"ALTER COLUMN {col_name} COMMENT '{_esc(col_comment)}'"
        )
    operacoes.append("comment_colunas_tecnicas:3")

    # (c) SET TAGS na tabela
    tipo = "fato" if tabela.startswith(("ft_", "fl_")) else "dimensao"
    spark.sql(
        f"ALTER TABLE {tabela_full} "
        f"SET TAGS ("
        f"'camada'='bronze', "
        f"'fonte'='portal_compras_mg', "
        f"'dominio'='compras_publicas', "
        f"'tipo'='{tipo}'"
        f")"
    )
    operacoes.append(f"set_tags:tipo={tipo}")

    # (d) SET TAGS nas colunas PII
    for col_pii in meta.get("colunas_pii", []):
        spark.sql(
            f"ALTER TABLE {tabela_full} "
            f"ALTER COLUMN {col_pii} "
            f"SET TAGS ('pii'='true', 'lgpd'='dado_pessoal_anonimizado')"
        )
    if meta.get("colunas_pii"):
        operacoes.append(f"set_tags_pii:{len(meta['colunas_pii'])}")

    # (e) SET TBLPROPERTIES
    spark.sql(
        f"ALTER TABLE {tabela_full} "
        f"SET TBLPROPERTIES ("
        f"'fonte_dados'='Portal de Compras MG - dados abertos', "
        f"'frequencia_atualizacao'='manual', "
        f"'responsavel'='{RESPONSAVEL}', "
        f"'projeto'='uemg_compras'"
        f")"
    )
    operacoes.append("tblproperties")

    print(f"✓ {tabela_full}: {' | '.join(operacoes)}")
    return {"tabela": tabela_full, "status": "OK", "operacoes": operacoes}

# COMMAND ----------

# DBTITLE 1,3. Loop sobre as tabelas
# ── 3. Loop sobre as tabelas do dicionário (try/except por tabela) ─

resultados_governanca = []

for tabela, meta in DICIONARIO_DADOS.items():
    try:
        resultado = aplicar_governanca(tabela, meta)
        resultados_governanca.append(resultado)
    except Exception as e:
        tabela_full = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
        print(f"✗ ERRO em {tabela_full}: {e}")
        resultados_governanca.append({
            "tabela": tabela_full,
            "status": f"ERRO: {e}",
            "operacoes": [],
        })

print(f"\nGovernança aplicada em {len(resultados_governanca)} tabela(s).")

# COMMAND ----------

# DBTITLE 1,4. Relatório de cobertura de governança
# ── 4. Relatório de cobertura de governança ────────────────────────
# Consulta information_schema para MEDIR a cobertura real (não confiar apenas
# no que o código tentou aplicar). Constrói linhas como dicionário com StructType explícito.

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
)
from collections import defaultdict

# Tabelas documentadas no dicionário
tabelas_dict = set(DICIONARIO_DADOS.keys())

# 4a. information_schema.columns — colunas e comentários
df_isc = spark.sql(
    f"SELECT table_name, column_name, comment "
    f"FROM {CATALOGO}.information_schema.columns "
    f"WHERE table_schema = '{SCHEMA_BRONZE}'"
)
col_data = df_isc.collect()

# 4b. information_schema.tables — comentário da tabela
df_ist = spark.sql(
    f"SELECT table_name, comment AS table_comment "
    f"FROM {CATALOGO}.information_schema.tables "
    f"WHERE table_schema = '{SCHEMA_BRONZE}'"
)
tab_data = df_ist.collect()

# 4c. information_schema.table_tags — tags aplicadas
df_itg = spark.sql(
    f"SELECT table_name, tag_name, tag_value "
    f"FROM {CATALOGO}.information_schema.table_tags "
    f"WHERE schema_name = '{SCHEMA_BRONZE}'"
)
tag_data = df_itg.collect()

# Organizar dados coletados por tabela
cols_por_tabela = defaultdict(list)
for row in col_data:
    if row["table_name"] in tabelas_dict:
        cols_por_tabela[row["table_name"]].append({
            "column_name": row["column_name"],
            "comment": row["comment"],
        })

tab_comments = {
    row["table_name"]: row["table_comment"]
    for row in tab_data
    if row["table_name"] in tabelas_dict
}

tags_por_tabela = defaultdict(list)
for row in tag_data:
    if row["table_name"] in tabelas_dict:
        tags_por_tabela[row["table_name"]].append(row["tag_name"])

# Schema explícito do relatório principal
schema_relatorio = StructType([
    StructField("tabela", StringType(), True),
    StructField("tem_comentario_tabela", BooleanType(), True),
    StructField("qtd_colunas", IntegerType(), True),
    StructField("qtd_colunas_comentadas", IntegerType(), True),
    StructField("pct_cobertura", DoubleType(), True),
    StructField("qtd_tags", IntegerType(), True),
    StructField("status", StringType(), True),
])

# Schema para colunas sem documentação
schema_sem_doc = StructType([
    StructField("tabela", StringType(), True),
    StructField("coluna_sem_documentacao", StringType(), True),
])

linhas_relatorio = []
linhas_sem_doc = []

for tabela in sorted(cols_por_tabela.keys()):
    tabela_full = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    cols = cols_por_tabela[tabela]
    qtd_total = len(cols)
    qtd_comentadas = sum(1 for c in cols if c["comment"] is not None)
    pct = round(100.0 * qtd_comentadas / qtd_total, 1) if qtd_total > 0 else 0.0
    tem_comment_tab = tab_comments.get(tabela) is not None
    qtd_tags = len(tags_por_tabela.get(tabela, []))

    # Verificar colunas sem documentação no dicionário
    colunas_esperadas = set(DICIONARIO_DADOS.get(tabela, {}).get("colunas", {}).keys())
    colunas_esperadas.update(COLUNAS_TECNICAS_COMENTARIOS.keys())

    for c in cols:
        if c["column_name"] not in colunas_esperadas:
            linhas_sem_doc.append({
                "tabela": tabela_full,
                "coluna_sem_documentacao": c["column_name"],
            })

    status = "OK" if (qtd_comentadas == qtd_total and tem_comment_tab) else "COBERTURA_INCOMPLETA"

    linhas_relatorio.append({
        "tabela": tabela_full,
        "tem_comentario_tabela": tem_comment_tab,
        "qtd_colunas": qtd_total,
        "qtd_colunas_comentadas": qtd_comentadas,
        "pct_cobertura": pct,
        "qtd_tags": qtd_tags,
        "status": status,
    })

# Criar DataFrames com schema explícito
df_relatorio = spark.createDataFrame(linhas_relatorio, schema_relatorio).orderBy("tabela")

df_sem_doc = (
    spark.createDataFrame(linhas_sem_doc, schema_sem_doc)
    if linhas_sem_doc
    else spark.createDataFrame([], schema_sem_doc)
)

print("═" * 70)
print("RELATÓRIO DE COBERTURA DE GOVERNANÇA")
print("═" * 70)
df_relatorio.display()

if linhas_sem_doc:
    print()
    print("═" * 70)
    print("COLUNAS SEM DOCUMENTAÇÃO NO DICIONÁRIO")
    print("═" * 70)
    df_sem_doc.display()
else:
    print("\n✓ Todas as colunas estão documentadas no dicionário.")

# Resumo final
qtd_ok = sum(1 for l in linhas_relatorio if l["status"] == "OK")
qtd_parcial = sum(1 for l in linhas_relatorio if l["status"] != "OK")
qtd_sem_doc = len(linhas_sem_doc)

print(f"\n{'═' * 70}")
print(f"Resumo: {qtd_ok} tabela(s) com cobertura total, "
      f"{qtd_parcial} com cobertura incompleta, "
      f"{qtd_sem_doc} coluna(s) sem documentação")
print(f"{'═' * 70}")