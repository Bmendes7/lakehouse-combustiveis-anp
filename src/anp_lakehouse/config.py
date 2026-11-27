CATALOGO = "workspace"
SCHEMA = "anp"
VOLUME_RAW = f"/Volumes/{CATALOGO}/{SCHEMA}/raw"
PASTA_ZIP = f"{VOLUME_RAW}/zip"
PASTA_CSV = f"{VOLUME_RAW}/csv"
ENCODING_CSV = "UTF-8"

def tabela(nome:str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"

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

