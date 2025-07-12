import os
import logging
import requests
import base64
import json
import urllib
import pandas as pd
import awswrangler as wr
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

# Logging estruturado
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def decode_api_params(api_token:str) -> dict:
    decoded_bytes = base64.b64decode(api_token)
    return json.loads(urllib.parse.unquote(decoded_bytes.decode('utf-8')))

def encode_json_to_api(params_api: dict) -> str:
    """Codifica parâmetros da API em base64."""
    encoded_json_str = json.dumps(params_api, separators=(',', ':'))
    return base64.b64encode(encoded_json_str.encode('utf-8')).decode('utf-8')

def build_b3_url(api_conf: dict) -> str:
    """
    Monta e valida a URL da API da B3, garantindo que não haja barras duplas ou ausências de barras.
    """
    host = api_conf["host"].rstrip('/')
    route = api_conf["route"]
    if not route.startswith('/'):
        route = '/' + route
    if not route.endswith('/'):
        route += '/'

    # Remove barras duplicadas ao juntar host e route
    url_base = "https://" + f"{host}{route}".replace('//', '/')
    b64_params = encode_json_to_api(api_conf["parameters"])
    url = f"{url_base}{b64_params}"

    # Validação final da URL
    if '//' in url[8:]:  # ignora o 'https://'
        logger.error(f"URL malformada detectada: {url}")
        raise ValueError(f"URL malformada detectada: {url}")
    return url

def get_portfolio_day(api_conf: dict, session: requests.Session) -> dict:
    """Consulta a API da B3 e retorna o JSON, usando a função de montagem e validação da URL."""
    try:
        url = build_b3_url(api_conf)
        response = session.get(
            url,
            timeout=api_conf.get("timeout", 60),
            headers=api_conf.get("headers", {"Content-Type": "application/json"})
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Erro na requisição HTTP: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao montar a URL da API: {e}")

def portfolio_day_to_df(json_data: dict) -> pd.DataFrame:
    """Converte o JSON da B3 em DataFrame."""
    try:
        page_info = json_data['page']
        header_info = json_data['header']
        rows = []
        for result_item in json_data.get('results', []):
            date_partition = datetime.strptime(header_info['date'], '%d/%m/%y')
            row = {
                'page_pageNumber': page_info.get('pageNumber'),
                'page_pageSize': page_info.get('pageSize'),
                'page_totalRecords': page_info.get('totalRecords'),
                'page_totalPages': page_info.get('totalPages'),
                'header_date': header_info.get('date'),
                'header_text': header_info.get('text'),
                'header_part': header_info.get('part'),
                'header_partAcum': header_info.get('partAcum'),
                'header_textReductor': header_info.get('textReductor'),
                'header_reductor': header_info.get('reductor'),
                'header_theoricalQty': header_info.get('theoricalQty'),
                'results_segment': result_item.get('segment'),
                'results_cod': result_item.get('cod'),
                'results_asset': result_item.get('asset'),
                'results_type': result_item.get('type'),
                'results_part': result_item.get('part'),
                'results_partAcum': result_item.get('partAcum'),
                'results_theoricalQty': result_item.get('theoricalQty'),
                "year": date_partition.year,
                "month": date_partition.month,
                "day": date_partition.day
            }
            rows.append(row)
        if not rows:
            logger.warning("Nenhum dado encontrado em 'results'.")
        return pd.DataFrame(rows)
    except KeyError as e:
        logger.error(f"JSON está faltando a Key: {e}")
        raise ValueError(f"JSON está faltando a Key: {e}")

def validate_event(event: dict) -> tuple:
    """Valida se todos os parâmetros obrigatórios foram fornecidos corretamente."""
    required_fields = ["s3_bucket", "s3_prefix", "api"]
    missing = [field for field in required_fields if field not in event or not event[field]]
    if missing:
        logger.error(f"Parâmetros obrigatórios ausentes: {missing}")
        return False, f"Parâmetros obrigatórios ausentes: {missing}"

    api_conf = event["api"]
    api_required = ["host", "route", "parameters"]
    api_missing = [field for field in api_required if field not in api_conf or not api_conf[field]]
    if api_missing:
        logger.error(f"Parâmetros obrigatórios ausentes em 'api': {api_missing}")
        return False, f"Parâmetros obrigatórios ausentes em 'api': {api_missing}"

    params_required = ["language", "pageNumber", "pageSize", "index", "segment"]
    params = api_conf["parameters"]
    params_missing = [field for field in params_required if field not in params or params[field] in [None, ""]]
    if params_missing:
        logger.error(f"Parâmetros obrigatórios ausentes em 'api.parameters': {params_missing}")
        return False, f"Parâmetros obrigatórios ausentes em 'api.parameters': {params_missing}"

    return True, ""

def lambda_handler(event, context):
    # Validação dos parâmetros
    is_valid, msg = validate_event(event)
    if not is_valid:
        return {'statusCode': 400, 'body': json.dumps(msg)}

    s3_bucket = event["s3_bucket"]
    s3_prefix = event["s3_prefix"]
    api_conf = event["api"]

    # Garante que o prefixo termina com '/'
    if s3_prefix and not s3_prefix.endswith('/'):
        s3_prefix += '/'

    s3_path = f"s3://{s3_bucket}/{s3_prefix}"

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        logger.info("Iniciando scrap B3")
        json_data = get_portfolio_day(api_conf, session)
        df = portfolio_day_to_df(json_data)
        if df.empty:
            logger.warning("DataFrame retornado está vazio.")
            return {'statusCode': 204, 'body': json.dumps('Nenhum dado encontrado.')}
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            mode='overwrite_partitions',
            partition_cols=["year", "month", "day"],
            compression="snappy"
        )
        logger.info("Scrap B3 realizado com sucesso!")
        return {'statusCode': 200, 'body': json.dumps('Scrap B3 realizado com sucesso!')}
    except Exception as e:
        logger.error(f"Erro ao realizar o scrap B3: {str(e)}", exc_info=True)
        return {'statusCode': 500, 'body': json.dumps(f'Erro ao realizar o scrap B3: {str(e)}')}