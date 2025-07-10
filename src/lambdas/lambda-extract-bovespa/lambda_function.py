import requests
import base64
import json
import urllib
import pandas as pd
import awswrangler as wr

def decode_api_params(api_token:str) -> dict:
    decoded_bytes = base64.b64decode(api_token)
    return json.loads(urllib.parse.unquote(decoded_bytes.decode('utf-8')))

def enconde_json_to_api(params_api: dict) -> str:
    encoded_json_str = json.dumps(params_api, separators=(',', ':'))
    return base64.b64encode(encoded_json_str.encode('utf-8')).decode('utf-8')

def get_portfolio_day(params_api: dict) -> dict:
    b64 = enconde_json_to_api(params_api)
    url = requests.get(
            f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{b64}",
            timeout=60,
            headers={"Content-Type": "application/json","Access-Control-Allow-Origin": "*"}
        )
    url.raise_for_status()
    return url.json()

def portfolio_day_to_df(json_data: dict) -> pd.DataFrame:
    page_info = json_data['page']
    header_info = json_data['header']
    rows = []
    for result_item in json_data['results']:
        row = {
            # Campos de 'page'
            'page_pageNumber': page_info['pageNumber'],
            'page_pageSize': page_info['pageSize'],
            'page_totalRecords': page_info['totalRecords'],
            'page_totalPages': page_info['totalPages'],
            # Campos de 'header'
            'header_date': header_info['date'],
            'header_text': header_info['text'],
            'header_part': header_info['part'],
            'header_partAcum': header_info['partAcum'],
            'header_textReductor': header_info['textReductor'],
            'header_reductor': header_info['reductor'],
            'header_theoricalQty': header_info['theoricalQty'],
            # Campos de 'results' (do item atual)
            'results_segment': result_item['segment'],
            'results_cod': result_item['cod'],
            'results_asset': result_item['asset'],
            'results_type': result_item['type'],
            'results_part': result_item['part'],
            'results_partAcum': result_item['partAcum'],
            'results_theoricalQty': result_item['theoricalQty']
        }
        rows.append(row)
    return pd.DataFrame(rows)

def lambda_handler(event, context):
    s3_path ="s3://fiap-ml-tc-fase2-data/raw-zone/tbl_raw_bovespa/"
    params_api = {
        'language': 'pt-br',
        'pageNumber': 1,
        'pageSize': 120,
        'index': 'IBOV',
        'segment': '2' # segmento 1 (consulta por codigo) ou 2 (consulta por setor de atuacao)
    }
    try:
        json_data = get_portfolio_day(params_api)
        df = portfolio_day_to_df(json_data)
        wr.s3.to_parquet(df=df,path=s3_path, dataset=True, mode='overwrite_partitions', partition_cols=['header_date'])
        return {'statusCode': 200,'body': json.dumps('Scrap B3 realizado com sucesso!')}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f'Erro ao realizar o scrap B3: {str(e)}')}
    finally:
        print("Scrap B3 finalizado.")
