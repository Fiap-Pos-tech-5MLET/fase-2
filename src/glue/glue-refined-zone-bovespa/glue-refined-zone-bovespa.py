import awswrangler as wr
import boto3
import sys
import logging
from typing import Dict, List
from pyspark.sql import DataFrame, SparkSession, functions as sf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from botocore.exceptions import ClientError

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_PARAMS: List[str] = [
    'JOB_NAME',
    'TABLE_NAME',
    'DATABASE_NAME',
    'S3_BUCKET',
    'OBJECT_KEY',
    'S3_OUTPUT_BUCKET',
    'S3_OUTPUT_PREFIX',
    'AWS_REGION'
]

EXPECTED_COLUMNS: List[str] = [
    "results_segment", "results_asset", "results_cod", "results_type",
    "results_theoricalQty", "results_part", "results_partAcum", "header_date"
]

def validate_params(args: Dict[str, str]) -> None:
    """
    Valida se todos os parâmetros obrigatórios foram fornecidos.
    """
    missing = [param for param in REQUIRED_PARAMS if param not in args or not args[param]]
    if missing:
        logger.error(f"Parâmetros obrigatórios ausentes: {missing}")
        raise ValueError(f"Parâmetros obrigatórios ausentes: {missing}")

def validate_schema(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Valida se o DataFrame possui todas as colunas esperadas.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        logger.error(f"Colunas esperadas ausentes: {missing}")
        raise ValueError(f"Colunas esperadas ausentes: {missing}")

def read_data(spark: SparkSession, s3_bucket: str, object_key: str) -> DataFrame:
    """
    Lê os dados do S3 no formato Parquet.
    """
    logger.info(f"Lendo dados do bucket S3: {s3_bucket} com prefixo: {object_key}")
    try:
        df = spark.read.parquet(f"s3://{s3_bucket}/{object_key}")
        logger.info(f"Dados lidos com sucesso. Número de registros: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler dados do S3: {e}")
        raise

def drop_and_log_nulls(df: DataFrame) -> DataFrame:
    """
    Remove linhas com valores nulos e loga a quantidade de linhas removidas.
    """
    qtd_total = df.count()
    df_clean = df.na.drop()
    qtd_rows_unic = df_clean.count()
    logger.info(f"Linhas removidas por valores nulos: {qtd_total - qtd_rows_unic}")
    return df_clean

def process_data(df: DataFrame) -> DataFrame:
    """
    Realiza o processamento e limpeza dos dados.
    """
    logger.info("Processando dados...")
    try:
        validate_schema(df, EXPECTED_COLUMNS)
        logger.info("Validação de esquema concluída com sucesso.")
        df = df.drop(
                "page_pageNumber", "page_pageSize", "page_totalRecords", "page_totalPages",
                "header_text", "header_part", "header_partAcum", "header_textReductor",
                "header_reductor", "header_theoricalQty")
        df.cache()
        logger.info("Cache aplicado aos dados.")
        df = drop_and_log_nulls(df)
        df = (
            df.withColumnsRenamed({
                "results_segment": "nom_setor",
                "results_asset": "nom_empresa",
                "results_cod": "cod_acao",
                "results_type": "des_tipo_acao",
                "results_theoricalQty": "qtd_teorica",
                "results_part": "perc_participacao_setor",
                "results_partAcum": "perc_participacao_setor_acumulada",
                "header_date": "data_ref"
            })
            .withColumn("nom_setor", sf.trim("nom_setor"))
            .withColumn("nom_empresa", sf.trim("nom_empresa"))
            .withColumn("cod_acao", sf.trim("cod_acao"))
            .withColumn("des_tipo_acao", sf.trim("des_tipo_acao"))
            .withColumn("perc_participacao_setor", sf.regexp_replace("perc_participacao_setor", ",", ".").cast("decimal(5,3)"))
            .withColumn("perc_participacao_setor_acumulada", sf.regexp_replace("perc_participacao_setor_acumulada", ",", ".").cast("decimal(5,3)"))
            .withColumn("qtd_teorica", sf.regexp_replace("qtd_teorica", "[. ]", "").cast("bigint"))
            .withColumn("data_ref", sf.to_date("data_ref", "dd/MM/yy"))
            .withColumn("year", sf.year("data_ref"))
            .withColumn("month", sf.month("data_ref"))
            .withColumn("day", sf.day("data_ref"))
        )
        logger.info(f"Processamento de dados concluído. Registros finais: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        raise
    
def aggregate_data(df: DataFrame) -> DataFrame:
    """
    Realiza a agregação dos dados por setor, empresa e data de referência.
    """
    try:
        df = (
            df.groupBy("nom_setor","nom_empresa","data_ref","year","month","day") \
            .agg(
                sf.count(sf.col("cod_acao")).alias("qtd_registros"),
                sf.count_distinct(sf.col("cod_acao")).alias("qtd_acao"),
                sf.count_distinct(sf.col("des_tipo_acao")).alias("qtd_tipos_acao"),
                sf.sum(sf.col("qtd_teorica")).alias("qtd_teorica_acumulada"),
                sf.max(sf.col("qtd_teorica")).alias("qtd_teorica_max"),
                sf.min(sf.col("qtd_teorica")).alias("qtd_teorica_min"),
                sf.avg(sf.col("perc_participacao_setor")).alias("avg_participacao_setor_total"),
                sf.avg(sf.col("perc_participacao_setor_acumulada")).alias("avg_participacao_setor_acumulada_total")
            ) \
            .withColumn("dth_etl_processamento", sf.current_timestamp()) \
            .withColumn("qtd_dias_atraso", sf.date_diff("data_ref","dth_etl_processamento")) \
            .select(
                "nom_empresa","qtd_registros","qtd_acao","qtd_tipos_acao","qtd_teorica_acumulada","qtd_teorica_max",
                "qtd_teorica_min","avg_participacao_setor_total","avg_participacao_setor_acumulada_total","qtd_dias_atraso",
                "data_ref","dth_etl_processamento","year","month","day","nom_setor"
            )
        )
        logger.info(f"Agregação de dados concluída. Registros agregados: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao agregar dados: {e}")
        raise

def write_data(df: DataFrame,s3_output_bucket: str,s3_output_prefix: str,partition_keys: List[str] = ["year", "month", "day"],compression: str = "snappy") -> None:
    """
    Escreve os dados processados no S3 particionados e cataloga no Glue.
    """
    try:
        df.write \
            .mode("overwrite") \
            .option("compression", compression) \
            .option("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .partitionBy(*partition_keys) \
            .format("parquet") \
            .parquet(f"s3://{s3_output_bucket}/{s3_output_prefix}")
        logger.info(f"Dados gravados com sucesso em s3://{s3_output_bucket}/{s3_output_prefix}")
    except Exception as e:
        logger.error(f"Erro ao gravar dados no S3: {e}")
        raise

# Catalogação automática usando boto3
def create_table_if_not_exists(database_name:str,table_name:str,s3_output_bucket: str,s3_output_prefix: str,aws_region:str) -> None:
    glue = boto3.client('glue', region_name=aws_region)
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        logger.info(f"Tabela {table_name} já existe no catálogo {database_name}. Nenhuma ação necessária.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Tabela {table_name} não existe. Criando tabela no catálogo Glue...")
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': [
                            {'Name': 'nom_empresa', 'Type': 'string'},
                            {'Name': 'qtd_registros', 'Type': 'bigint'},
                            {'Name': 'qtd_acao', 'Type': 'bigint'},
                            {'Name': 'qtd_tipos_acao', 'Type': 'bigint'},
                            {'Name': 'qtd_teorica_acumulada', 'Type': 'bigint'},
                            {'Name': 'qtd_teorica_max', 'Type': 'bigint'},
                            {'Name': 'qtd_teorica_min', 'Type': 'bigint'},
                            {'Name': 'qtd_dias_atraso', 'Type': 'int'},
                            {'Name': 'avg_participacao_setor_total', 'Type': 'decimal(9,7)'},
                            {'Name': 'avg_participacao_setor_acumulada_total', 'Type': 'decimal(9,7)'},
                            {'Name': 'data_ref', 'Type': 'date'},
                            {'Name': 'dth_etl_processamento', 'Type': 'timestamp'},
                        ],
                        'Location': f"s3://{s3_output_bucket}/{s3_output_prefix}",
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {'serialization.format': '1'}
                        },
                        'Compressed': True,
                        'StoredAsSubDirectories': False
                    },
                    'PartitionKeys': [
                            {'Name': 'year', 'Type': 'int'},
                            {'Name': 'month', 'Type': 'int'},
                            {'Name': 'day', 'Type': 'int'},
                            {'Name': 'nom_setor', 'Type': 'string'}
                        ],
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet'
                    }
                }
            )
            logger.info(f"Tabela {table_name} criada com sucesso no catálogo {database_name}.")
        else:
            logger.error(f"Erro ao acessar o catálogo Glue: {e}")
            raise

def msck_repair_table(database_name: str, table_name: str) -> None:
    """
    Executa o comando MSCK REPAIR TABLE via Athena usando awswrangler.
    """
    try:
        logger.info(f"Executando MSCK REPAIR TABLE {database_name}.{table_name} via Athena...")
        wr.athena.start_query_execution(
            sql=f"MSCK REPAIR TABLE {database_name}.{table_name}",
            database=database_name,
            workgroup="primary"
        )
        logger.info("MSCK REPAIR TABLE executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar MSCK REPAIR TABLE: {e}")
        raise

def main() -> None:
    """
    Função principal do Glue Job.
    """
    args = getResolvedOptions(sys.argv, REQUIRED_PARAMS)
    validate_params(args)

    logger.info(f"Iniciando o job Glue: {args['JOB_NAME']}")
    logger.info(f"Argumentos do job Glue: {args}")

    with SparkContext() as sc:
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        df = read_data(spark=spark, s3_bucket=args['S3_BUCKET'], object_key=args['OBJECT_KEY'])
        processed_df = process_data(df=df)
        agg_df = aggregate_data(df=processed_df)
        write_data(
                df= agg_df,
                s3_output_bucket=args['S3_OUTPUT_BUCKET'],
                s3_output_prefix= args['S3_OUTPUT_PREFIX'],
                partition_keys=["year", "month", "day", "nom_setor"]
        )

        create_table_if_not_exists(
                database_name=args['DATABASE_NAME'],
                table_name=args['TABLE_NAME'],
                s3_output_bucket=args['S3_OUTPUT_BUCKET'],
                s3_output_prefix= args['S3_OUTPUT_PREFIX'],
                aws_region=args['AWS_REGION']
        )

        msck_repair_table(database_name=args['DATABASE_NAME'],table_name=args['TABLE_NAME'])
        job.commit()
        logger.info("Job Glue finalizado com sucesso.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Falha na execução do job: {e}")
        raise