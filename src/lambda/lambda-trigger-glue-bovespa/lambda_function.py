import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_event(event: dict) -> tuple:
    """
    Valida se todos os parâmetros obrigatórios foram fornecidos corretamente.
    """
    required_fields = ["job_name", "job_parameters"]
    missing = [field for field in required_fields if field not in event or not event[field]]
    if missing:
        logger.error(f"Parâmetros obrigatórios ausentes: {missing}")
        return False, f"Parâmetros obrigatórios ausentes: {missing}"

    if not isinstance(event["job_parameters"], dict) or not event["job_parameters"]:
        logger.error("O campo 'job_parameters' deve ser um dicionário não vazio.")
        return False, "O campo 'job_parameters' deve ser um dicionário não vazio."

    return True, ""

def start_glue_job(job_name: str, job_parameters: dict) -> dict:
    """
    Inicia um job do AWS Glue com os parâmetros fornecidos.
    """
    glue_client = boto3.client('glue')
    try:
        logger.info(f"Iniciando Glue Job: {job_name} com parâmetros: {job_parameters}")
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_parameters
        )
        logger.info(f"Glue Job iniciado com sucesso: {response.get('JobRunId')}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Glue Job iniciado com sucesso.",
                "JobRunId": response.get("JobRunId")
            })
        }
    except Exception as e:
        logger.error(f"Erro ao iniciar Glue Job: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Erro ao iniciar Glue Job: {str(e)}")
        }

def lambda_handler(event, context):
    """
    Handler principal da Lambda.
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    is_valid, msg = validate_event(event)
    if not is_valid:
        return {
            "statusCode": 400,
            "body": json.dumps(msg)
        }

    job_name = event["job_name"]
    job_parameters = event["job_parameters"]
    return start_glue_job(job_name, job_parameters)