import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# Define o schema fixo para manter consistência entre os arquivos
SCHEMA = pa.schema([
    ("message_id", pa.int64()),
    ("user_id", pa.int64()),
    ("user_is_bot", pa.bool_()),
    ("user_first_name", pa.string()),
    ("chat_id", pa.int64()),
    ("chat_type", pa.string()),
    ("date", pa.int64()),
    ("text", pa.string()),  # Garante que text seja sempre string (pode ser None)
])


def lambda_handler(event: dict, context: dict) -> bool:
    '''
    Compacta arquivos JSON diários em um único arquivo Parquet, no bucket enriquecido.
    '''

    # renomear AWS_S3_BUCKET e AWS_S3_ENRICHED

    RAW_BUCKET = os.environ['AWS_S3_BUCKET']
    ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']

    # p/ testar a função, substituir date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d') por date = (datetime.now(tzinfo) - timedelta(days=0)).strftime('%Y-%m-%d')

    tzinfo = timezone(offset=timedelta(hours=-3))
    date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d')
    timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')

    table = None
    client = boto3.client('s3')

    try:
        response = client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f'telegram/context_date={date}')

        if 'Contents' not in response:
            logging.warning(f"Nenhum arquivo encontrado para a data {date}.")
            return False

        for content in response['Contents']:
            key = content['Key']
            local_file = f"/tmp/{key.split('/')[-1]}"
            client.download_file(RAW_BUCKET, key, local_file)

            with open(local_file, mode='r', encoding='utf8') as fp:
                data = json.load(fp)
                data = data.get("message", {})

            parsed_data = parse_data(data=data)
            iter_table = pa.Table.from_pydict(mapping=parsed_data, schema=SCHEMA)

            if table:
                table = pa.concat_tables([table, iter_table])
            else:
                table = iter_table

        parquet_path = f"/tmp/{timestamp}.parquet"
        pq.write_table(table=table, where=parquet_path)
        client.upload_file(parquet_path, ENRICHED_BUCKET, f"telegram/context_date={date}/{timestamp}.parquet")

        return True

    except Exception as exc:
        logging.error(msg=exc)
        return False


def parse_data(data: dict) -> dict:
    parsed_data = {
        "message_id": [data.get("message_id")],
        "user_id": [data.get("from", {}).get("id")],
        "user_is_bot": [data.get("from", {}).get("is_bot")],
        "user_first_name": [data.get("from", {}).get("first_name")],
        "chat_id": [data.get("chat", {}).get("id")],
        "chat_type": [data.get("chat", {}).get("type")],
        "date": [data.get("date")],
        "text": [data.get("text") if "text" in data else None],
    }
    return parsed_data