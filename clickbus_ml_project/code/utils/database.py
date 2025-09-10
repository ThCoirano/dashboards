import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
from sqlalchemy import create_engine


def get_secret(host):

    secret_name = host
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return secret

keepalive_kwargs = {
  "keepalives": 1,
  "keepalives_idle": 60,
  "keepalives_interval": 10,
  "keepalives_count": 5
}


def connectdatabase(host):

    if host == 'pad-redshift':
        port = 5439
    else:
        port = 5432

    constants_con = get_secret(host)

    con = psycopg2.connect(
        host = constants_con['endpoint'],
        dbname = constants_con['database'],
        user = constants_con['user'],
        password = constants_con['password'],
        port = port,
        **keepalive_kwargs
    )
    return con

def conn_engine(host):

    constants_con = get_secret(host)

    engine = create_engine(f"postgresql+psycopg2://{constants_con['user']}:{constants_con['password']}@{constants_con['endpoint']}:5432/{constants_con['database']}")

    return engine




# host: dw-instance1.cn4666ecytqb.us-east-1.rds.amazonaws.com
# user: postgres
# senha: 0P3NF1N4NC32024


