import sys
import pandas as pd
import boto3
import sqlalchemy as db
from sqlalchemy import MetaData, Integer, String, Date, Column, Table, DECIMAL


# READ CSV FROM S3 BUCKET
def s3_cloud(bucket_name, object_key):
    aws_client = boto3.client('s3')
    csv_obj_from_s3 = aws_client.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_csv(csv_obj_from_s3['Body'], encoding='latin-1')
    return df


# DB CREATION
def db_creation(df):
    engine = db.create_engine('sqlite:///s3_info.db')
    meta_obj = MetaData()
    df.to_sql(
        'cessao_fundo',
        engine,
        dtype={
            'ID_CESSAO': Integer,
            'ORIGINADOR': String(250),
            'DOC_ORIGINADOR': Integer,
            'CEDENTE': String(250),
            'DOC_CEDENTE': Integer,
            'CCB INT': Integer,
            'ID_EXTERNO': Integer,
            'CLIENTE': String(250),
            'CCPF_CNPJ': String(50),
            'ENDERECO': String(250),
            'CEP': String(50),
            'CIDADE': String(250),
            'UF': String(50),
            'VALOR_DO_EMPRESTIMO': DECIMAL(10, 2),
            'VALOR_PARCELA': DECIMAL(10, 2),
            'TOTAL_PARCELAS': Integer,
            'PARCELA': Integer,
            'DATA_DE_EMISSAO': Date,
            'DATA_DE_VENCIMENTO': Date,
            'PRECO_DE_AQUISICAO': DECIMAL(10, 2),
        }
    )
    meta_obj.create_all(engine)


if __name__ == '__main__':
    bucket_name = sys.argv[1]
    object_key = sys.argv[2]
    res_df = s3_cloud(bucket_name, object_key)
    db_creation(res_df)
