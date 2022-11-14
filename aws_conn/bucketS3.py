import pandas as pd
import boto3
import pyodbc

# BUCKET CONN
bucket_name = 'sam_bucket'
object_key = 'arquivo_exemplo.csv'
aws_client = boto3.client('s3')

# GET CSV
get_csv_from_s3 = aws_client.get_object(Bucket=bucket_name, Key=object_key)

# READ CSV ['BODY']
info = pd.read_csv(get_csv_from_s3['Body'])

# READS SQL FILE
sql_file = open('CREATE TABLE CESSAO_FUNDO.sql')
sql_str = sql_file.read()

# CONN TO LOCAL DB
connection = pyodbc.connect('DRIVER={driver};'
                            'SERVER=localhost;'
                            f'DATABASE={object_key};'
                            'UID=sam;'
                            'PWD=12345')

cursor = connection.cursor()
cursor.execute(sql_str)

