import boto3
import requests
from datetime import datetime


def collect_api(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erro na solicitação: {e}")
        return None


def lambda_handler(event, context):
    url_api = "https://jsonplaceholder.typicode.com/posts"
    data = collect_api(url_api)
    datetime_now = datetime.now()
    date_now = datetime_now.strftime("%Y-%m-%d")
    hour_now = datetime_now.strftime("%H-%M-%S")

    s3 = boto3.client("s3")
    bucket_name = "camada-raw"
    s3_key = f"jsonplaceholder/date={date_now}/{hour_now}-posts.json"

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
