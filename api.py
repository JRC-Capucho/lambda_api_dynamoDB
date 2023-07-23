import boto3
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Attr
    
def convert_decimal_json(data):
    if isinstance(data, Decimal):
        return str(data)
    
    
def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb') # declarando que e dynamodb que irei usar
    
    table = dynamodb.Table('data-thing') # nome da tabela
    
        # filter do time de  0 ate o final
        # me devolve uma o lista
    response = table.scan(FilterExpression=Attr('time').gte(0))
    
    data =  response["Items"] # lista 
    
    datas = []
    
    for chunk in data:
        temp = chunk['device_data']['Temperatura']
        humi = chunk['device_data']['Umidade']
        res = {
            "time": chunk['time'],
            "id": chunk['id'],
            "Humidity": humi,
            "Temperature": temp
            }
        datas.append(res)
    
    return json.dumps(datas, default=convert_decimal_json)
