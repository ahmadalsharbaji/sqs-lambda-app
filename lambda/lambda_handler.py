def handler(event, context):
    print(event['Records'][0]['body'])
    return {'statusCode': 200, 'body': 'Success'}
