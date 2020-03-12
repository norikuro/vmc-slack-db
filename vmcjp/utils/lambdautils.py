import json
from botocore.session import Session
from botocore.config import Config

def call_lambda(function, data):
    s = Session()
    clientLambda = s.create_client(
        "lambda", 
        config=Config(retries={'max_attempts': 0})
    )
    clientLambda.invoke(
        FunctionName=function,
        InvocationType="Event",
        Payload=json.dumps(data)
    )
