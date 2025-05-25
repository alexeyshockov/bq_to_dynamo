import datetime

from boto3.dynamodb.types import TypeSerializer

_serializer = TypeSerializer()


# See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types
def value_to_dynamo(value):
    if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
        return {"S": value.isoformat()}
    if isinstance(value, float):
        return {"N": str(value)}
    return _serializer.serialize(value)
