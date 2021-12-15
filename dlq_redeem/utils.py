import datetime
import decimal
import json


class DynamodbEncoder(json.JSONEncoder):
    """
    Dictionaries returned from DynamoDB by boto3 have potential value types that are not JSON-serializable, e.g.,
    if a field's value is Number type then the item dictionary will contain it as a decimal.Decimal value.
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return int(o) if o % 1 == 0 else str(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        return super().default(o)
