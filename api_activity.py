import boto3
import os
import json

endpoint_url = os.environ.get("endpoint_url")
gatewayapi = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)

def send_message_to_connection(connection_id, data):
    try:
        # Convert dict → JSON string → bytes
        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")
        elif isinstance(data, str):
            data = data.encode("utf-8")
        print('data:', data)
        gatewayapi.post_to_connection(
            Data=data,
            ConnectionId=connection_id
        )
    except Exception as e:
        print(f"❌ Error sending message to connection {connection_id}: {e}")
    