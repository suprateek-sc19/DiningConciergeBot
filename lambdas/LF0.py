import json
import boto3
import traceback

lex_client = boto3.client('lex-runtime')


def process_message(message):
    # Assuming 'message' contains the text to send to Lex
    # Replace 'BotName' and 'BotAlias' with your Lex bot's name and alias
    lex_response = lex_client.post_text(
        botName='DiningBot',
        botAlias='Stage',
        userId='12345',
        inputText=message
    )

    print(lex_response["message"])

    # Here you can format the Lex response as needed
    return {
        "type": "unstructured",
        "unstructured": {
            "id": "string",  # Modify as necessary
            "text": lex_response['message'],  # Using the message from Lex response
            "timestamp": "string"  # Modify as necessary
        }
    }


def lambda_handler(event, context):
    try:
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)

        # Processing each message in the request
        print(body["messages"][0]["unstructured"]["text"])
        response_messages = [process_message(body["messages"][0]["unstructured"]["text"])]

        # Construct the successful response
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"messages": response_messages})
        }

    except Exception as e:
        # Log the error
        print(f"Error: {str(traceback.format_exc())}")

        # Construct the error response
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"code": 500, "message": e})
        }
