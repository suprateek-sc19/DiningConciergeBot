import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json
import re
import requests
import traceback
import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

email = None

SQS_URL = "https://sqs.us-east-1.amazonaws.com/637423434700/DiningBotQueue"
ses_client = boto3.client('ses')
sqs_client = boto3.client('sqs')
es_client = boto3.client('es')
dynamodb = boto3.resource('dynamodb')


def sendSQS(request_data):
    try:

        # Assuming request_data keys are exactly as they appear here and match the case used in your slots
        location = request_data["Location"]
        cuisine = request_data["Cuisine"]
        number_of_people = request_data["NumberOfPeople"]
        dining_date = request_data["DiningDate"]
        dining_time = request_data["DiningTime"]

        # Message attributes setup
        message_attributes = {
            "location": {
                'DataType': 'String',
                'StringValue': location
            },
            "Cuisine": {
                'DataType': 'String',
                'StringValue': cuisine
            },
            "NumberOfPeople": {
                'DataType': 'Number',
                'StringValue': number_of_people
            },
            "DiningDate": {
                'DataType': 'String',
                'StringValue': dining_date
            },
            "DiningTime": {
                'DataType': 'String',
                'StringValue': dining_time
            },
            "Email": {
                'DataType': 'String',
                'StringValue': email
            }
        }

        # The body of the message
        body = 'Dining bot slots'

        # Sending the message to the SQS queue
        response = sqs_client.send_message(
            QueueUrl=SQS_URL,
            MessageAttributes=message_attributes,
            MessageBody=body
        )

        print("Saved attributes in SQS successfully")

        return response
    except Exception as e:
        print(traceback.format_exc())


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def valid_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if (re.fullmatch(regex, email)):
        return True
    return False


def validate_dining_suggestions(location, cuisine, number_of_people, dining_date, dining_time):
    # Locations
    locations = ['manhattan']
    cuisines = ['chinese', 'italian', 'mexican']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                       'Location',
                                       'We do not have dining suggestions for {}, would you like suggestions for other locations?  '
                                       'Our most popular location is Manhattan'.format(location))

    # Cuisine
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False, 'Cuisine',
                                       'We do not have suggestions for {}, would you like suggestions for another cuisine?'
                                       'Our most popular cuisine is Italian'.format(cuisine))

    # Number of people
    if number_of_people is not None:
        number_of_people = parse_int(number_of_people)
        if not 0 < number_of_people < 30:
            return build_validation_result(False, 'NumberOfPeople', '{} does not look like a valid number, '
                                                                    'please enter a number less than 30'.format(
                number_of_people))

    # DiningDate
    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'DiningDate',
                                           'I did not understand that, what date would you like for your suggestion?')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'DiningDate',
                                           'You can pick a date from today onwards.  What day would you like for your suggestion?')

    # DiningTime
    if dining_time is not None:
        if len(dining_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', None)

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', None)

        # Edge case
        ctime = datetime.datetime.now()

        if datetime.datetime.strptime(dining_date, "%Y-%m-%d").date() == datetime.datetime.today():
            if (ctime.hour >= hour and ctime.minute > minute) or ctime.hour < hour or (
                    ctime.hour == hour and minute <= ctime.minute):
                return build_validation_result(False, 'DiningTime', 'Please select a time in the future.')

    return build_validation_result(True, None, None)


def check_email_in_db(email):
    print("CHECKING")
    table_name = 'previous-recs'
    table = dynamodb.Table(table_name)

    response = table.get_item(
        Key={
            'email': str(email)
        }
    )
    if 'Item' in response:
        return response['Item']
    else:
        return None


def es_query_for_cuisine(es_client, cuisine_type):
    es_url = 'https://search-restaurants-data-muort2nl3f5mctqi6272obu7e4.aos.us-east-1.on.aws/restaurants/_search?'
    headers = {'Content-Type': 'application/json'}
    auth = ('suprateek-es', 'Password@12')
    query = {
        "query": {
            "match": {
                "cuisine": cuisine_type
            }
        }, "size": 100
    }
    try:
        response = requests.get(es_url, headers=headers, auth=auth, data=json.dumps(query))
        if response.status_code == 200:
            results = response.json()
            ids = []
            for res in results['hits']['hits']:
                id = res['_source']['restaurant_id']
                ids.append(id)
            return ids
        return []
    except Exception as e:
        print(e)


def format_email_body(restaurants_info, user_details):
    html = """<tr style="background-color: #f2f2f2;">
    <th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Name</th>
    <th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Rating</th>
    <th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Review Count</th>
    <th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Address</th>
    """

    for data in restaurants_info:
        address = data['address'].replace("\'", '').replace('[', '').replace(']', '').replace('"', '')

        # Append a row to the table for each JSON object
        html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{data['name']}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{data['rating']}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{data['review_count']}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{address}</td>
                </tr>
        """

    html_1 = f'''<html><head></head><body><p>Here are the requested suggestions for the following details: <br> Location: {user_details['Location']} <br> 
    Cuisine: {user_details['Cuisine']} <br>
    </p><p><table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd;">>'''
    html_2 = "</table></p></body></html>"

    return html_1 + html + html_2


def send_email(ses_client, email, email_body):
    sender_email = "suprateek1912@gmail.com"

    subject = "Restaurant Recommendations based on previous search"

    try:
        response = ses_client.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [
                    email
                ]
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': 'Here are some fresh recommendations based on your previous search!',
                        'Charset': 'UTF-8'
                    },
                    'Html': {
                        'Data': email_body,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        print("Email sent! Message ID:", response['MessageId'])
    except Exception as e:
        print(f"Failed to send email: {str(e)}")


def fetch_restaurant_info(dynamodb, restaurant_id):
    table_name = 'yelp-restaurants'
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={'restaurant_id': restaurant_id}
        )

        if 'Item' in response:

            return response['Item']
        else:
            print(f"No item found with id: {restaurant_id}")
            return None
    except Exception as e:
        print(f"Failed to fetch item from DynamoDB: {str(e)}")
        return None


def dining_suggestions(intent_request):
    slots = get_slots(intent_request)
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    number_of_people = slots["NumberOfPeople"]
    dining_date = slots["DiningDate"]
    dining_time = slots["DiningTime"]
    source = intent_request['invocationSource']

    request_data = {
        "Location": location,
        "Cuisine": cuisine,
        "NumberOfPeople": number_of_people,
        "DiningDate": dining_date,
        "DiningTime": dining_time,
    }

    output_session_attributes = intent_request['sessionAttributes'] if intent_request[
                                                                           'sessionAttributes'] is not None else {}
    output_session_attributes['requestData'] = json.dumps(request_data)

    if source == 'DialogCodeHook':
        validation_result = validate_dining_suggestions(location, cuisine, number_of_people, dining_date, dining_time)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'], intent_request['currentIntent']['name'], slots,
                               validation_result['violatedSlot'], validation_result['message'])

        return delegate(output_session_attributes, slots)
    sendSQS(request_data)
    return close(intent_request['sessionAttributes'], 'Fulfilled', {'contentType': 'PlainText',
                                                                    'content': 'I have sent you the suggestions  on your email. Have a good day!'})


""" --- Intents --- """


def greeting_intent(intent_request):
    global email
    # Extract slots and session attributes from the intent request
    slots = intent_request.get('currentIntent', {}).get('slots', {})
    session_attributes = intent_request.get('sessionAttributes', {})

    # Extract the email slot value
    email = slots.get('email', None)

    # Check if the email slot is filled
    if not email:
        # If not, elicit the email slot
        return elicit_slot(session_attributes, intent_request['currentIntent']['name'],
                           slots, 'email',
                           {'contentType': 'PlainText',
                            'content': 'Welcome to the Dining Concierge bot! Please enter your email address to continue.'})
    else:
        # If the email slot is filled, proceed with the greeting

        res = check_email_in_db(email)

        if res is not None:
            cuisine_type = res['cuisine']
            es_response = es_query_for_cuisine(es_client, cuisine_type)

            if len(es_response) > 0:
                ids = random.sample(es_response, 3)

            restaurants = []

            for id in ids:
                restaurant_info = fetch_restaurant_info(dynamodb, id)
                restaurants.append(restaurant_info)

            user_details = {'Location': res['location'], 'Cuisine': cuisine_type}

            email_body = format_email_body(restaurants, user_details)

            send_email(ses_client, 'sc10344@nyu.edu', email_body)

            content = 'Fresh restaurant recommendations have been emailed to you based on your previous search. How can I help again?'
        else:
            content = 'You are a new user. How can I help?'

        return close(session_attributes,
                     'Fulfilled',
                     {'contentType': 'PlainText',
                      'content': content})


def thank_you_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "You're welcome."})


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    # logger.debug(f"{intent_request} ----------------- ")
    # logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    try:

        intent_name = intent_request['currentIntent']['name']

        # Dispatch to your bot's intent handlers
        if intent_name == 'DiningSuggestionsIntent':
            return dining_suggestions(intent_request)
        elif intent_name == 'GreetingIntent':
            return greeting_intent(intent_request)
        elif intent_name == 'ThankYouIntent':
            return thank_you_intent(intent_request)
    except Exception as e:
        logger.debug(str(traceback.format_exc()))


def lambda_handler(event, context):
    time.tzset()
    return dispatch(event)
