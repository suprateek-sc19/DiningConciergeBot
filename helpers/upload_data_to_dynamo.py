import boto3
import pandas as pd
import datetime
# Load CSV data
csv_file_path = 'results.csv'  # Adjust path as needed
df = pd.read_csv(csv_file_path)

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Replace with your table name

# Iterate through each row in the DataFrame and add it to DynamoDB
for index, row in df.iterrows():
    item = {
        'restaurant_id': str(row['restaurant_id']),
        'name': str(row['name']),
        'cuisine' : str(row['cuisine']),
        'location': str(row['location']),
        'review_count': str(row['review_count']),
        'rating': str(row['rating']),
        'coordinates': str(row['coordinates']),
        'zip_code': str(row['zip_code']),
        'address': str(row['address']),
        'inserted_at_timestamp': str(datetime.datetime.now())
    }
    # Insert the item into DynamoDB
    response = table.put_item(Item=item)
    print(f"Added item: {index+1}/{len(df)}", response)

print("Data upload complete.")
