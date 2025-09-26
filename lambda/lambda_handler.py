import boto3
import logging
import pandas as pd
from io import StringIO
import re
import os
from decimal import Decimal
import math

# Initialize the S3 client
s3_client = boto3.client('s3')

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['DYNAMODB_TABLE']  # Get table name from environment variable
table = dynamodb.Table(table_name)


def validate_data(df):
    """
    Function to validate the data.
    - Ensure all required fields are present.
    - Validate data types and formats.
    """
    # Check for missing required fields
    required_columns = ['user_id', 'email', 'name']
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Missing required field: {column}")

    # Validate email format
    email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    invalid_emails = []
    for email in df['email']:
        if not re.match(email_regex, email):
            invalid_emails.append(email)

    if invalid_emails:
        logging.warning(f"Invalid email formats found: {', '.join(invalid_emails)}")

    # Validate age and handle empty or invalid age
    if 'age' in df.columns:
        # Skip rows where age is missing or invalid
        invalid_ages = df[df['age'].isnull() | (df['age'] <= 0)]
        if not invalid_ages.empty:
            logging.warning(f"Invalid or missing ages found: {invalid_ages['user_id'].tolist()}")

    return df


def cleanse_data(df):
    """
    Function to cleanse the data.
    - Normalize text fields (e.g., email, name).
    - Handle missing values and duplicates.
    """
    # Normalize email to lowercase
    df['email'] = df['email'].str.lower()

    # Remove leading/trailing spaces in name field
    df['name'] = df['name'].str.strip()

    # Replace missing values with default values (e.g., "N/A" for missing phone numbers)
    df['phone_number'] = df['phone_number'].fillna('N/A')

    # Drop duplicates based on user_id
    df = df.drop_duplicates(subset='user_id')

    return df


def store_processed_data(df):
    """
    Function to store the processed data in DynamoDB.
    Convert all numeric fields to Decimal for DynamoDB compatibility,
    and replace NaN or Infinity with a default value.
    """
    for index, row in df.iterrows():
        # Replace NaN and Infinity values with default (0 for numeric fields, 'unknown' for string fields)
        if isinstance(row['age'], (float, int)):
            if math.isnan(row['age']) or math.isinf(row['age']):
                row['age'] = 0  # Replace NaN or Infinity with 0

        # Prepare the data for insertion
        item = {
            'user_id': str(row['user_id']),  # Ensure user_id is stored as a string
            'email': row['email'],
            'name': row['name'],
            'phone_number': row['phone_number'],
            # Convert numeric fields (like age) to Decimal
            'age': Decimal(str(row['age'])) if isinstance(row['age'], (float, int)) else row['age'],
            # Convert to Decimal
        }

        # Ensure that any numeric values are converted to Decimal
        for key, value in item.items():
            if isinstance(value, (float, int)):
                item[key] = Decimal(str(value))  # Convert to Decimal

        # Insert the item into DynamoDB
        try:
            table.put_item(Item=item)
            logging.info(f"Stored processed data for user_id: {row['user_id']}")
        except Exception as e:
            logging.error(f"Error storing data for user_id {row['user_id']}: {str(e)}")


def handler(event, context):
    # Log the entire event to inspect its structure
    print(f"Event: {event}")

    # Check if the event is from SQS
    if 'Records' in event and event['Records'][0].get('eventSource') == 'aws:sqs':
        for record in event['Records']:
            # Process SQS Message
            body = record.get('body', None)
            if body:
                print(f"SQS Message Body: {body}")
                # You can add further processing here if needed
            else:
                print("SQS Message body is missing")
        return {'statusCode': 200, 'body': 'SQS Message Processed'}

    # Check if the event is from S3
    elif 'Records' in event and event['Records'][0].get('eventSource') == 'aws:s3':
        for record in event['Records']:
            # Process S3 Event (file upload)
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']

            logging.info(f"File uploaded to S3: {bucket_name}/{file_key}")

            # Get the file content from S3
            try:
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = file_obj['Body'].read().decode('utf-8')
                logging.info(f"File content loaded successfully.")
            except Exception as e:
                logging.error(f"Error getting file from S3: {str(e)}")
                return {'statusCode': 500, 'body': f"Error: {str(e)}"}

            # Process CSV file with pandas
            try:
                df = pd.read_csv(StringIO(file_content))
                logging.info(f"CSV file loaded successfully. Rows: {len(df)}")
            except Exception as e:
                logging.error(f"Error reading CSV file: {str(e)}")
                return {'statusCode': 500, 'body': f"Error: {str(e)}"}

            # Validate and cleanse the data
            try:
                df = validate_data(df)  # Validate data
                df = cleanse_data(df)  # Cleanse data
                logging.info(f"Data validated and cleansed. Processed rows:\n{df.head()}")
            except ValueError as e:
                logging.error(f"Data validation failed: {str(e)}")
                return {'statusCode': 400, 'body': f"Data validation error: {str(e)}"}

            # Store the processed data in DynamoDB
            store_processed_data(df)

            # Final success response after processing
            return {'statusCode': 200, 'body': 'File processed and data validated successfully'}

    else:
        logging.error("Unsupported event source")
        return {'statusCode': 400, 'body': 'Unsupported event source'}
