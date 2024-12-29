import boto3
import json
import random
import csv
from io import StringIO

# AWS Clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

# DynamoDB and S3 details
USER_STATE_TABLE = "UserQuestionState"
USERS_DATASET_BUCKET = "realtimerecommendation"
USERS_DATASET_KEY = "updated_usersmnew.csv"
QUESTIONS_DATASET_KEY = "updated_items (1).csv"

def fetch_user_preferences(user_id):
    """Fetch user preferences from the user dataset in S3."""
    try:
        response = s3.get_object(Bucket=USERS_DATASET_BUCKET, Key=USERS_DATASET_KEY)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        for row in csv_reader:
            if row['user_id'] == str(user_id):
                return row['preferences'].split(', '), int(row['user_level'])
    except Exception as e:
        print(f"Error fetching user preferences: {e}")
    return [], 1  # Default to an empty preference list and beginner level.

def fetch_questions():
    """Fetch all questions from the question dataset in S3."""
    try:
        response = s3.get_object(Bucket=USERS_DATASET_BUCKET, Key=QUESTIONS_DATASET_KEY)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        return [row for row in csv_reader]
    except Exception as e:
        print(f"Error fetching questions: {e}")
    return []

def select_question(user_id, preferences, difficulty, questions):
    """Select a question based on user preferences and difficulty."""
    # Filter questions matching preferences and difficulty
    filtered_questions = [
        question for question in questions
        if question['difficulty'].lower() == difficulty.lower() and 
           any(pref.lower() in question['tags'].lower() for pref in preferences)
    ]
    if filtered_questions:
        return random.choice(filtered_questions)  # Return a random matching question
    return None  # No matching questions found

def lambda_handler(event, context):
    user_id = event.get("user_id")
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing user_id in request"})
        }

    # Step 1: Fetch user preferences
    preferences, user_level = fetch_user_preferences(user_id)
    if not preferences:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": f"No preferences found for user_id: {user_id}"})
        }

    # Step 2: Fetch all questions
    questions = fetch_questions()
    if not questions:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to fetch questions from dataset"})
        }

    # Step 3: Select an easy question for the user
    difficulty = "easy"
    question = select_question(user_id, preferences, difficulty, questions)
    
    if not question:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": f"No questions found for topic: {', '.join(preferences)} and difficulty: {difficulty}"})
        }

    # Step 4: Update user's current question in DynamoDB
    try:
        dynamodb.put_item(
            TableName=USER_STATE_TABLE,
            Item={
                "user_id": {"S": str(user_id)},
                "current_question": {"S": question['ITEM_INT_ID']}
            }
        )
    except Exception as e:
        print(f"Error updating user state in DynamoDB: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to update user state in DynamoDB"})
        }

    # Step 5: Return the selected question
    return {
        "statusCode": 200,
        "body": json.dumps({
            "question_id": question['ITEM_INT_ID'],
            "difficulty": question['difficulty'],
            "tags": question['tags']
        })
    }
