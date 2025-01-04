import boto3
import json
import random
import csv
from io import StringIO
from datetime import datetime

# AWS Clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

# DynamoDB and S3 details
USER_STATE_TABLE = "UserQuestionState"
USERS_DATASET_BUCKET = "realtimerecommendation"
USERS_DATASET_KEY = "updated_usersmnew.csv"
QUESTIONS_DATASET_KEY = "updated_items (1).csv"
INTERACTION_DATASET_KEY = "updated_interactions_with_profiles_and_scores.csv"


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


def fetch_user_interaction_history(user_id):
    """Fetch user interaction history from the S3 dataset."""
    try:
        response = s3.get_object(Bucket=USERS_DATASET_BUCKET, Key=INTERACTION_DATASET_KEY)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        interaction_history = [row for row in csv_reader if row['user_id'] == str(user_id)]
        return interaction_history
    except Exception as e:
        print(f"Error fetching interaction history: {e}")
        return []


def convert_user_profile(profile):
    """Converts user profile to numerical value."""
    return {"Beginner": 1, "Intermediate": 2, "Expert": 3}.get(profile, 1)


def calculate_interaction_score(feedback):
    """Calculates interaction score based on feedback."""
    if feedback.lower() == "correct":
        return 3
    elif feedback.lower() == "incorrect":
        return 1
    else:
        return 0


def select_question(user_id, preferences, difficulty, questions):
    """Select a question based on user preferences, feedback, and difficulty."""
    filtered_questions = [
        question for question in questions
        if question['difficulty'].lower() == difficulty.lower()
        and any(pref.lower() in question['tags'].lower() for pref in preferences)
    ]
    if filtered_questions:
        return random.choice(filtered_questions)
    return None


def lambda_handler(event, context):
    # Log the entire event object for debugging
    print("Received event:", json.dumps(event, indent=2))

    # Fetch user_id from the event
    user_id = event.get('user_id') or event.get('queryStringParameters', {}).get('user_id')
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing or invalid user_id in request"})
        }

    # Step 1: Fetch user preferences and interaction history
    preferences, user_level = fetch_user_preferences(user_id)
    interaction_history = fetch_user_interaction_history(user_id)

    if not preferences:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": f"No preferences found for user_id: {user_id}"})
        }

    # Step 2: Determine difficulty level based on the last feedback
    last_feedback = interaction_history[-1] if interaction_history else {}
    last_feedback_type = last_feedback.get('FEEDBACK', "skipped").lower()
    last_difficulty = last_feedback.get('difficulty', 'easy').lower()

    if last_feedback_type == "correct":
        if last_difficulty == "easy":
            next_difficulty = "medium"
        elif last_difficulty == "medium":
            next_difficulty = "hard"
        else:
            next_difficulty = "easy"
    elif last_feedback_type == "incorrect":
        next_difficulty = "easy"
    else:
        next_difficulty = "easy"

    # Step 3: Fetch all questions and select one
    questions = fetch_questions()
    question = select_question(user_id, preferences, next_difficulty, questions)

    if not question:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": f"No questions found for topic: {', '.join(preferences)}"})
        }

    # Step 4: Update UserQuestionState in DynamoDB
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

    # Step 5: Append the interaction to the interaction dataset in S3
    try:
        # Fetch existing interactions
        response = s3.get_object(Bucket=USERS_DATASET_BUCKET, Key=INTERACTION_DATASET_KEY)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        existing_data = list(csv_reader)

        # Prepare the new entry
        new_entry = {
            "user_id": user_id,
            "item_id": question['ITEM_INT_ID'],
            "FEEDBACK": last_feedback_type,
            "timestamp": str(int(datetime.now().timestamp())),
            "difficulty": next_difficulty,
            "topic": question['tags'],
            "user_profile": convert_user_profile(user_level),
            "interaction_score": calculate_interaction_score(last_feedback_type)
        }

        # Append new interaction to existing data
        existing_data.append(new_entry)

        # Save back to S3
        updated_csv = StringIO()
        csv_writer = csv.DictWriter(updated_csv, fieldnames=existing_data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(existing_data)
        s3.put_object(
            Bucket=USERS_DATASET_BUCKET,
            Key=INTERACTION_DATASET_KEY,
            Body=updated_csv.getvalue()
        )
    except Exception as e:
        print(f"Error updating interaction dataset in S3: {e}")

    # Step 6: Return the selected question
    return {
        "statusCode": 200,
        "body": json.dumps({
            "question_id": question['ITEM_INT_ID'],
            "difficulty": next_difficulty,
            "tags": question['tags']
        })
    }
