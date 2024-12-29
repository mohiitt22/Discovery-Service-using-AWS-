import boto3
import json
import time

# Initialize AWS services
dynamodb = boto3.resource('dynamodb')
personalize_runtime = boto3.client('personalize-runtime')

# Table names
user_state_table_name = "UserQuestionState"  # Existing table for storing user state
feedback_table_name = "UserFeedback"        # New table for storing feedback

# DynamoDB tables
user_state_table = dynamodb.Table(user_state_table_name)
feedback_table = dynamodb.Table(feedback_table_name)

# Amazon Personalize Campaign ARN (replace with your actual ARN)
campaign_arn = "arn:aws:personalize:us-east-1:058264091727:campaign/myrealtimecampaign"

def lambda_handler(event, context):
    # Retrieve user_id and feedback from the event
    user_id = event.get('user_id')
    feedback = event.get('feedback')  # Example values: "Correct", "Incorrect", "Skipped"

    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing user_id in request"})
        }

    # Step 1: Retrieve current question for the user
    try:
        user_state_response = user_state_table.get_item(Key={'user_id': user_id})
        if 'Item' not in user_state_response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"No state found for user_id {user_id}"})
            }

        current_question = user_state_response['Item']['current_question']
        print(f"Current question for user {user_id}: {current_question}")

    except Exception as e:
        print(f"Error fetching user state: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error fetching user state: {str(e)}"})
        }

    # Step 2: Save feedback to the UserFeedback table
    try:
        timestamp = int(time.time())  # Current timestamp
        feedback_table.put_item(
            Item={
                'user_id': user_id,
                'question_id': current_question,
                'feedback': feedback,
                'timestamp': timestamp
            }
        )
        print(f"Successfully stored feedback for user {user_id}, question {current_question}, feedback {feedback}")
    except Exception as e:
        print(f"Error storing feedback: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error storing feedback: {str(e)}"})
        }

    # Step 3: Return a response indicating success
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Feedback '{feedback}' stored for question {current_question} by user {user_id}"
        })
    }
