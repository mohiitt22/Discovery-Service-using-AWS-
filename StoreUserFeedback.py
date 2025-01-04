import boto3
import json
import time
import csv
from io import StringIO

# AWS Service Initialization
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Table names and S3 Bucket details
USER_STATE_TABLE = "UserQuestionState"
USER_FEEDBACK_TABLE = "UserFeedback"
USERS_DATASET_BUCKET = "realtimerecommendation"
USERS_DATASET_KEY = "updated_usersmnew.csv"

# Initialize DynamoDB Tables
user_state_table = dynamodb.Table(USER_STATE_TABLE)
feedback_table = dynamodb.Table(USER_FEEDBACK_TABLE)

def calculate_user_accuracy(user_id):
    """Calculate accuracy based on feedback data stored in DynamoDB."""
    try:
        response = feedback_table.scan()
        feedback_data = response.get('Items', [])
        
        # Filter feedback for the current user
        user_feedback = [fb for fb in feedback_data if fb['user_id'] == user_id]
        
        if not user_feedback:
            return 0.0
        
        # Calculate accuracy
        correct_count = sum(1 for feedback in user_feedback if feedback['feedback'].lower() == 'correct')
        total_count = len(user_feedback)
        accuracy = correct_count / total_count
        return accuracy
    except Exception as e:
        print(f"Error calculating accuracy: {e}")
        return 0.0

def determine_user_profile(accuracy):
    """Determine user profile based on accuracy with numeric values."""
    if accuracy > 0.8:
        return 3  # Expert = 3
    elif accuracy > 0.5:
        return 2  # Intermediate = 2
    else:
        return 1  # Beginner = 1

def update_user_metadata_in_s3(user_id, updated_profile):
    """Update the user profile in the S3 dataset with numeric profile values."""
    try:
        response = s3.get_object(Bucket=USERS_DATASET_BUCKET, Key=USERS_DATASET_KEY)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        
        # Prepare updated content
        updated_rows = []
        for row in csv_reader:
            if row['user_id'] == user_id:
                row['user_level'] = updated_profile  # Numeric Profile Value Updated Here
            updated_rows.append(row)
        
        # Write back to S3
        csv_output = StringIO()
        csv_writer = csv.DictWriter(csv_output, fieldnames=updated_rows[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(updated_rows)
        
        # Upload updated dataset back to S3
        s3.put_object(Bucket=USERS_DATASET_BUCKET, Key=USERS_DATASET_KEY, Body=csv_output.getvalue())
        print(f"S3 user profile updated for user: {user_id}")
        
    except Exception as e:
        print(f"Error updating S3 user metadata: {e}")

def lambda_handler(event, context):
    """Main Lambda function to handle user feedback and update profiles."""
    user_id = event.get('user_id')
    feedback = event.get('feedback')

    if not user_id or not feedback:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing user_id or feedback in request"})
        }

    # Step 1: Fetch current question from UserQuestionState Table
    try:
        user_state_response = user_state_table.get_item(Key={'user_id': user_id})
        if 'Item' not in user_state_response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"No state found for user_id: {user_id}"})
            }
        current_question = user_state_response['Item']['current_question']
    except Exception as e:
        print(f"Error fetching user state: {e}")
        return {"statusCode": 500, "body": json.dumps({"message": str(e)})}

    # Step 2: Store Feedback in DynamoDB (Timestamp Corrected)
    try:
        timestamp = int(time.time())  # Ensuring timestamp is stored as Number
        feedback_table.put_item(
            Item={
                'user_id': user_id,
                'question_id': current_question,
                'feedback': feedback,
                'timestamp': timestamp  # FIXED: Number instead of string
            }
        )
        print(f"Feedback recorded for user {user_id}")
    except Exception as e:
        print(f"Error storing feedback: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"message": str(e)})}

    # Step 3: Calculate Accuracy and Check Profile Upgrade
    accuracy = calculate_user_accuracy(user_id)
    new_profile = determine_user_profile(accuracy)

    # Step 4: Update Profile in DynamoDB and S3
    try:
        # Update User Profile in DynamoDB
        user_state_table.update_item(
            Key={'user_id': user_id},
            UpdateExpression="SET current_profile = :profile",
            ExpressionAttributeValues={':profile': new_profile}
        )
        
        # Update User Metadata in S3 Dataset
        update_user_metadata_in_s3(user_id, new_profile)
    except Exception as e:
        print(f"Error updating user profile: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"message": str(e)})}

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"User profile updated to level '{new_profile}' for user {user_id}",
            "accuracy": accuracy
        })
    }
