import boto3
import json
import csv
import io

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# Constants
USER_FEEDBACK_TABLE = 'UserFeedback'
ITEMS_METADATA_S3_BUCKET = 'realtimerecommendation'
ITEMS_METADATA_S3_KEY = 'updated_items (1).csv'
INTERACTIONS_DATASET_S3_BUCKET = 'realtimerecommendation'
INTERACTIONS_DATASET_S3_KEY = 'updated_interactions_with_profiles_and_scores.csv'

def fetch_item_metadata(item_int_id):
    try:
        # Fetch the items dataset from S3
        response = s3.get_object(Bucket=ITEMS_METADATA_S3_BUCKET, Key=ITEMS_METADATA_S3_KEY)
        csv_data = response['Body'].read().decode('utf-8')
        
        # Parse CSV to find the matching item
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        for row in csv_reader:
            if row['ITEM_INT_ID'] == str(item_int_id):  # Match using ITEM_INT_ID
                return {
                    'difficulty': row.get('difficulty', 'unknown'),
                    'tags': row.get('tags', 'unknown')  # Use 'tags' as a single string
                }
        print(f"Item metadata not found for ITEM_INT_ID: {item_int_id}")
        return None
    except Exception as e:
        print(f"Error fetching item metadata from S3: {e}")
        return None

def determine_user_profile(user_feedback):
    correct_count = sum(1 for feedback in user_feedback if feedback['feedback'] == 'correct')
    total_count = len(user_feedback)
    if total_count == 0:
        return 'beginner'
    accuracy = correct_count / total_count
    if accuracy > 0.8:
        return 'expert'
    elif accuracy > 0.5:
        return 'intermediate'
    else:
        return 'beginner'

def lambda_handler(event, context):
    try:
        # Fetch feedback from DynamoDB
        response = dynamodb.scan(TableName=USER_FEEDBACK_TABLE)
        feedback_data = response.get('Items', [])
        
        if not feedback_data:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'No feedback available to process.'})
            }

        enriched_feedback = []

        # Enrich feedback
        for feedback in feedback_data:
            user_id = feedback['user_id']['S']
            item_int_id = feedback['question_id']['S']  # Use 'question_id' instead of 'item_id'
            event_type = feedback['feedback']['S'].lower()
            timestamp = feedback['timestamp']['N']

            # Fetch item metadata
            item_metadata = fetch_item_metadata(item_int_id)
            if not item_metadata:
                print(f"Item metadata not found for question_id: {item_int_id}")
                continue

            difficulty = item_metadata['difficulty']
            tags = item_metadata['tags']

            enriched_feedback.append({
                'user_id': user_id,
                'item_id': item_int_id,
                'event_type': event_type,
                'timestamp': int(timestamp),
                'difficulty': difficulty,
                'topic': tags,  # Keep topics as a single string
                'user_profile': determine_user_profile(feedback_data),  # Determine user profile dynamically
                'interaction_score': 1 if event_type == 'correct' else -1 if event_type == 'incorrect' else 0
            })

        # Load existing interactions dataset
        s3_object = s3.get_object(Bucket=INTERACTIONS_DATASET_S3_BUCKET, Key=INTERACTIONS_DATASET_S3_KEY)
        existing_data = s3_object['Body'].read().decode('utf-8')
        existing_rows = existing_data.splitlines()

        # Append enriched feedback to interactions dataset
        enriched_rows = []
        for feedback in enriched_feedback:
            enriched_rows.append(
                f"{feedback['user_id']},{feedback['item_id']},{feedback['event_type']},{feedback['timestamp']},{feedback['difficulty']},\"{feedback['topic']}\",{feedback['user_profile']},{feedback['interaction_score']}"
            )
        
        updated_dataset = '\n'.join(existing_rows + enriched_rows)

        # Upload updated dataset to S3
        s3.put_object(
            Bucket=INTERACTIONS_DATASET_S3_BUCKET,
            Key=INTERACTIONS_DATASET_S3_KEY,
            Body=updated_dataset
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Interactions dataset updated successfully.'})
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'An error occurred: {e}'})
        }
