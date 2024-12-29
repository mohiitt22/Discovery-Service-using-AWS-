import boto3

# Initialize Personalize client
personalize = boto3.client('personalize')

# Replace with your dataset group, schemas, and S3 bucket
s3_bucket_name = "realtimerecommendation"
role_arn = "arn:aws:iam::<your-account-id>:role/<your-role-name>"

def import_dataset(dataset_arn, s3_path, role_arn):
    response = personalize.create_dataset_import_job(
        jobName=f"import-{dataset_arn.split('/')[-1]}",
        datasetArn=dataset_arn,
        dataSource={"dataLocation": s3_path},
        roleArn=role_arn
    )
    return response['datasetImportJobArn']

# Replace with actual ARNs for datasets
interaction_dataset_arn = "arn:aws:personalize:<region>:<account-id>:dataset/InteractionsDataset"
item_dataset_arn = "arn:aws:personalize:<region>:<account-id>:dataset/ItemsDataset"
user_dataset_arn = "arn:aws:personalize:<region>:<account-id>:dataset/UsersDataset"

# Import datasets
interaction_job_arn = import_dataset(interaction_dataset_arn, f"s3://{s3_bucket_name}/updated_interactions_with_profiles_and_scores.csv", role_arn)
item_job_arn = import_dataset(item_dataset_arn, f"s3://{s3_bucket_name}/updated_items (1).csv", role_arn)
user_job_arn = import_dataset(user_dataset_arn, f"s3://{s3_bucket_name}/updated_usersmnew.csv", role_arn)

print("Interaction Dataset Import Job ARN:", interaction_job_arn)
print("Item Dataset Import Job ARN:", item_job_arn)
print("User Dataset Import Job ARN:", user_job_arn)
