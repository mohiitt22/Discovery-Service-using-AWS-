import boto3
import time

# Initialize Personalize client
personalize = boto3.client('personalize')

# Replace with actual ARNs
dataset_group_arn = "arn:aws:personalize:<region>:<account-id>:dataset-group/DiscoveryServiceGroup"
recipe_arn = "arn:aws:personalize:::recipe/aws-user-personalization"

def create_solution(name, dataset_group_arn, recipe_arn):
    response = personalize.create_solution(
        name=name,
        datasetGroupArn=dataset_group_arn,
        recipeArn=recipe_arn
    )
    return response['solutionArn']

def create_solution_version(solution_arn):
    response = personalize.create_solution_version(solutionArn=solution_arn)
    return response['solutionVersionArn']

def create_campaign(name, solution_version_arn):
    response = personalize.create_campaign(
        name=name,
        solutionVersionArn=solution_version_arn,
        minProvisionedTPS=1
    )
    return response['campaignArn']

# Create solution
solution_arn = create_solution("DiscoveryServiceSolution", dataset_group_arn, recipe_arn)
print("Solution ARN:", solution_arn)

# Wait for the solution to be ready
print("Waiting for solution to be ready...")
time.sleep(120)  # Adjust based on solution creation time

solution_version_arn = create_solution_version(solution_arn)
print("Solution Version ARN:", solution_version_arn)

# Wait for the solution version to be ready
print("Waiting for solution version to be ready...")
time.sleep(120)  # Adjust based on solution version creation time

campaign_arn = create_campaign("DiscoveryServiceCampaign", solution_version_arn)
print("Campaign ARN:", campaign_arn)
