import pandas as pd
import random

# Load the item dataset
def load_item_data(file_path):
    item_data = pd.read_csv(file_path)
    return item_data

# Generate synthetic user data based on item tags
def generate_user_data(item_data, num_users):
    all_tags = []
    for tags in item_data['tags']:
        all_tags.extend([tag.strip() for tag in tags.split(',')])
    unique_tags = list(set(all_tags))  # Unique tags from the item dataset

    synthetic_data = {
        "user_id": [i + 1 for i in range(num_users)],
        "preferences": [
            ", ".join(random.sample(unique_tags, random.randint(2, 5)))  # Random subset of tags
            for _ in range(num_users)
        ],
        "user_level": [random.randint(1, 3) for _ in range(num_users)]  # Random user level (1-3)
    }
    return pd.DataFrame(synthetic_data)

# Main script
item_file_path = '/mnt/data/updated_items (1).csv'
item_data = load_item_data(item_file_path)

num_users = 100  # Number of users to simulate
synthetic_user_data = generate_user_data(item_data, num_users)

# Save the simulated user data to a CSV file
synthetic_user_data.to_csv("updated_usersmnew.csv", index=False)

# Display the first few rows of the synthetic user dataset
print(synthetic_user_data.head())
