# Simulate interactions again with the user profile included
interactions = []

for user_id in users[user_id_column]:
    profile = user_profiles[user_id]
    profile_probabilities = profiles[profile]
    
    # Simulate 10 interactions per user
    for _ in range(10):
        item = items.sample(1).iloc[0]
        difficulty = item[difficulty_column]
        topic = item[topic_column]
        
        # Determine event type based on profile probabilities and difficulty
        if difficulty == 'easy':
            event_type = random.choices(
                event_types,
                weights=[
                    profile_probabilities.get('easy_correct', 0.0),
                    1 - profile_probabilities.get('easy_correct', 0.0) - profile_probabilities.get('easy_skipped', 0.0),
                    profile_probabilities.get('easy_skipped', 0.0)
                ]
            )[0]
        elif difficulty == 'medium':
            event_type = random.choices(
                event_types,
                weights=[
                    profile_probabilities.get('medium_correct', 0.0),
                    1 - profile_probabilities.get('medium_correct', 0.0) - profile_probabilities.get('medium_skipped', 0.0),
                    profile_probabilities.get('medium_skipped', 0.0)
                ]
            )[0]
        else:  # hard
            event_type = random.choices(
                event_types,
                weights=[
                    profile_probabilities.get('hard_correct', 0.0),
                    1 - profile_probabilities.get('hard_correct', 0.0) - profile_probabilities.get('hard_skipped_or_incorrect', 0.0),
                    profile_probabilities.get('hard_skipped_or_incorrect', 0.0)
                ]
            )[0]
        
        # Simulate timestamp
        timestamp = start_time + timedelta(minutes=random.randint(1, 1000))
        
        # Add interaction record
        interactions.append({
            'user_id': user_id,
            'item_id': item[item_id_column],
            'event_type': event_type,
            'timestamp': timestamp,
            'difficulty': difficulty,
            'topic': topic,
            'user_profile': profile  # Include the user's current profile
        })

# Create the updated interactions DataFrame
interactions_df = pd.DataFrame(interactions)

# Save the updated dataset
updated_output_path = '/mnt/data/simulated_interactions_with_profiles.csv'
interactions_df.to_csv(updated_output_path, index=False)

import ace_tools as tools; tools.display_dataframe_to_user(name="Simulated Interaction Dataset with Profiles", dataframe=interactions_df)

updated_output_path
