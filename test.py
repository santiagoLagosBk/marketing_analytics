import random

# Define the list of items
items = ["sent_event", "delivered_event", "opened_event", "clicked_event", "forwarded_event", "spam_event", "hard_bounce_event"]

# Define the probabilities for each item
probabilities = [0.2, 0.2, 0.6, 0.2, 0.2]  # Example probabilities, summing up to 1

# Select items based on the specified probabilities
selected_items = random.choices(items[:len(probabilities)], weights=probabilities, k=1)

print(selected_items)