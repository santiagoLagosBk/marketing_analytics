import subprocess

# Define the container ID and the Kafka topics
container_id = "11f685c2037d"
topics = ["customers_topic", "events_topic", "events-country-aggregation"]
bootstrap_server = "broker:29092"

# Define the command to delete and recreate a Kafka topic
def kafka_topic_command(topic, action):
    return f"kafka-topics --{action} --topic {topic} --bootstrap-server {bootstrap_server}"


# Loop over the topics
for topic in topics:
    # Delete the topic
    delete_command = kafka_topic_command(topic, "delete")
    subprocess.run(["docker", "exec", "-i", container_id, "bash", "-c", delete_command])

    # Recreate the topic
    create_command = kafka_topic_command(topic, "create")
    subprocess.run(["docker", "exec", "-i", container_id, "bash", "-c", create_command])

print("The Kafka topics have been deleted and recreated successfully.")
