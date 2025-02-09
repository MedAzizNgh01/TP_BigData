from kafka import KafkaConsumer
import json
import pandas as pd

# Kafka broker address and topic name
kafka_broker = "localhost:9092"  # Ensure this matches your Kafka setup
kafka_topic = "transactions"

# Initialize KafkaConsumer
try:
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        auto_offset_reset='earliest',  # Start reading from the beginning of the topic
        enable_auto_commit=True,       # Automatically commit offsets
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
    )
    print(f"Connected to Kafka topic: {kafka_topic}")

    # Create an empty list to store the transaction data
    transactions_data = []

    # Continuously listen for messages
    for message in consumer:
        transaction = message.value  # Get the actual message content
        transactions_data.append(transaction)  # Add the transaction data to the list

        # Print the transaction for verification
        print(f"Received transaction: {transaction}")

        # Stop after collecting 1000 transactions (or any other condition you want)
        if len(transactions_data) >= 1000:
            break

except Exception as e:
    print(f"Failed to consume messages from Kafka: {e}")
finally:
    consumer.close()

# Convert the list of transactions into a Pandas DataFrame
df = pd.DataFrame(transactions_data)

# Display the DataFrame to verify
print(df)

# Save the DataFrame to CSV
df.to_csv('transactions.csv', index=False)

