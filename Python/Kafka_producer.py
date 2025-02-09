from confluent_kafka import Producer
import datetime as dt
import json
import random
import time
import uuid


def generate_transaction():
    """Generate a random transaction with various attributes."""
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    villes = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", 
              "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon"]
    rues = ["Rue de la République", "Rue de Paris", "Rue Auguste Delaune", "Rue Gustave Courbet", 
            "Rue Fontaine", "Rue Zinedine Zidane", "Rue Gambetta", "Rue du Faubourg Saint-Antoine"]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": dt.datetime.now().isoformat(),
        "lieu": f"{random.choice(rues)}, {random.choice(villes)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(rues)}, {random.choice(villes)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }

    return transaction_data


def send_to_kafka(producer, topic, message):
    """
    Sends a JSON message to a Kafka topic.
    """
    try:
        producer.produce(topic, json.dumps(message).encode('utf-8'))
        producer.flush()  # Ensure the message is sent immediately
        print(f"Sent message to Kafka topic '{topic}': {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")


if __name__ == "__main__":
    # Kafka broker configuration
    kafka_broker = "localhost:9092"  # Replace with your Kafka broker address
    kafka_topic = "transactions"  # Replace with your Kafka topic name

    # Initialize Kafka Producer
    producer = Producer({'bootstrap.servers': kafka_broker})

    # Send transactions to Kafka every second
    try:
        while True:
            transaction = generate_transaction()
            send_to_kafka(producer, kafka_topic, transaction)
            time.sleep(1)  # Wait 1 second before sending the next message
    except KeyboardInterrupt:
        print("\nStopped sending messages.")
    finally:
        print("Closing Kafka Producer.")
