from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from pyspark.sql.streaming import Trigger

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("KafkaConsumerWithSpark") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

kafkaBootstrapServers = "localhost:9092"
kafkaTopic = "transactions"

kafkaOptions = {
    "kafka.bootstrap.servers": kafkaBootstrapServers,
    "subscribe": kafkaTopic,
    "startingOffsets": "earliest"
}

# Définir le schéma pour les données de transaction
transactionSchema = StructType() \
    .add("idTransaction", StringType()) \
    .add("typeTransaction", StringType()) \
    .add("montant", StringType()) \
    .add("devise", StringType()) \
    .add("date", StringType()) \
    .add("lieu", StringType()) \
    .add("moyenPaiement", StringType()) \
    .add("details", StringType()) \
    .add("utilisateur", StringType())

# Créer le flux brut depuis Kafka
rawStream = spark.readStream \
    .format("kafka") \
    .options(**kafkaOptions) \
    .load()

# Analyser les données JSON brutes et appliquer le schéma
transactionsStream = rawStream \
    .selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), transactionSchema).alias("transaction")) \
    .select("transaction.*")

# Définir la requête pour écrire les données dans un fichier Parquet
query = transactionsStream \
    .limit(100) \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://transaction/transactions") \
    .option("checkpointLocation", "s3a://transaction/transactions/checkpoint") \
    .trigger(Trigger.Once()) \
    .start()

# Attendre que la requête se termine
query.awaitTermination()

# Arrêter la session Spark
spark.stop()
