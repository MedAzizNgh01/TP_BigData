from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, from_utc_timestamp, when, sum as spark_sum
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

# Définir le schéma des données de transaction
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

# **1. Transformation des données**
# Convertir USD en EUR (Taux de change : 1 USD = 0.92 EUR)
transactionsStream = transactionsStream \
    .withColumn("EUR", when(col("devise") == "USD", col("montant") * 0.92).otherwise(col("montant")))

# Convertir la colonne 'date' au format date et ajouter une colonne pour le fuseau horaire
transactionsStream = transactionsStream \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("date_with_timezone", from_utc_timestamp(col("date"), "Europe/Paris"))

# Filtrer les transactions de type "error"
transactionsStream = transactionsStream.filter(col("typeTransaction") != "error")

# Supprimer les lignes où 'lieu' est nulle
transactionsStream = transactionsStream.na.drop(subset=["lieu"])

# **2. Agrégation des données**
# Agréger le montant total ('EUR') par devise 
aggregatedStream = transactionsStream \
    .groupBy("devise") \
    .agg(spark_sum("EUR").alias("Somme des transactions"))

# **3. Appliquer .limit(100) aux résultats agrégés**
aggregatedStreamLimited = aggregatedStream.limit(100)

# **4. Écrire les résultats agrégés dans un fichier Parquet (avec .limit(100))**
aggregatedQuery = aggregatedStreamLimited \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://transaction/aggregated_transactions") \
    .option("checkpointLocation", "s3a://transaction/transactions/checkpoint_aggregated") \
    .trigger(Trigger.Once()) \
    .start()

# Attendre que la requête se termine
aggregatedQuery.awaitTermination()

# Arrêter la session Spark
spark.stop()
