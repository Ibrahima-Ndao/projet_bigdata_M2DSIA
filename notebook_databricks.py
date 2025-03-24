# Databricks notebook source
# MAGIC %md
# MAGIC # Partie 1 : Analyse avec PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chargement des données dans Spark DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


# COMMAND ----------


spark = SparkSession.builder.appName("VehicleCrashAnalysis").getOrCreate()

# Charge le fichier CSV
data_path = "dbfs:/FileStore/tables/Motor_Vehicle_Crashes___Vehicle_Information__Three_Year_Window_1_.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

df.columns

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examiner les colonnes et types :

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Résumé statistique :

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nettoyage des Données

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Identifier et gérer les valeurs manquantes :

# COMMAND ----------

# Compter les valeurs nulles par colonne
missing_values = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
missing_values.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supprimer les valeurs manquantes

# COMMAND ----------

# Supprimer les lignes où des valeurs sont manquantes dans certaines colonnes
df_clean = df.dropna(subset=["Vehicle Year", "State of Registration", "Number of Occupants", "Engine Cylinders", "Vehicle Make", "Event Type", "Partial VIN"])
df_clean.display()

# COMMAND ----------

df_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2- Identification des doublons :

# COMMAND ----------

# Trouver les doublons
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Nombre de doublons : {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution des types de véhicules : Quantifier les différents types de véhicules impliqués.

# COMMAND ----------

vehicle_count = df_clean.groupBy("Vehicle Body Type").count().orderBy("count", ascending=False)
vehicle_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Répartition par année : Identifier les tendances annuelles sur les accidents.

# COMMAND ----------

year_distribution = df_clean.groupBy("Year").count().orderBy("Year")
year_distribution.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Nombre d'occupants : Étudier la corrélation entre le nombre d'occupants et les accidents.

# COMMAND ----------

occupants_distribution = df_clean.groupBy("Number of Occupants").count().orderBy("count", ascending=False)
occupants_distribution.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Facteur contributif principal : Identifier les facteurs contribuant le plus souvent aux accidents

# COMMAND ----------

contributing_factors = df_clean.groupBy("Contributing Factor 1").count().orderBy("count", ascending=False)
contributing_factors.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Facteurs multiples : Examiner les interactions entre plusieurs facteurs.

# COMMAND ----------

factor_interactions = df_clean.groupBy("Contributing Factor 1", "Contributing Factor 2").count().orderBy("count", ascending=False)
factor_interactions.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### État d'enregistrement : Comparer le nombre d'accidents par État.

# COMMAND ----------

state_distribution = df_clean.groupBy("State of Registration").count().orderBy("count", ascending=False)
state_distribution.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Direction de déplacement : Étudier si certains trajets sont plus risqués.

# COMMAND ----------

direction_travel = df_clean.groupBy("Direction of Travel").count().orderBy("count", ascending=False)
direction_travel.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Âge des véhicules : Étudier la corrélation entre l'année des véhicules et les accidents.

# COMMAND ----------

vehicle_age = df_clean.withColumn("Vehicle Age", 2025 - col("Vehicle Year"))
vehicle_age.groupBy("Vehicle Age").count().orderBy("count", ascending=False).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Carburant utilisé : Examiner si certains types de carburants sont liés à des accidents.

# COMMAND ----------

fuel_analysis = df_clean.groupBy("Fuel Type").count().orderBy("count", ascending=False)
fuel_analysis.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cylindres du moteur : Vérifier si les caractéristiques du moteur influencent les collisions.

# COMMAND ----------

engine_analysis = df_clean.groupBy("Engine Cylinders").count().orderBy("count", ascending=False)
engine_analysis.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Actions avant l'accident : Étudier les comportements des conducteurs avant l'incident.

# COMMAND ----------

action_analysis = df_clean.groupBy("Action Prior to Accident").count().orderBy("count", ascending=False)
action_analysis.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Type d'événement : Identifier les événements les plus fréquents (collision, dérapage, etc.).

# COMMAND ----------

event_type_analysis = df_clean.groupBy("Event Type").count().orderBy("count", ascending=False)
event_type_analysis.display()

