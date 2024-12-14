# Databricks notebook source
# Configurer les paramètres pour Bronze et Gold
storage_name = "dlkefrei91320"
access_key = "9+pTAab8hreKzlVRl0opcGYSfXP4qbDYahf2QlQ7xqNSQcooJNdN0/VJNw6WqETE5Hu/zCK5tuPs+AStpCqyTQ=="

# Bronze
container_bronze = "ds-bronze"
mount_point_bronze = "/mnt/ds-bronze"

# Gold
container_gold = "ds-gold"
mount_point_gold = "/mnt/ds-gold"

# Configurer les sources
source_bronze = f"wasbs://{container_bronze}@{storage_name}.blob.core.windows.net"
source_gold = f"wasbs://{container_gold}@{storage_name}.blob.core.windows.net"

configs = {"fs.azure.account.key." + storage_name + ".blob.core.windows.net": access_key}

# Vérifier si Bronze est monté
if mount_point_bronze not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.mount(
        source=source_bronze,
        mount_point=mount_point_bronze,
        extra_configs=configs
    )
    print(f"Bronze monté sur : {mount_point_bronze}")
else:
    print(f"Bronze est déjà monté sur : {mount_point_bronze}")

# Vérifier si Gold est monté
if mount_point_gold not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.mount(
        source=source_gold,
        mount_point=mount_point_gold,
        extra_configs=configs
    )
    print(f"Gold monté sur : {mount_point_gold}")
else:
    print(f"Gold est déjà monté sur : {mount_point_gold}")

# Lister les fichiers disponibles dans Bronze
print("Fichiers disponibles dans ds-bronze :")
display(dbutils.fs.ls(mount_point_bronze))


# COMMAND ----------

# Charger les données brutes depuis Bronze
games_df = spark.read.format("csv").option("header", "true").load(f"{mount_point_bronze}/games/")

# Afficher un aperçu des données brutes
print("Données brutes de 'games':")
games_df.show(10)


# COMMAND ----------

# Nettoyage des données 'games'

# 1. Supprimer les colonnes inutiles
columns_to_drop = ['url', 'aggregate']
games_cleaned = games_df.drop(*columns_to_drop)

# 2. Supprimer les lignes avec des valeurs nulles dans les colonnes critiques
columns_critical = ['home_club_name', 'away_club_name', 'home_club_goals', 'away_club_goals']
games_cleaned = games_cleaned.dropna(subset=columns_critical)

# 3. Convertir les dates en format datetime
from pyspark.sql.functions import to_date
games_cleaned = games_cleaned.withColumn("date", to_date(games_cleaned["date"], "yyyy-MM-dd"))

# 4. Supprimer les doublons
games_cleaned = games_cleaned.dropDuplicates()

# 5. Trier les données par date et home_club_name
games_cleaned = games_cleaned.orderBy(["date", "home_club_name"])


# COMMAND ----------

# Afficher les données nettoyées
print("Données nettoyées et triées de 'games':")
games_cleaned.show(10)


# COMMAND ----------

# Sauvegarder les données nettoyées en un seul fichier dans Gold
temp_path = f"{mount_point_gold}/games_cleaned_temp/"
final_path = f"{mount_point_gold}/games_cleaned/games_cleaned.csv"

# Réduire à une seule partition et sauvegarder temporairement
games_cleaned.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(temp_path)

# Identifier et renommer le fichier généré
csv_file = [f.path for f in dbutils.fs.ls(temp_path) if f.name.endswith(".csv")][0]
dbutils.fs.mv(csv_file, final_path)

print(f"Fichier unique sauvegardé dans : {final_path}")


# COMMAND ----------

# Lister les fichiers sauvegardés dans Gold
print("Contenu de ds-gold après sauvegarde :")
display(dbutils.fs.ls(f"{mount_point_gold}/games_cleaned/"))

