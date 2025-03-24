# Analyse des données avec Spark et Polars

## **Contexte**

L'objectif de ce projet est de réaliser une analyse approfondie d'un jeu de données en utilisant deux technologies distinctes : PySpark sur Databricks et Polars dans un environnement local. En parallèle, une plateforme de données a été mise en place avec Docker pour gérer les sources de données et le stockage des objets.

Le jeu de données utilisé provient de [Motor Vehicle Crashes - Vehicle Information Three Years](https://data.ny.gov/Transportation/Motor-Vehicle-Crashes-Vehicle-Information-Three-Ye/xe9x-a24f/about_data) et contient des informations sur les accidents de voiture sur une période de trois ans.

---

## **Démarche**

### **1. Analyse des données avec PySpark sur Databricks**

1. Activation de la fonctionnalité **DBFS** (Databricks File System) pour charger le jeu de données sur Databricks.
2. Création d'un **notebook PySpark** dans Databricks pour effectuer une analyse exhaustive des données :
   - Exploration et nettoyage des données.
   - Calcul de statistiques descriptives.
   - Visualisation des tendances et des corrélations entre les variables.
3. Sauvegarde des résultats intermédiaires pour référence future.

### **2. Mise en place d'une plateforme de données avec Polars**

#### **Configuration de PostgreSQL et Docker**

1. Création d'un fichier `insert_data.py` :
   - Développement de scripts Python pour insérer le jeu de données dans une base de données PostgreSQL.
2. Utilisation de Docker pour déployer PostgreSQL :
   - Création et exécution des conteneurs nécessaires.
   - Importation du jeu de données dans PostgreSQL à partir du script Python.

#### **Analyse des données avec Polars**

1. Utilisation de **VS Code** pour créer un notebook en local.
2. Connexion à la base de données PostgreSQL depuis Polars via une source de données.
3. Répétition des mêmes analyses que celles réalisées sur Databricks :
   - Exploration et nettoyage des données.
   - Calcul de statistiques descriptives.
   - Visualisation des tendances et des corrélations.

### **3. Intégration avec MinIO pour le stockage des objets**

1. Création d'un fichier **Docker Compose** pour déployer MinIO en tant que solution de stockage d'objets.
2. Sauvegarde des données analysées dans MinIO pour une gestion efficace et sécurisée.

---

## **Résumé**

Ce projet met en évidence :

- La puissance de **Databricks et PySpark** pour le traitement distribué des données.
- L'efficacité de **Polars** pour des analyses rapides dans un environnement local.
- L'intégration harmonieuse de Docker, PostgreSQL et MinIO pour construire une plateforme de données scalable et robuste.

---

## **Structure des fichiers**

Voici les principaux fichiers et dossiers de ce projet :

- **Notebook Databricks :** Analyse des données avec PySpark.
- **insert_data.py :** Script Python pour insérer les données dans PostgreSQL.
- **Notebook VS Code :** Analyse des données avec Polars.
- **docker-compose.yml :** Configuration pour déployer MinIO.
- **Documents de comparaison :** Analyse détaillée des performances entre Spark et Polars.
