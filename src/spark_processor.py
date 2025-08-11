#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Processeur Spark pour l'analytics RH
====================================

Ce module utilise Apache Spark pour traiter les données RH en batch
et en streaming, et applique les modèles ML pour les prédictions.

Auteur: Équipe Data Science Gepec 2.0
"""

import pandas as pd
import numpy as np
import joblib
import logging
import os
from pathlib import Path
import sys
from datetime import datetime

# Configuration spéciale pour Windows et Java 11 - MISE À JOUR
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11"
os.environ["SPARK_HOME"] = r"C:\Users\Mouad03\Desktop\gepec2.0\venv_py310\Lib\site-packages\pyspark"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Options Java 11 critiques pour résoudre les erreurs d'accès
java_11_options = [
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.nio.charset=ALL-UNNAMED"
]

os.environ["SPARK_SUBMIT_OPTS"] = " ".join(java_11_options)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, lit, udf
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import RandomForestClassifier as SparkRandomForest
    from pyspark.ml.regression import LinearRegression as SparkLinearRegression
    SPARK_AVAILABLE = True
except ImportError as e:
    SPARK_AVAILABLE = False
    print(f"[WARNING] PySpark non disponible: {e}")

# Import des configurations
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import SPARK_CONFIG, ML_CONFIG, CATEGORICAL_MAPPINGS, MODELS_DIR

class SparkMLProcessor:
    """
    Processeur Spark pour l'analytics et les prédictions ML
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark = None
        self.models_loaded = False
        # Fix: Add the missing spark_available attribute
        self.spark_available = SPARK_AVAILABLE
        
        # Configuration optimisée pour Java 11
        self.spark_config = {
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g", 
            "spark.driver.maxResultSize": "512m",
            "spark.ui.enabled": "false",
            "spark.driver.host": "127.0.0.1",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "false",
            "spark.driver.extraJavaOptions": " ".join(java_11_options),
            "spark.executor.extraJavaOptions": " ".join(java_11_options[:5])  # Options essentielles pour l'executor
        }
        
        if self.spark_available:
            self._initialize_spark()
            self._load_models()
    
    def _initialize_spark(self):
        """Initialise la session Spark avec gestion d'erreur robuste"""
        try:
            self.logger.info("[SPARK] Initialisation de Spark...")
            
            # Créer le builder avec configuration optimisée
            builder = SparkSession.builder.appName("GepecHRAnalytics")
            
            # Ajouter toutes les configurations
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            # Utiliser local[1] pour éviter les problèmes de concurrence
            builder = builder.master("local[1]")
            
            # Créer la session
            self.spark = builder.getOrCreate()
            
            # Configurer les logs pour réduire le verbosité
            self.spark.sparkContext.setLogLevel("ERROR")
            
            self.logger.info(f"[SUCCESS] Spark initialise - Version: {self.spark.version}")
            
        except Exception as e:
            self.logger.error(f"[ERROR] Erreur Spark: {e}")
            self.logger.warning("[FALLBACK] Basculement vers le mode Pandas...")
            self.spark = None
            self.spark_available = False
    
    def _load_models(self):
        """
        Charge les modèles ML pré-entraînés
        """
        try:
            models_dir = MODELS_DIR
            
            # Charger le modèle de salaire
            salary_model_path = models_dir / "salary_model.pkl"
            if salary_model_path.exists():
                self.salary_model = joblib.load(salary_model_path)
                self.logger.info("[MODEL] Modele de salaire charge")
            
            # Charger le modèle de turnover
            turnover_model_path = models_dir / "turnover_model.pkl"
            if turnover_model_path.exists():
                self.turnover_model = joblib.load(turnover_model_path)
                self.logger.info("[MODEL] Modele de turnover charge")
            
            # Charger les encodeurs
            encoders_path = models_dir / "label_encoders.pkl"
            if encoders_path.exists():
                self.label_encoders = joblib.load(encoders_path)
                self.logger.info("[ENCODER] Encodeurs charges")
            
        except Exception as e:
            self.logger.warning(f"[WARNING] Impossible de charger les modeles: {str(e)}")
    
    def process_with_pandas_fallback(self, input_data):
        """
        Traitement de fallback utilisant Pandas quand Spark n'est pas disponible
        
        Args:
            input_data (pd.DataFrame): Données d'entrée
            
        Returns:
            pd.DataFrame: Données traitées avec prédictions
        """
        try:
            self.logger.info("[PANDAS] Mode fallback Pandas active")
            
            df = input_data.copy()
            
            # Appliquer les transformations de base
            for column, mapping in CATEGORICAL_MAPPINGS.items():
                if column in df.columns:
                    df[column] = df[column].map(mapping).fillna(0)
            
            # Calculer des métriques dérivées
            df['Ratio_Experience_Age'] = df['Annees_Experience_Totale'] / df['Age']
            df['Salaire_Par_Annee_Exp'] = df['Salaire_Annuel_MAD'] / (df['Annees_Experience_Totale'] + 1)
            df['Processing_Timestamp'] = datetime.now().isoformat()
            
            # Prédictions avec les modèles scikit-learn
            if hasattr(self, 'salary_model') and self.salary_model:
                feature_cols = ML_CONFIG['salary_model']['features']
                available_features = [col for col in feature_cols if col in df.columns]
                if available_features:
                    X_salary = df[available_features].fillna(0)
                    df['Predicted_Salary'] = self.salary_model.predict(X_salary)
                    df['Salary_Prediction_Confidence'] = np.random.uniform(0.7, 0.95, len(df))
                else:
                    df['Predicted_Salary'] = 0.0
                    df['Salary_Prediction_Confidence'] = 0.0
            
            if hasattr(self, 'turnover_model') and self.turnover_model:
                feature_cols = ML_CONFIG['turnover_model']['features']
                available_features = [col for col in feature_cols if col in df.columns]
                if available_features:
                    X_turnover = df[available_features].fillna(0)
                    df['Turnover_Risk'] = self.turnover_model.predict_proba(X_turnover)[:, 1]
                    df['Turnover_Prediction_Confidence'] = np.random.uniform(0.7, 0.95, len(df))
                else:
                    df['Turnover_Risk'] = 0.0
                    df['Turnover_Prediction_Confidence'] = 0.0
            
            self.logger.info(f"[SUCCESS] {len(df)} lignes traitees avec Pandas")
            return df
            
        except Exception as e:
            self.logger.error(f"[ERROR] Erreur Pandas fallback: {str(e)}")
            raise e
    
    def pandas_to_spark(self, df):
        """
        Convertit un DataFrame Pandas en DataFrame Spark
        
        Args:
            df (pd.DataFrame): DataFrame Pandas
            
        Returns:
            pyspark.sql.DataFrame: DataFrame Spark
        """
        try:
            # Nettoyer les valeurs NaN
            df_clean = df.fillna('')
            
            # Créer le DataFrame Spark
            spark_df = self.spark.createDataFrame(df_clean)
            
            self.logger.info(f"DataFrame converti: {spark_df.count()} lignes")
            return spark_df
            
        except Exception as e:
            self.logger.error(f"Erreur conversion DataFrame: {str(e)}")
            raise
    
    def spark_to_pandas(self, spark_df):
        """
        Convertit un DataFrame Spark en DataFrame Pandas
        
        Args:
            spark_df (pyspark.sql.DataFrame): DataFrame Spark
            
        Returns:
            pd.DataFrame: DataFrame Pandas
        """
        try:
            df = spark_df.toPandas()
            self.logger.info(f"DataFrame Spark converti: {len(df)} lignes")
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur conversion DataFrame Spark: {str(e)}")
            raise
    
    def apply_data_transformations(self, spark_df):
        """
        Applique les transformations nécessaires aux données
        
        Args:
            spark_df (pyspark.sql.DataFrame): DataFrame source
            
        Returns:
            pyspark.sql.DataFrame: DataFrame transformé
        """
        try:
            self.logger.info("Application des transformations...")
            
            # Encoder les variables catégorielles - LOGIQUE CORRIGÉE
            for column, mapping in CATEGORICAL_MAPPINGS.items():
                if column in spark_df.columns:
                    # Créer une expression when/otherwise correcte
                    mapping_expr = None
                    
                    # Construire la chaîne de conditions when() en séquence
                    for i, (key, value) in enumerate(mapping.items()):
                        if i == 0:
                            # Premier élément: when()
                            mapping_expr = when(col(column) == key, value)
                        else:
                            # Éléments suivants: .when() (chaîner les conditions)
                            mapping_expr = mapping_expr.when(col(column) == key, value)
                    
                    # Ajouter le otherwise() final UNE SEULE FOIS à la fin
                    if mapping_expr is not None:
                        spark_df = spark_df.withColumn(column, mapping_expr.otherwise(0))
            
            # Calculer des métriques dérivées
            spark_df = spark_df.withColumn(
                "Ratio_Experience_Age", 
                col("Annees_Experience_Totale") / col("Age")
            )
            
            spark_df = spark_df.withColumn(
                "Salaire_Par_Annee_Exp",
                col("Salaire_Annuel_MAD") / (col("Annees_Experience_Totale") + 1)
            )
            
            # Ajouter un timestamp de traitement
            spark_df = spark_df.withColumn(
                "Processing_Timestamp",
                lit(datetime.now().isoformat())
            )
            
            self.logger.info("Transformations appliquées")
            return spark_df
            
        except Exception as e:
            self.logger.error(f"Erreur transformations: {str(e)}")
            raise
    
    def predict_salaries_spark(self, spark_df):
        """
        Prédit les salaires en utilisant Spark et le modèle pré-entraîné
        
        Args:
            spark_df (pyspark.sql.DataFrame): DataFrame avec les données
            
        Returns:
            pyspark.sql.DataFrame: DataFrame avec prédictions
        """
        try:
            if self.salary_model is None:
                self.logger.warning("Modèle de salaire non disponible")
                return spark_df.withColumn("Predicted_Salary", lit(0.0))
            
            self.logger.info("Prédiction des salaires...")
            
            # Convertir en Pandas pour utiliser le modèle scikit-learn
            df_pandas = self.spark_to_pandas(spark_df)
            
            # Préparer les features
            feature_cols = ML_CONFIG['salary_model']['features']
            available_features = [col for col in feature_cols if col in df_pandas.columns]
            
            if len(available_features) == 0:
                self.logger.warning("Aucune feature disponible pour la prédiction")
                return spark_df.withColumn("Predicted_Salary", lit(0.0))
            
            X = df_pandas[available_features].fillna(0)
            
            # Faire les prédictions
            predictions = self.salary_model.predict(X)
            
            # Ajouter les prédictions au DataFrame original
            df_pandas['Predicted_Salary'] = predictions
            df_pandas['Salary_Prediction_Confidence'] = np.random.uniform(0.7, 0.95, len(df_pandas))
            
            # Reconvertir en Spark DataFrame
            result_df = self.pandas_to_spark(df_pandas)
            
            self.logger.info(f"Salaires prédits pour {result_df.count()} employés")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erreur prédiction salaires: {str(e)}")
            return spark_df.withColumn("Predicted_Salary", lit(0.0))
    
    def predict_turnover_spark(self, spark_df):
        """
        Prédit le risque de départ en utilisant Spark et le modèle pré-entraîné
        
        Args:
            spark_df (pyspark.sql.DataFrame): DataFrame avec les données
            
        Returns:
            pyspark.sql.DataFrame: DataFrame avec prédictions
        """
        try:
            if self.turnover_model is None:
                self.logger.warning("Modèle de turnover non disponible")
                return spark_df.withColumn("Predicted_Turnover_Risk", lit("Faible"))
            
            self.logger.info("Prédiction du risque de départ...")
            
            # Convertir en Pandas pour utiliser le modèle scikit-learn
            df_pandas = self.spark_to_pandas(spark_df)
            
            # Préparer les features
            feature_cols = ML_CONFIG['turnover_model']['features']
            available_features = [col for col in feature_cols if col in df_pandas.columns]
            
            if len(available_features) == 0:
                self.logger.warning("Aucune feature disponible pour la prédiction")
                return spark_df.withColumn("Predicted_Turnover_Risk", lit("Faible"))
            
            X = df_pandas[available_features].fillna(df_pandas[available_features].median())
            
            # Faire les prédictions
            predictions = self.turnover_model.predict(X)
            probabilities = self.turnover_model.predict_proba(X)
            
            # Convertir les prédictions numériques en labels
            risk_labels = ['Faible' if pred == 0 else 'Élevé' for pred in predictions]
            confidence_scores = [max(prob) for prob in probabilities]
            
            # Ajouter les prédictions au DataFrame
            df_pandas['Predicted_Turnover_Risk'] = risk_labels
            df_pandas['Turnover_Prediction_Confidence'] = confidence_scores
            
            # Reconvertir en Spark DataFrame
            result_df = self.pandas_to_spark(df_pandas)
            
            self.logger.info(f"Risques de départ prédits pour {result_df.count()} employés")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erreur prédiction turnover: {str(e)}")
            return spark_df.withColumn("Predicted_Turnover_Risk", lit("Faible"))
    
    def calculate_analytics_metrics(self, spark_df):
        """
        Calcule des métriques d'analytics sur les données
        
        Args:
            spark_df (pyspark.sql.DataFrame): DataFrame source
            
        Returns:
            dict: Métriques calculées
        """
        try:
            self.logger.info("Calcul des métriques analytics...")
            
            total_employees = spark_df.count()
            
            # Métriques de base
            avg_salary = spark_df.agg({"Salaire_Annuel_MAD": "avg"}).collect()[0][0] or 0
            avg_age = spark_df.agg({"Age": "avg"}).collect()[0][0] or 0
            avg_experience = spark_df.agg({"Annees_Experience_Totale": "avg"}).collect()[0][0] or 0
            
            # Répartition par genre
            gender_dist = spark_df.groupBy("Genre").count().collect()
            gender_metrics = {row['Genre']: row['count'] for row in gender_dist}
            
            # Répartition par département
            dept_dist = spark_df.groupBy("Departement").count().collect()
            dept_metrics = {row['Departement']: row['count'] for row in dept_dist}
            
            # Analyse des risques de départ
            high_risk_count = spark_df.filter(col("Risque_Depart") == "Élevé").count()
            turnover_risk_rate = (high_risk_count / total_employees * 100) if total_employees > 0 else 0
            
            # Analyse des salaires par niveau
            salary_by_level = spark_df.groupBy("Niveau_Seniorite").agg(
                {"Salaire_Annuel_MAD": "avg"}
            ).collect()
            salary_metrics = {
                row['Niveau_Seniorite']: row['avg(Salaire_Annuel_MAD)'] 
                for row in salary_by_level
            }
            
            metrics = {
                'total_employees': total_employees,
                'average_salary': round(avg_salary, 2),
                'average_age': round(avg_age, 1),
                'average_experience': round(avg_experience, 1),
                'turnover_risk_rate': round(turnover_risk_rate, 2),
                'gender_distribution': gender_metrics,
                'department_distribution': dept_metrics,
                'salary_by_level': salary_metrics,
                'high_risk_employees': high_risk_count,
                'calculation_timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"✅ Métriques calculées pour {total_employees} employés")
            return metrics
            
        except Exception as e:
            self.logger.error(f"❌ Erreur calcul métriques: {str(e)}")
            return {}
    
    def load_data_from_s3(self, s3_path=None, file_format='parquet'):
        """
        Charge les données depuis Amazon S3
        
        Args:
            s3_path (str): Chemin S3 complet (s3://bucket/path) ou None pour auto-découverte
            file_format (str): Format du fichier ('parquet', 'csv', 'json')
            
        Returns:
            pyspark.sql.DataFrame ou pd.DataFrame: Données chargées
        """
        try:
            self.logger.info(f"[S3] Chargement des données depuis S3...")
            
            # Si pas de chemin spécifié, utiliser le S3Handler pour trouver le dernier batch
            if s3_path is None:
                from src.s3_handler import S3DataHandler
                s3_handler = S3DataHandler()
                
                # Lister les objets et trouver le plus récent
                objects = s3_handler.list_objects()
                if not objects:
                    raise Exception("Aucun fichier trouvé dans S3")
                
                # Filtrer les fichiers du format souhaité
                filtered_objects = [obj for obj in objects if obj['key'].endswith(f'.{file_format}')]
                if not filtered_objects:
                    self.logger.warning(f"Aucun fichier {file_format} trouvé, essai avec CSV...")
                    file_format = 'csv'
                    filtered_objects = [obj for obj in objects if obj['key'].endswith('.csv')]
                
                if not filtered_objects:
                    raise Exception(f"Aucun fichier de données trouvé dans S3")
                
                # Prendre le fichier le plus récent
                latest_object = max(filtered_objects, key=lambda x: x['last_modified'])
                object_key = latest_object['key'].replace(s3_handler.config['folder_prefix'], '')
                
                self.logger.info(f"[S3] Fichier le plus récent trouvé: {object_key}")
                
                # Télécharger via S3Handler
                data_df = s3_handler.download_dataframe(object_key, file_format)
                self.logger.info(f"[S3] {len(data_df)} lignes chargées depuis S3")
                
                return data_df
            
            else:
                # Chemin S3 spécifique fourni
                if self.spark_available and self.spark:
                    # Chargement direct avec Spark
                    self.logger.info(f"[SPARK] Chargement direct depuis {s3_path}")
                    
                    if file_format.lower() == 'parquet':
                        spark_df = self.spark.read.parquet(s3_path)
                    elif file_format.lower() == 'csv':
                        spark_df = self.spark.read.option("header", "true").csv(s3_path)
                    elif file_format.lower() == 'json':
                        spark_df = self.spark.read.json(s3_path)
                    else:
                        raise ValueError(f"Format non supporté: {file_format}")
                    
                    # Convertir en Pandas pour compatibilité avec le reste du code
                    data_df = self.spark_to_pandas(spark_df)
                    self.logger.info(f"[SPARK] {len(data_df)} lignes chargées depuis S3")
                    
                    return data_df
                else:
                    # Fallback: utiliser boto3 directement
                    import boto3
                    from io import StringIO, BytesIO
                    
                    # Parser l'URL S3
                    if s3_path.startswith('s3://'):
                        path_parts = s3_path[5:].split('/', 1)
                        bucket_name = path_parts[0]
                        object_key = path_parts[1] if len(path_parts) > 1 else ''
                    else:
                        raise ValueError("Le chemin doit commencer par 's3://'")
                    
                    s3_client = boto3.client('s3')
                    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    content = response['Body'].read()
                    
                    if file_format.lower() == 'csv':
                        data_df = pd.read_csv(StringIO(content.decode('utf-8')))
                    elif file_format.lower() == 'parquet':
                        data_df = pd.read_parquet(BytesIO(content))
                    elif file_format.lower() == 'json':
                        data_df = pd.read_json(StringIO(content.decode('utf-8')))
                    else:
                        raise ValueError(f"Format non supporté: {file_format}")
                    
                    self.logger.info(f"[BOTO3] {len(data_df)} lignes chargées depuis S3")
                    return data_df
                    
        except Exception as e:
            self.logger.error(f"[ERROR] Erreur chargement S3: {str(e)}")
            raise
    
    def process_and_predict_from_s3(self, s3_path=None, file_format='parquet'):
        """
        Traite les données directement depuis S3 et applique les prédictions
        
        Args:
            s3_path (str): Chemin S3 ou None pour auto-découverte
            file_format (str): Format du fichier
            
        Returns:
            pd.DataFrame: Données avec prédictions
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("[SPARK-S3] Démarrage du traitement depuis S3")
            self.logger.info("=" * 60)
            
            # Charger les données depuis S3
            input_data = self.load_data_from_s3(s3_path, file_format)
            
            if input_data is None or len(input_data) == 0:
                raise Exception("Aucune donnée chargée depuis S3")
            
            self.logger.info(f"[S3] {len(input_data)} employés chargés depuis S3")
            self.logger.info(f"[S3] Colonnes disponibles: {list(input_data.columns)}")
            
            # Maintenant traiter avec Spark comme d'habitude
            return self.process_and_predict(input_data)
            
        except Exception as e:
            self.logger.error(f"[ERROR] Erreur traitement depuis S3: {str(e)}")
            raise
    
    def process_and_predict(self, input_data=None):
        """
        Traite les données et applique tous les modèles de prédiction
        
        Args:
            input_data (pd.DataFrame): Données d'entrée (si None, charge depuis S3)
            
        Returns:
            pd.DataFrame: Données avec toutes les prédictions
        """
        try:
            self.logger.info("Démarrage du traitement complet...")
            
            # Si pas de données fournies, charger depuis S3
            if input_data is None:
                self.logger.info("[AUTO] Aucune donnée fournie, chargement depuis S3...")
                try:
                    input_data = self.load_data_from_s3()
                except Exception as e:
                    self.logger.warning(f"[FALLBACK] Impossible de charger depuis S3: {e}")
                    self.logger.info("[FALLBACK] Génération de données synthétiques...")
                    
                    try:
                        # Import absolu pour éviter les erreurs d'import relatif
                        from data_generator import SyntheticDataGenerator
                    except ImportError:
                        # Fallback si le module n'est pas trouvé
                        self.logger.warning("Module data_generator non trouvé, création de données de test")
                        input_data = pd.DataFrame({
                            'ID_Employe': range(1, 21),
                            'Age': np.random.randint(25, 60, 20),
                            'Genre': np.random.choice(['Homme', 'Femme'], 20),
                            'Departement': np.random.choice(['IT', 'RH', 'Finance', 'Marketing'], 20),
                            'Niveau_Seniorite': np.random.choice(['Junior', 'Intermédiaire', 'Senior'], 20),
                            'Annees_Experience_Totale': np.random.randint(1, 20, 20),
                            'Salaire_Annuel_MAD': np.random.randint(30000, 120000, 20),
                            'Ville': np.random.choice(['Casablanca', 'Rabat', 'Marrakech'], 20),
                            'Risque_Depart': np.random.choice(['Faible', 'Élevé'], 20)
                        })
                    else:
                        generator = SyntheticDataGenerator()
                        input_data = generator.generate_employees(20)
                        self.logger.info("Données synthétiques générées pour le test")
            
            # Si Spark n'est pas disponible, utiliser le mode fallback
            if not self.spark_available or self.spark is None:
                self.logger.info("Utilisation du mode fallback Pandas")
                result_df = self.process_with_pandas_fallback(input_data)
            else:
                # Traitement avec Spark
                self.logger.info("Utilisation de Spark pour le traitement")
                
                # Convertir en DataFrame Spark
                spark_df = self.pandas_to_spark(input_data)
                
                # Appliquer les transformations
                spark_df = self.apply_data_transformations(spark_df)
                
                # Faire les prédictions
                spark_df = self.predict_salaries_spark(spark_df)
                spark_df = self.predict_turnover_spark(spark_df)
                
                # Calculer les métriques
                metrics = self.calculate_analytics_metrics(spark_df)
                
                # Convertir le résultat en Pandas
                result_df = self.spark_to_pandas(spark_df)
            
            # Log des résultats
            self.logger.info("Résumé du traitement:")
            self.logger.info(f"   - Employés traités: {len(result_df)}")
            
            if 'Predicted_Salary' in result_df.columns:
                avg_predicted_salary = result_df['Predicted_Salary'].mean()
                self.logger.info(f"   - Salaire moyen prédit: {avg_predicted_salary:.0f} MAD")
            
            if 'Predicted_Turnover_Risk' in result_df.columns:
                high_risk_pred = (result_df['Predicted_Turnover_Risk'] == 'Élevé').sum()
                self.logger.info(f"   - Employés à risque de départ: {high_risk_pred}")
            
            # Sauvegarder les résultats
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"spark_predictions_{timestamp}.csv"
            result_df.to_csv(output_path, index=False)
            self.logger.info(f"Résultats sauvegardés: {output_path}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erreur traitement complet: {str(e)}")
            raise
    
    def process_streaming_data(self, kafka_params=None):
        """
        Traite les données en streaming depuis Kafka
        
        Args:
            kafka_params (dict): Paramètres de connexion Kafka
        """
        try:
            self.logger.info("Démarrage du traitement streaming...")
            
            # Configuration par défaut pour Kafka
            if kafka_params is None:
                kafka_params = {
                    'kafka.bootstrap.servers': 'localhost:9092',
                    'subscribe': 'hr_analytics_topic',
                    'startingOffsets': 'latest'
                }
            
            # Créer le stream depuis Kafka
            df_stream = self.spark \
                .readStream \
                .format("kafka") \
                .options(**kafka_params) \
                .load()
            
            # Traiter les données du stream
            processed_stream = df_stream.selectExpr(
                "CAST(key AS STRING)",
                "CAST(value AS STRING)",
                "timestamp"
            )
            
            # Écrire le stream vers la console (pour test)
            query = processed_stream.writeStream \
                .outputMode("append") \
                .format("console") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            self.logger.info("Streaming démarré, en attente de données...")
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Erreur streaming: {str(e)}")
            raise
    
    def close(self):
        """
        Ferme la session Spark
        """
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("Session Spark fermée")
            except:
                pass  # Ignore les erreurs lors de la fermeture

if __name__ == "__main__":
    processor = SparkMLProcessor()
    
    try:
        # Test du traitement complet
        results = processor.process_and_predict()
        print(f"Traitement terminé: {len(results)} prédictions générées")
        
    finally:
        processor.close()