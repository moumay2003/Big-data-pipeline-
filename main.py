#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline HR Analytics - Projet Gepec 2.0
=========================================

Ce script principal orchestre le pipeline complet d'analyse RH :
1. Generation de donnees RH synthetiques
2. Streaming via Kafka
3. Stockage vers S3
4. Recuperation et traitement avec Spark
5. Predictions avec modeles pre-entraines

Flux de donnees: Generation → Kafka → S3 → Spark → Predictions

Auteur: Equipe Data Science
Date: Mai 2025
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pathlib import Path
import time

# Configuration des chemins
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"
LOGS_DIR = PROJECT_ROOT / "logs"

# Creation des dossiers
for directory in [DATA_DIR, MODELS_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f'pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def verify_models_exist():
    """
    Verifie que les modeles pre-entraines existent
    """
    required_models = [
        MODELS_DIR / "salary_model.pkl",
        MODELS_DIR / "turnover_model.pkl", 
        MODELS_DIR / "label_encoders.pkl"
    ]
    
    missing_models = [model for model in required_models if not model.exists()]
    
    if missing_models:
        logger.error("[ERROR] Modeles pre-entraines manquants:")
        for model in missing_models:
            logger.error(f"   - {model}")
        logger.error("[INFO] Veuillez d'abord entrainer les modeles avec le script d'entrainement")
        return False
    
    logger.info("[SUCCESS] Tous les modeles pre-entraines sont disponibles")
    return True

def main():
    """
    Fonction principale qui execute le pipeline complet
    """
    logger.info("[PIPELINE] Demarrage du Pipeline HR Analytics Gepec 2.0")
    logger.info("[INFO] Flux: Generation -> Kafka -> S3 -> Spark -> Predictions")
    
    try:
        # Verification prealable des modeles
        logger.info("[CHECK] Verification des modeles pre-entraines...")
        if not verify_models_exist():
            return None
        
        # Etape 1: Generation de donnees RH synthetiques
        logger.info("=" * 60)
        logger.info("[STEP 1] Generation de donnees RH synthetiques")
        logger.info("=" * 60)
        
        from src.data_generator import SyntheticDataGenerator
        generator = SyntheticDataGenerator()
        num_employees = 50  # Nombre d'employes a generer
        synthetic_data = generator.generate_employees(num_records=num_employees)
        
        logger.info(f"[SUCCESS] {len(synthetic_data)} employes synthetiques generes")
        logger.info(f"[DATA] Colonnes: {list(synthetic_data.columns)}")
        
        # Etape 2: Streaming via Kafka
        logger.info("=" * 60)
        logger.info("[STEP 2] Envoi des donnees vers Kafka")
        logger.info("=" * 60)
        
        from src.kafka_producer import KafkaDataProducer, KafkaDataConsumer
        producer = KafkaDataProducer()
        
        # Envoyer les donnees par batch
        kafka_success = producer.send_data_batch(synthetic_data)
        
        if kafka_success:
            logger.info("[SUCCESS] Donnees envoyees avec succes vers Kafka")
        else:
            logger.warning("[WARNING] Probleme avec l'envoi Kafka, mais on continue...")
        
        # Petite pause pour laisser Kafka traiter
        time.sleep(2)
        
        # Maintenant RÉCUPÉRER les données depuis Kafka
        logger.info("[KAFKA] Recuperation des donnees depuis Kafka...")
        consumer = KafkaDataConsumer(group_id='pipeline_consumer')
        kafka_messages = consumer.consume_messages(max_messages=num_employees)
        
        # Convertir les messages Kafka en DataFrame
        kafka_data_list = []
        for msg in kafka_messages:
            employee_data = msg['value']['data']
            kafka_data_list.append(employee_data)
        
        if kafka_data_list:
            kafka_dataframe = pd.DataFrame(kafka_data_list)
            logger.info(f"[SUCCESS] {len(kafka_dataframe)} employes recuperes depuis Kafka")
            # Utiliser les données depuis Kafka pour la suite
            data_for_processing = kafka_dataframe
        else:
            logger.warning("[WARNING] Aucune donnee recuperee depuis Kafka, utilisation des donnees originales")
            data_for_processing = synthetic_data
        
        consumer.close()
        
        # Etape 3: Stockage vers S3
        logger.info("=" * 60)
        logger.info("[STEP 3] Chargement vers S3 Data Lake")
        logger.info("=" * 60)
        
        from src.s3_handler import S3DataHandler
        s3_handler = S3DataHandler()
        
        # Upload vers S3 (maintenant avec les données depuis Kafka)
        batch_id = s3_handler.upload_batch(data_for_processing)
        logger.info(f"[SUCCESS] Donnees uploadees vers S3 avec l'ID: {batch_id}")
        
        # Etape 4: Recuperation depuis S3 et traitement Spark
        logger.info("=" * 60)
        logger.info("[STEP 4] Traitement Spark avec donnees depuis S3")
        logger.info("=" * 60)
        
        from src.spark_processor import SparkMLProcessor
        spark_processor = SparkMLProcessor()
        
        # Maintenant Spark charge les données DEPUIS S3 (pas les données directes)
        logger.info("[SPARK] Spark va charger les donnees depuis S3...")
        try:
            # Utiliser la nouvelle méthode qui charge depuis S3
            predictions = spark_processor.process_and_predict_from_s3()
            logger.info(f"[SUCCESS] Spark a traite les donnees depuis S3")
        except Exception as e:
            logger.warning(f"[FALLBACK] Erreur chargement S3: {e}")
            logger.info("[FALLBACK] Spark utilise les donnees en memoire...")
            # Fallback vers les données en mémoire si S3 échoue
            predictions = spark_processor.process_and_predict(data_for_processing)
        
        # Etape 5: Resultats et analyse
        logger.info("=" * 60)
        logger.info("[STEP 5] Resultats et Analyse")
        logger.info("=" * 60)
        
        if predictions is not None and len(predictions) > 0:
            logger.info("[SUCCESS] Pipeline execute avec succes !")
            logger.info(f"[RESULTS] {len(predictions)} predictions generees")
            
            # Afficher un resume detaille
            logger.info("[SUMMARY] Resume des resultats:")
            
            if 'Predicted_Salary' in predictions.columns:
                avg_predicted_salary = predictions['Predicted_Salary'].mean()
                min_salary = predictions['Predicted_Salary'].min()
                max_salary = predictions['Predicted_Salary'].max()
                logger.info(f"   [SALARY] Salaire moyen predit: {avg_predicted_salary:.0f} MAD")
                logger.info(f"   [SALARY] Salaire min/max: {min_salary:.0f} - {max_salary:.0f} MAD")
            
            if 'Predicted_Turnover_Risk' in predictions.columns:
                high_risk_count = (predictions['Predicted_Turnover_Risk'] == 'Eleve').sum()
                low_risk_count = (predictions['Predicted_Turnover_Risk'] == 'Faible').sum()
                logger.info(f"   [RISK] Employes a risque eleve de depart: {high_risk_count}")
                logger.info(f"   [RISK] Employes a risque faible de depart: {low_risk_count}")
                
                if high_risk_count > 0:
                    risk_percentage = (high_risk_count / len(predictions)) * 100
                    logger.info(f"   [STATS] Pourcentage a risque eleve: {risk_percentage:.1f}%")
            
            # Sauvegarder les resultats finaux
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = DATA_DIR / f"pipeline_results_{timestamp}.csv"
            predictions.to_csv(output_file, index=False)
            logger.info(f"[SAVE] Resultats sauvegardes: {output_file}")
            
            # Afficher quelques exemples
            logger.info("[PREVIEW] Apercu des predictions (5 premiers employes):")
            display_columns = ['ID_Employe', 'Age', 'Departement', 'Predicted_Salary', 'Predicted_Turnover_Risk']
            available_columns = [col for col in display_columns if col in predictions.columns]
            if available_columns:
                print(predictions[available_columns].head().to_string(index=False))
        
        else:
            logger.error("[ERROR] Aucune prediction generee")
            return None
        
        logger.info("=" * 60)
        logger.info("[COMPLETE] PIPELINE GEPEC 2.0 TERMINE AVEC SUCCES!")
        logger.info("=" * 60)
        
        return predictions
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur dans le pipeline: {str(e)}")
        import traceback
        logger.error(f"[DEBUG] Details de l'erreur: {traceback.format_exc()}")
        sys.exit(1)
    
    finally:
        # Nettoyage des ressources
        try:
            if 'spark_processor' in locals():
                spark_processor.close()
        except:
            pass

if __name__ == "__main__":
    results = main()
    if results is not None:
        print(f"\n[SUCCESS] Pipeline termine: {len(results)} employes traites avec predictions")
    else:
        print("\n[ERROR] Pipeline echoue")