#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Producteur Kafka pour le streaming RH
=====================================

Ce module gère l'envoi des données RH vers Kafka pour le traitement
en temps réel dans le pipeline analytics.

Auteur: Équipe Data Science Gepec 2.0
"""

import json
import logging
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
from pathlib import Path

# Import des configurations
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import KAFKA_CONFIG

class KafkaDataProducer:
    """
    Producteur Kafka pour envoyer les données RH
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = KAFKA_CONFIG
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """
        Initialise le producteur Kafka
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['bootstrap_servers'],
                client_id=self.config['client_id'],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                batch_size=self.config['batch_size'],
                linger_ms=self.config['linger_ms'],
                acks='all',  # Assurer la livraison
                retries=3,
                compression_type='gzip'
            )
            self.logger.info("🔗 Producteur Kafka initialisé avec succès")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'initialisation du producteur Kafka: {str(e)}")
            raise
    
    def send_single_record(self, employee_data, employee_id=None):
        """
        Envoie un seul enregistrement d'employé vers Kafka
        
        Args:
            employee_data (dict): Données de l'employé
            employee_id (str): ID de l'employé (optionnel, utilisé comme clé)
        """
        try:
            # Ajouter des métadonnées
            message = {
                'data': employee_data,
                'timestamp': datetime.now().isoformat(),
                'source': 'hr_analytics_pipeline',
                'version': '2.0'
            }
            
            # Utiliser l'ID employé comme clé pour le partitioning
            key = employee_id or employee_data.get('Employe_ID', str(datetime.now().timestamp()))
            
            # Envoyer le message
            future = self.producer.send(
                topic=self.config['topic_name'],
                key=key,
                value=message
            )
            
            # Callback pour gérer le succès/échec
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return future
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'envoi vers Kafka: {str(e)}")
            raise
    
    def send_data_batch(self, data_df, batch_size=None):
        """
        Envoie un batch de données vers Kafka
        
        Args:
            data_df (pd.DataFrame): DataFrame contenant les données
            batch_size (int): Taille du batch (optionnel)
        """
        if batch_size is None:
            batch_size = self.config['batch_size']
        
        total_records = len(data_df)
        self.logger.info(f"📡 Envoi de {total_records} enregistrements vers Kafka...")
        
        sent_count = 0
        failed_count = 0
        
        try:
            for index, row in data_df.iterrows():
                try:
                    # Convertir la ligne en dictionnaire
                    employee_data = row.to_dict()
                    
                    # Nettoyer les valeurs NaN
                    employee_data = {k: (v if pd.notna(v) else None) for k, v in employee_data.items()}
                    
                    # Envoyer l'enregistrement
                    self.send_single_record(employee_data, employee_data.get('Employe_ID'))
                    sent_count += 1
                    
                    # Log du progrès
                    if sent_count % 50 == 0:
                        self.logger.info(f"📊 {sent_count}/{total_records} enregistrements envoyés")
                    
                except Exception as e:
                    failed_count += 1
                    self.logger.warning(f"⚠️ Échec envoi enregistrement {index}: {str(e)}")
            
            # Forcer l'envoi des messages en attente
            self.producer.flush()
            
            self.logger.info(f"✅ Envoi terminé: {sent_count} succès, {failed_count} échecs")
            
            return {
                'total_records': total_records,
                'sent_successfully': sent_count,
                'failed': failed_count,
                'success_rate': (sent_count / total_records) * 100 if total_records > 0 else 0
            }
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'envoi du batch: {str(e)}")
            raise
    
    def send_streaming_data(self, data_generator, interval_seconds=1):
        """
        Envoie des données en streaming continu
        
        Args:
            data_generator: Générateur qui produit des données
            interval_seconds (int): Intervalle entre les envois
        """
        import time
        
        self.logger.info(f"🔄 Démarrage du streaming (intervalle: {interval_seconds}s)")
        
        try:
            for employee_data in data_generator:
                # Envoyer les données
                self.send_single_record(employee_data)
                
                # Attendre avant le prochain envoi
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("⏹️ Streaming arrêté par l'utilisateur")
        except Exception as e:
            self.logger.error(f"❌ Erreur durant le streaming: {str(e)}")
            raise
    
    def _on_send_success(self, record_metadata):
        """
        Callback appelé en cas de succès d'envoi
        """
        self.logger.debug(f"✅ Message envoyé: topic={record_metadata.topic}, "
                         f"partition={record_metadata.partition}, "
                         f"offset={record_metadata.offset}")
    
    def _on_send_error(self, exception):
        """
        Callback appelé en cas d'erreur d'envoi
        """
        self.logger.error(f"❌ Erreur envoi Kafka: {str(exception)}")
    
    def create_topic_if_not_exists(self):
        """
        Crée le topic Kafka s'il n'existe pas
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.config['bootstrap_servers'],
                client_id=f"{self.config['client_id']}_admin"
            )
            
            # Vérifier si le topic existe
            existing_topics = admin_client.list_topics()
            
            if self.config['topic_name'] not in existing_topics:
                # Créer le topic
                topic = NewTopic(
                    name=self.config['topic_name'],
                    num_partitions=3,
                    replication_factor=1
                )
                
                admin_client.create_topics([topic])
                self.logger.info(f"📝 Topic '{self.config['topic_name']}' créé")
            else:
                self.logger.info(f"📝 Topic '{self.config['topic_name']}' existe déjà")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Impossible de créer le topic: {str(e)}")
    
    def get_producer_metrics(self):
        """
        Récupère les métriques du producteur
        """
        if self.producer:
            metrics = self.producer.metrics()
            
            # Extraire les métriques importantes
            key_metrics = {
                'records_sent': metrics.get('producer-metrics', {}).get('record-send-total', 0),
                'batch_size_avg': metrics.get('producer-metrics', {}).get('batch-size-avg', 0),
                'record_size_avg': metrics.get('producer-metrics', {}).get('record-size-avg', 0),
                'requests_in_flight': metrics.get('producer-metrics', {}).get('requests-in-flight', 0)
            }
            
            return key_metrics
        return {}
    
    def close(self):
        """
        Ferme proprement le producteur
        """
        if self.producer:
            self.logger.info("🔌 Fermeture du producteur Kafka...")
            self.producer.flush()  # Envoyer les messages en attente
            self.producer.close(timeout=10)
            self.logger.info("✅ Producteur Kafka fermé")

class KafkaDataConsumer:
    """
    Consommateur Kafka pour lire les données RH (optionnel pour tests)
    """
    
    def __init__(self, group_id='hr_analytics_consumer'):
        self.logger = logging.getLogger(__name__)
        self.config = KAFKA_CONFIG
        self.group_id = group_id
        self.consumer = None
        
    def _initialize_consumer(self):
        """
        Initialise le consommateur Kafka
        """
        try:
            from kafka import KafkaConsumer
            
            self.consumer = KafkaConsumer(
                self.config['topic_name'],
                bootstrap_servers=self.config['bootstrap_servers'],
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            
            self.logger.info("📥 Consommateur Kafka initialisé")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur initialisation consommateur: {str(e)}")
            raise
    
    def consume_messages(self, max_messages=100):
        """
        Consomme les messages du topic
        """
        if not self.consumer:
            self._initialize_consumer()
        
        messages = []
        count = 0
        
        self.logger.info(f"📥 Lecture des messages (max: {max_messages})...")
        
        try:
            for message in self.consumer:
                messages.append({
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp,
                    'value': message.value
                })
                
                count += 1
                if count >= max_messages:
                    break
            
            self.logger.info(f"📊 {len(messages)} messages consommés")
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la consommation: {str(e)}")
            raise
    
    def close(self):
        """
        Ferme le consommateur
        """
        if self.consumer:
            self.consumer.close()
            self.logger.info("✅ Consommateur Kafka fermé")

if __name__ == "__main__":
    # Test du producteur
    producer = KafkaDataProducer()
    
    # Créer le topic si nécessaire
    producer.create_topic_if_not_exists()
    
    # Test d'envoi d'un message
    test_data = {
        'Employe_ID': 'TEST_001',
        'Nom': 'Test',
        'Prenom': 'Utilisateur',
        'Salaire_Annuel_MAD': 50000
    }
    
    producer.send_single_record(test_data)
    producer.close()
    
    print("✅ Test du producteur Kafka terminé")