#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gestionnaire de stockage AWS S3
===============================

Ce module gère le stockage et la récupération des données RH
dans Amazon S3 pour le pipeline analytics.

Auteur: Équipe Data Science Gepec 2.0
"""

import boto3
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
import sys
from io import StringIO, BytesIO
from botocore.exceptions import ClientError, NoCredentialsError

# Import des configurations
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import S3_CONFIG

class S3DataHandler:
    """
    Gestionnaire pour les opérations S3
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = S3_CONFIG
        self.s3_client = None
        self.s3_resource = None
        self._initialize_s3_clients()
    
    def _initialize_s3_clients(self):
        """
        Initialise les clients S3
        """
        try:
            # Configuration de session AWS
            session_config = {}
            if self.config.get('aws_access_key_id') and self.config.get('aws_secret_access_key'):
                session_config.update({
                    'aws_access_key_id': self.config['aws_access_key_id'],
                    'aws_secret_access_key': self.config['aws_secret_access_key']
                })
            
            # Créer la session
            session = boto3.Session(**session_config)
            
            # Initialiser les clients
            self.s3_client = session.client('s3', region_name=self.config['region'])
            self.s3_resource = session.resource('s3', region_name=self.config['region'])
            
            # Tester la connexion
            self._test_connection()
            
            self.logger.info("☁️ Clients S3 initialisés avec succès")
            
        except NoCredentialsError:
            self.logger.warning("⚠️ Pas de credentials AWS trouvés, utilisation du profil par défaut")
            self.s3_client = boto3.client('s3', region_name=self.config['region'])
            self.s3_resource = boto3.resource('s3', region_name=self.config['region'])
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'initialisation S3: {str(e)}")
            raise
    
    def _test_connection(self):
        """
        Teste la connexion S3
        """
        try:
            # Lister les buckets pour tester la connexion
            self.s3_client.list_buckets()
            self.logger.debug("✅ Connexion S3 testée avec succès")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Test de connexion S3 échoué: {str(e)}")
    
    def create_bucket_if_not_exists(self):
        """
        Crée le bucket S3 s'il n'existe pas
        """
        bucket_name = self.config['bucket_name']
        
        try:
            # Vérifier si le bucket existe
            self.s3_client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"📦 Bucket '{bucket_name}' existe déjà")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                # Le bucket n'existe pas, le créer
                try:
                    if self.config['region'] == 'us-east-1':
                        # Pour us-east-1, pas besoin de spécifier la région
                        self.s3_client.create_bucket(Bucket=bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.config['region']}
                        )
                    
                    self.logger.info(f"📦 Bucket '{bucket_name}' créé avec succès")
                    
                except Exception as create_error:
                    self.logger.error(f"❌ Erreur création bucket: {str(create_error)}")
                    raise
            else:
                self.logger.error(f"❌ Erreur accès bucket: {str(e)}")
                raise
    
    def upload_dataframe(self, df, object_key, file_format='csv'):
        """
        Upload un DataFrame vers S3
        
        Args:
            df (pd.DataFrame): DataFrame à uploader
            object_key (str): Clé de l'objet S3
            file_format (str): Format du fichier ('csv', 'parquet', 'json')
        """
        try:
            # Ajouter le préfixe du dossier
            full_key = f"{self.config['folder_prefix']}{object_key}"
            
            # Préparer les données selon le format
            if file_format.lower() == 'csv':
                buffer = StringIO()
                df.to_csv(buffer, index=False, encoding='utf-8')
                content = buffer.getvalue().encode('utf-8')
                content_type = 'text/csv'
                
            elif file_format.lower() == 'parquet':
                buffer = BytesIO()
                df.to_parquet(buffer, index=False, engine='pyarrow')
                content = buffer.getvalue()
                content_type = 'application/octet-stream'
                
            elif file_format.lower() == 'json':
                buffer = StringIO()
                df.to_json(buffer, orient='records', force_ascii=False, indent=2)
                content = buffer.getvalue().encode('utf-8')
                content_type = 'application/json'
                
            else:
                raise ValueError(f"Format non supporté: {file_format}")
            
            # Upload vers S3
            self.s3_client.put_object(
                Bucket=self.config['bucket_name'],
                Key=full_key,
                Body=content,
                ContentType=content_type,
                Metadata={
                    'uploaded_at': datetime.now().isoformat(),
                    'records_count': str(len(df)),
                    'columns_count': str(len(df.columns)),
                    'format': file_format,
                    'source': 'hr_analytics_pipeline'
                }
            )
            
            self.logger.info(f"📤 DataFrame uploadé: s3://{self.config['bucket_name']}/{full_key}")
            self.logger.info(f"📊 {len(df)} lignes, {len(df.columns)} colonnes")
            
            return f"s3://{self.config['bucket_name']}/{full_key}"
            
        except Exception as e:
            self.logger.error(f"❌ Erreur upload DataFrame: {str(e)}")
            raise
    
    def upload_batch(self, data_df, batch_name=None):
        """
        Upload un batch de données avec horodatage automatique
        
        Args:
            data_df (pd.DataFrame): Données à uploader
            batch_name (str): Nom du batch (optionnel)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if batch_name is None:
            batch_name = f"hr_data_batch_{timestamp}"
        
        results = {}
        
        try:
            # Upload en CSV
            csv_key = f"{batch_name}.csv"
            csv_url = self.upload_dataframe(data_df, csv_key, 'csv')
            results['csv'] = csv_url
            
            # Upload en Parquet (plus efficace pour analytics)
            parquet_key = f"{batch_name}.parquet"
            parquet_url = self.upload_dataframe(data_df, parquet_key, 'parquet')
            results['parquet'] = parquet_url
            
            # Upload métadonnées en JSON
            metadata = {
                'batch_name': batch_name,
                'upload_timestamp': datetime.now().isoformat(),
                'records_count': len(data_df),
                'columns': list(data_df.columns),
                'data_types': {col: str(dtype) for col, dtype in data_df.dtypes.items()},
                'file_sizes': {
                    'csv': len(data_df.to_csv(index=False).encode('utf-8')),
                    'parquet': len(data_df.to_parquet(index=False, engine='pyarrow'))
                }
            }
            
            metadata_key = f"{batch_name}_metadata.json"
            metadata_buffer = StringIO()
            json.dump(metadata, metadata_buffer, indent=2, ensure_ascii=False, default=str)
            
            self.s3_client.put_object(
                Bucket=self.config['bucket_name'],
                Key=f"{self.config['folder_prefix']}{metadata_key}",
                Body=metadata_buffer.getvalue().encode('utf-8'),
                ContentType='application/json'
            )
            
            results['metadata'] = f"s3://{self.config['bucket_name']}/{self.config['folder_prefix']}{metadata_key}"
            
            self.logger.info(f"✅ Batch '{batch_name}' uploadé avec succès")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Erreur upload batch: {str(e)}")
            raise
    
    def download_dataframe(self, object_key, file_format='csv'):
        """
        Télécharge un DataFrame depuis S3
        
        Args:
            object_key (str): Clé de l'objet S3
            file_format (str): Format du fichier
            
        Returns:
            pd.DataFrame: DataFrame téléchargé
        """
        try:
            full_key = f"{self.config['folder_prefix']}{object_key}"
            
            # Télécharger l'objet
            response = self.s3_client.get_object(
                Bucket=self.config['bucket_name'],
                Key=full_key
            )
            
            content = response['Body'].read()
            
            # Parser selon le format
            if file_format.lower() == 'csv':
                df = pd.read_csv(StringIO(content.decode('utf-8')))
                
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(BytesIO(content))
                
            elif file_format.lower() == 'json':
                df = pd.read_json(StringIO(content.decode('utf-8')), orient='records')
                
            else:
                raise ValueError(f"Format non supporté: {file_format}")
            
            self.logger.info(f"📥 DataFrame téléchargé: {len(df)} lignes, {len(df.columns)} colonnes")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Erreur téléchargement: {str(e)}")
            raise
    
    def list_objects(self, prefix=None):
        """
        Liste les objets dans le bucket
        
        Args:
            prefix (str): Préfixe pour filtrer les objets
            
        Returns:
            list: Liste des objets
        """
        try:
            prefix = prefix or self.config['folder_prefix']
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.config['bucket_name'],
                Prefix=prefix
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag'].strip('"')
                    })
            
            self.logger.info(f"📋 {len(objects)} objets trouvés avec le préfixe '{prefix}'")
            return objects
            
        except Exception as e:
            self.logger.error(f"❌ Erreur listage objets: {str(e)}")
            raise
    
    def delete_object(self, object_key):
        """
        Supprime un objet du bucket
        
        Args:
            object_key (str): Clé de l'objet à supprimer
        """
        try:
            full_key = f"{self.config['folder_prefix']}{object_key}"
            
            self.s3_client.delete_object(
                Bucket=self.config['bucket_name'],
                Key=full_key
            )
            
            self.logger.info(f"🗑️ Objet supprimé: {full_key}")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur suppression objet: {str(e)}")
            raise
    
    def get_object_metadata(self, object_key):
        """
        Récupère les métadonnées d'un objet
        
        Args:
            object_key (str): Clé de l'objet
            
        Returns:
            dict: Métadonnées de l'objet
        """
        try:
            full_key = f"{self.config['folder_prefix']}{object_key}"
            
            response = self.s3_client.head_object(
                Bucket=self.config['bucket_name'],
                Key=full_key
            )
            
            metadata = {
                'content_length': response.get('ContentLength'),
                'content_type': response.get('ContentType'),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"❌ Erreur récupération métadonnées: {str(e)}")
            raise
    
    def generate_presigned_url(self, object_key, expiration=3600):
        """
        Génère une URL pré-signée pour télécharger un objet
        
        Args:
            object_key (str): Clé de l'objet
            expiration (int): Durée de validité en secondes
            
        Returns:
            str: URL pré-signée
        """
        try:
            full_key = f"{self.config['folder_prefix']}{object_key}"
            
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.config['bucket_name'],
                    'Key': full_key
                },
                ExpiresIn=expiration
            )
            
            self.logger.info(f"🔗 URL pré-signée générée (expire dans {expiration}s)")
            return url
            
        except Exception as e:
            self.logger.error(f"❌ Erreur génération URL: {str(e)}")
            raise

if __name__ == "__main__":
    # Test du gestionnaire S3
    s3_handler = S3DataHandler()
    
    # Créer le bucket si nécessaire
    s3_handler.create_bucket_if_not_exists()
    
    # Test avec des données factices
    test_data = pd.DataFrame({
        'Employe_ID': ['TEST_001', 'TEST_002'],
        'Nom': ['Test1', 'Test2'],
        'Salaire_Annuel_MAD': [50000, 60000]
    })
    
    # Upload test
    results = s3_handler.upload_batch(test_data, "test_batch")
    print("✅ Test S3 terminé")
    print(f"Fichiers uploadés: {list(results.keys())}")