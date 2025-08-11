#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module d'entraînement des modèles ML
===================================

Ce module contient les classes pour entraîner :
1. Modèle de régression linéaire pour prédire les salaires
2. Modèle Random Forest pour prédire le risque de départ

Auteur: Équipe Data Science Gepec 2.0
"""

import pandas as pd
import numpy as np
import joblib
import logging
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report
import matplotlib.pyplot as plt
import seaborn as sns

# Import des configurations
import sys
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import ML_CONFIG, CATEGORICAL_MAPPINGS, MODELS_DIR, PROJECT_ROOT

class MLModelTrainer:
    """
    Classe principale pour l'entraînement des modèles ML
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.models_dir = MODELS_DIR
        self.models_dir.mkdir(exist_ok=True)
        self.salary_model = None
        self.turnover_model = None
        self.label_encoders = {}
        
    def load_data(self):
        """
        Charge et préprocesse les données du fichier CSV
        """
        try:
            # Charger le dataset
            data_path = PROJECT_ROOT / "employees_morocco_2024.csv"
            df = pd.read_csv(data_path)
            
            self.logger.info(f"📊 Dataset chargé: {len(df)} lignes, {len(df.columns)} colonnes")
            
            # Nettoyage des données
            df = self._clean_data(df)
            
            # Encodage des variables catégorielles
            df = self._encode_categorical_variables(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors du chargement des données: {str(e)}")
            raise
    
    def _clean_data(self, df):
        """
        Nettoie les données en traitant les valeurs manquantes et aberrantes
        """
        # Supprimer les doublons
        initial_len = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_len:
            self.logger.info(f"🧹 {initial_len - len(df)} doublons supprimés")
        
        # Convertir les dates
        df['Date_Embauche'] = pd.to_datetime(df['Date_Embauche'])
        df['Date_Estimee_Retraite'] = pd.to_datetime(df['Date_Estimee_Retraite'])
        
        # Traiter les valeurs manquantes dans Risque_Depart
        if 'Risque_Depart' in df.columns:
            missing_count = df['Risque_Depart'].isna().sum()
            if missing_count > 0:
                # Remplacer les valeurs manquantes par "Faible" (valeur par défaut)
                df['Risque_Depart'] = df['Risque_Depart'].fillna('Faible')
                self.logger.info(f"🧹 {missing_count} valeurs manquantes dans Risque_Depart remplacées par 'Faible'")
        
        # Nettoyer les salaires (supprimer les valeurs aberrantes)
        Q1 = df['Salaire_Annuel_MAD'].quantile(0.25)
        Q3 = df['Salaire_Annuel_MAD'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        before_len = len(df)
        df = df[(df['Salaire_Annuel_MAD'] >= lower_bound) & 
                (df['Salaire_Annuel_MAD'] <= upper_bound)]
        
        if len(df) < before_len:
            self.logger.info(f"🧹 {before_len - len(df)} valeurs aberrantes de salaires supprimées")
        
        return df
    
    def _encode_categorical_variables(self, df):
        """
        Encode les variables catégorielles en utilisant les mappings définis
        """
        df_encoded = df.copy()
        
        for column, mapping in CATEGORICAL_MAPPINGS.items():
            if column in df_encoded.columns:
                # Vérifier les valeurs uniques avant encodage
                unique_values = df_encoded[column].unique()
                unmapped_values = [val for val in unique_values if val not in mapping and pd.notna(val)]
                
                if unmapped_values:
                    self.logger.warning(f"⚠️ Valeurs non mappées dans {column}: {unmapped_values}")
                    # Pour Risque_Depart, assigner les valeurs non mappées à "Faible" (0)
                    if column == 'Risque_Depart':
                        for unmapped_val in unmapped_values:
                            mapping[unmapped_val] = 0  # Assigner à "Faible"
                        self.logger.info(f"🔧 Valeurs non mappées dans {column} assignées à 'Faible'")
                
                df_encoded[column] = df_encoded[column].map(mapping)
                
                # Vérifier et traiter les NaN après encodage
                nan_count = df_encoded[column].isna().sum()
                if nan_count > 0:
                    if column == 'Risque_Depart':
                        df_encoded[column] = df_encoded[column].fillna(0)  # Faible = 0
                        self.logger.info(f"🧹 {nan_count} valeurs NaN dans {column} remplacées par 0 (Faible)")
                    else:
                        df_encoded[column] = df_encoded[column].fillna(df_encoded[column].mode()[0] if len(df_encoded[column].mode()) > 0 else 0)
                        self.logger.info(f"🧹 {nan_count} valeurs NaN dans {column} remplacées par la mode")
                
                self.logger.info(f"🔄 Variable {column} encodée")
        
        # Encoder d'autres variables catégorielles avec LabelEncoder
        categorical_cols = ['Ville', 'Niveau_Etudes', 'Etablissement_Formation', 
                           'Departement', 'Poste', 'Competence_Principale', 
                           'Niveau_Competence_Principale', 'Role_Futur_Souhaite']
        
        for col in categorical_cols:
            if col in df_encoded.columns:
                le = LabelEncoder()
                df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
                self.label_encoders[col] = le
                
        return df_encoded
    
    def train_salary_model(self, df):
        """
        Entraîne le modèle de régression linéaire pour prédire les salaires
        """
        self.logger.info("🎯 Entraînement du modèle de prédiction des salaires...")
        
        config = ML_CONFIG['salary_model']
        
        # Préparer les features et target
        feature_cols = config['features']
        target_col = config['target']
        
        # Vérifier que toutes les colonnes existent
        missing_cols = [col for col in feature_cols if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Colonnes manquantes: {missing_cols}")
            feature_cols = [col for col in feature_cols if col in df.columns]
        
        X = df[feature_cols]
        y = df[target_col]
        
        # Division train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=config['test_size'], random_state=config['random_state']
        )
        
        # Entraînement du modèle
        self.salary_model = LinearRegression()
        self.salary_model.fit(X_train, y_train)
        
        # Évaluation
        y_pred = self.salary_model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        self.logger.info(f"📈 Modèle salaire - MSE: {mse:.2f}, R²: {r2:.3f}")
        
        # Sauvegarde du modèle
        model_path = self.models_dir / "salary_model.pkl"
        joblib.dump(self.salary_model, model_path)
        self.logger.info(f"💾 Modèle de salaire sauvegardé: {model_path}")
        
        # Visualisation des résultats
        self._plot_salary_results(y_test, y_pred)
        
        return {
            'mse': mse,
            'r2': r2,
            'features': feature_cols,
            'n_samples_train': len(X_train),
            'n_samples_test': len(X_test)
        }
    
    def train_turnover_model(self, df):
        """
        Entraîne le modèle Random Forest pour prédire le risque de départ
        """
        self.logger.info("🎯 Entraînement du modèle de prédiction du risque de départ...")
        
        config = ML_CONFIG['turnover_model']
        
        # Préparer les features et target
        feature_cols = config['features']
        target_col = config['target']
        
        # Vérifier que toutes les colonnes existent
        missing_cols = [col for col in feature_cols if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Colonnes manquantes: {missing_cols}")
            feature_cols = [col for col in feature_cols if col in df.columns]
        
        X = df[feature_cols]
        y = df[target_col]
        
        # Division train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=config['test_size'], random_state=config['random_state']
        )
        
        # Entraînement du modèle
        self.turnover_model = RandomForestClassifier(
            n_estimators=config['n_estimators'],
            max_depth=config['max_depth'],
            random_state=config['random_state']
        )
        self.turnover_model.fit(X_train, y_train)
        
        # Évaluation
        y_pred = self.turnover_model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        self.logger.info(f"📈 Modèle risque départ - Accuracy: {accuracy:.3f}")
        self.logger.info(f"📊 Rapport de classification:\n{classification_report(y_test, y_pred)}")
        
        # Sauvegarde du modèle
        model_path = self.models_dir / "turnover_model.pkl"
        joblib.dump(self.turnover_model, model_path)
        self.logger.info(f"💾 Modèle de risque de départ sauvegardé: {model_path}")
        
        # Visualisation des résultats
        self._plot_turnover_results(y_test, y_pred)
        
        return {
            'accuracy': accuracy,
            'features': feature_cols,
            'n_samples_train': len(X_train),
            'n_samples_test': len(X_test)
        }
    
    def _plot_salary_results(self, y_test, y_pred):
        """
        Crée des graphiques pour visualiser les résultats du modèle de salaire
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Graphique de corrélation prédictions vs réel
        ax1.scatter(y_test, y_pred, alpha=0.6)
        ax1.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
        ax1.set_xlabel('Salaire Réel (MAD)')
        ax1.set_ylabel('Salaire Prédit (MAD)')
        ax1.set_title('Prédictions vs Réalité - Modèle Salaire')
        
        # Histogramme des résidus
        residuals = y_test - y_pred
        ax2.hist(residuals, bins=30, alpha=0.7)
        ax2.set_xlabel('Résidus (MAD)')
        ax2.set_ylabel('Fréquence')
        ax2.set_title('Distribution des Résidus')
        
        plt.tight_layout()
        plt.savefig(self.models_dir / 'salary_model_evaluation.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def _plot_turnover_results(self, y_test, y_pred):
        """
        Crée des graphiques pour visualiser les résultats du modèle de turnover
        """
        from sklearn.metrics import confusion_matrix
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Matrice de confusion
        cm = confusion_matrix(y_test, y_pred)
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax1)
        ax1.set_xlabel('Prédictions')
        ax1.set_ylabel('Réalité')
        ax1.set_title('Matrice de Confusion - Risque de Départ')
        
        # Importance des features
        if hasattr(self.turnover_model, 'feature_importances_'):
            features = ML_CONFIG['turnover_model']['features']
            importances = self.turnover_model.feature_importances_
            
            feature_importance = pd.DataFrame({
                'feature': features,
                'importance': importances
            }).sort_values('importance', ascending=True)
            
            ax2.barh(feature_importance['feature'], feature_importance['importance'])
            ax2.set_xlabel('Importance')
            ax2.set_title('Importance des Variables - Random Forest')
        
        plt.tight_layout()
        plt.savefig(self.models_dir / 'turnover_model_evaluation.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def train_all_models(self):
        """
        Entraîne tous les modèles ML
        """
        try:
            # Charger les données
            df = self.load_data()
            
            # Entraîner le modèle de salaire
            salary_results = self.train_salary_model(df)
            
            # Entraîner le modèle de turnover
            turnover_results = self.train_turnover_model(df)
            
            # Sauvegarder les encodeurs
            encoders_path = self.models_dir / "label_encoders.pkl"
            joblib.dump(self.label_encoders, encoders_path)
            
            # Résumé des résultats
            results_summary = {
                'salary_model': salary_results,
                'turnover_model': turnover_results,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            
            self.logger.info("✅ Tous les modèles ont été entraînés avec succès!")
            self.logger.info(f"📊 Résultats du modèle salaire: R² = {salary_results['r2']:.3f}")
            self.logger.info(f"📊 Résultats du modèle turnover: Accuracy = {turnover_results['accuracy']:.3f}")
            
            # Affichage des résultats dans la console
            print("\n" + "="*80)
            print("🎯 RÉSULTATS DE L'ENTRAÎNEMENT DES MODÈLES ML - GEPEC 2.0")
            print("="*80)
            print(f"📈 MODÈLE DE PRÉDICTION DES SALAIRES:")
            print(f"   • R² Score: {salary_results['r2']:.3f}")
            print(f"   • MSE: {salary_results['mse']:,.2f} MAD")
            print(f"   • Échantillons d'entraînement: {salary_results['n_samples_train']}")
            print(f"   • Échantillons de test: {salary_results['n_samples_test']}")
            print(f"   • Variables utilisées: {len(salary_results['features'])}")
            
            print(f"\n🔄 MODÈLE DE PRÉDICTION DU TURNOVER:")
            print(f"   • Accuracy: {turnover_results['accuracy']:.3f}")
            print(f"   • Échantillons d'entraînement: {turnover_results['n_samples_train']}")
            print(f"   • Échantillons de test: {turnover_results['n_samples_test']}")
            print(f"   • Variables utilisées: {len(turnover_results['features'])}")
            
            print(f"\n💾 FICHIERS GÉNÉRÉS:")
            print(f"   • Modèle salaire: {self.models_dir}/salary_model.pkl")
            print(f"   • Modèle turnover: {self.models_dir}/turnover_model.pkl")
            print(f"   • Encodeurs: {self.models_dir}/label_encoders.pkl")
            print(f"   • Évaluation salaire: {self.models_dir}/salary_model_evaluation.png")
            print(f"   • Évaluation turnover: {self.models_dir}/turnover_model_evaluation.png")
            print("="*80)
            print("✅ ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS!")
            print("="*80)
            
            return results_summary
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'entraînement des modèles: {str(e)}")
            raise

if __name__ == "__main__":
    trainer = MLModelTrainer()
    trainer.train_all_models()