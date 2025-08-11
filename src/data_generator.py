#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur de données synthétiques RH
=====================================

Ce module génère des données d'employés synthétiques mais réalistes
basées sur le contexte marocain et les tendances du marché IT.

Auteur: Équipe Data Science Gepec 2.0
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import logging
from pathlib import Path
import sys

# Import des configurations
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    MOROCCAN_CITIES, EDUCATION_INSTITUTIONS, DEPARTMENTS_JOBS,
    TECHNICAL_SKILLS, CATEGORICAL_MAPPINGS
)

class SyntheticDataGenerator:
    """
    Générateur de données synthétiques d'employés
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.fake = Faker('fr_FR')  # Utilisation du français
        Faker.seed(42)  # Pour la reproductibilité
        random.seed(42)
        np.random.seed(42)
        
    def generate_employees(self, num_records=1000):
        """
        Génère un dataset d'employés synthétiques
        
        Args:
            num_records (int): Nombre d'enregistrements à générer
            
        Returns:
            pd.DataFrame: Dataset des employés générés
        """
        self.logger.info(f"🎲 Génération de {num_records} employés synthétiques...")
        
        employees = []
        
        for i in range(num_records):
            employee = self._generate_single_employee()
            employees.append(employee)
            
            if (i + 1) % 100 == 0:
                self.logger.info(f"📊 {i + 1}/{num_records} employés générés")
        
        df = pd.DataFrame(employees)
        
        # Post-traitement pour assurer la cohérence
        df = self._post_process_data(df)
        
        self.logger.info(f"✅ Génération terminée: {len(df)} employés créés")
        return df
    
    def _generate_single_employee(self):
        """
        Génère un seul employé avec toutes ses caractéristiques
        """
        # Informations de base
        genre = random.choice(['Homme', 'Femme'])
        prenom = self.fake.first_name_male() if genre == 'Homme' else self.fake.first_name_female()
        nom = self.fake.last_name()
        
        # Âge et expérience cohérents
        age = random.randint(23, 60)
        annees_exp_totale = max(0, age - 22 + random.randint(-2, 5))
        annees_exp_entreprise = min(annees_exp_totale, random.randint(1, 15))
        
        # Localisation
        ville = random.choice(MOROCCAN_CITIES)
        
        # Éducation
        niveau_etudes = random.choices(
            ['Bac+3', 'Bac+5', 'Bac+8'],
            weights=[0.3, 0.6, 0.1]
        )[0]
        etablissement = random.choice(EDUCATION_INSTITUTIONS)
        
        # Emploi
        departement = random.choice(list(DEPARTMENTS_JOBS.keys()))
        poste = random.choice(DEPARTMENTS_JOBS[departement])
        
        # Niveau de séniorité basé sur l'expérience
        if annees_exp_totale < 1:
            niveau_seniorite = 'Stagiaire'
        elif annees_exp_totale < 3:
            niveau_seniorite = 'Junior'
        elif annees_exp_totale < 6:
            niveau_seniorite = 'Confirmé'
        elif annees_exp_totale < 10:
            niveau_seniorite = 'Senior'
        elif annees_exp_totale < 15:
            niveau_seniorite = random.choice(['Senior', 'Lead'])
        else:
            niveau_seniorite = random.choice(['Lead', 'Manager', 'Directeur'])
        
        # Compétences
        competence_principale = random.choice(TECHNICAL_SKILLS)
        niveau_competence = random.choices(
            ['Débutant', 'Intermédiaire', 'Avancé', 'Expert'],
            weights=[0.1, 0.3, 0.4, 0.2]
        )[0]
        
        # Salaire basé sur le niveau et l'expérience
        salaire_base = self._calculate_salary(niveau_seniorite, annees_exp_totale, departement)
        
        # Dates
        date_embauche = self.fake.date_between(
            start_date=datetime.now() - timedelta(days=annees_exp_entreprise*365),
            end_date=datetime.now() - timedelta(days=30)
        )
        
        date_retraite = datetime.now() + timedelta(days=(65-age)*365)
        
        # Métriques de performance
        score_performance = np.random.normal(75, 15)
        score_performance = max(0, min(100, score_performance))  # Borner entre 0 et 100
        
        satisfaction = random.randint(1, 10)
        
        # Potentiel et risques
        potentiel_promotion = random.choices(
            ['Faible', 'Moyen', 'Élevé'],
            weights=[0.3, 0.5, 0.2]
        )[0]
        
        # Risque de départ basé sur plusieurs facteurs
        risque_depart = self._calculate_turnover_risk(
            satisfaction, score_performance, annees_exp_entreprise, age
        )
        
        # Statut télétravail
        statut_teletravail = random.choices(
            ['Présentiel', 'Hybride', 'Télétravail complet'],
            weights=[0.4, 0.5, 0.1]
        )[0]
        
        # Rôle futur souhaité
        role_futur = random.choice(DEPARTMENTS_JOBS[departement])
        
        return {
            'Employe_ID': f"EMP_{random.randint(10000, 99999)}",
            'Prenom': prenom,
            'Nom': nom,
            'Genre': genre,
            'Age': age,
            'Ville': ville,
            'Niveau_Etudes': niveau_etudes,
            'Etablissement_Formation': etablissement,
            'Departement': departement,
            'Poste': poste,
            'Niveau_Seniorite': niveau_seniorite,
            'Annees_Experience_Totale': annees_exp_totale,
            'Annees_Experience_Entreprise': annees_exp_entreprise,
            'Date_Embauche': date_embauche.strftime('%Y-%m-%d'),
            'Salaire_Annuel_MAD': salaire_base,
            'Competence_Principale': competence_principale,
            'Niveau_Competence_Principale': niveau_competence,
            'Score_Performance_N-1': round(score_performance, 1),
            'Satisfaction_Travail': satisfaction,
            'Potentiel_Promotion': potentiel_promotion,
            'Risque_Depart': risque_depart,
            'Statut_Teletravail': statut_teletravail,
            'Role_Futur_Souhaite': role_futur,
            'Date_Estimee_Retraite': date_retraite.strftime('%Y-%m-%d'),
            'Date_Generation': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def _calculate_salary(self, niveau_seniorite, experience, departement):
        """
        Calcule un salaire réaliste basé sur le niveau et l'expérience
        """
        # Salaires de base par niveau (en MAD)
        base_salaries = {
            'Stagiaire': 8000,
            'Junior': 25000,
            'Confirmé': 45000,
            'Senior': 65000,
            'Lead': 85000,
            'Manager': 120000,
            'Directeur': 180000
        }
        
        # Multiplicateurs par département
        dept_multipliers = {
            'Data Science & IA': 1.3,
            'Cybersécurité': 1.2,
            'Infrastructure & Cloud': 1.1,
            'DevOps & Automation': 1.15,
            'Développement Logiciel': 1.0
        }
        
        base_salary = base_salaries.get(niveau_seniorite, 45000)
        dept_multiplier = dept_multipliers.get(departement, 1.0)
        
        # Ajustement basé sur l'expérience
        exp_bonus = experience * 2000
        
        # Variation aléatoire ±15%
        variation = random.uniform(0.85, 1.15)
        
        final_salary = (base_salary + exp_bonus) * dept_multiplier * variation
        
        return round(final_salary, 0)
    
    def _calculate_turnover_risk(self, satisfaction, performance, tenure, age):
        """
        Calcule le risque de départ basé sur plusieurs facteurs
        """
        risk_score = 0
        
        # Satisfaction (poids fort)
        if satisfaction <= 3:
            risk_score += 40
        elif satisfaction <= 5:
            risk_score += 20
        elif satisfaction <= 7:
            risk_score += 10
        
        # Performance
        if performance < 50:
            risk_score += 25
        elif performance > 85:
            risk_score -= 10
        
        # Ancienneté (courbe en U)
        if tenure < 1:
            risk_score += 20
        elif tenure > 8:
            risk_score += 15
        
        # Âge
        if 25 <= age <= 35:
            risk_score += 15  # Tranche d'âge mobile
        elif age > 50:
            risk_score -= 10
        
        # Ajouter de la variabilité
        risk_score += random.randint(-10, 10)
        
        return 'Élevé' if risk_score > 30 else 'Faible'
    
    def _post_process_data(self, df):
        """
        Post-traitement pour assurer la cohérence des données
        """
        # S'assurer que l'expérience en entreprise <= expérience totale
        df['Annees_Experience_Entreprise'] = np.minimum(
            df['Annees_Experience_Entreprise'], 
            df['Annees_Experience_Totale']
        )
        
        # Ajuster les salaires aberrants
        Q1 = df['Salaire_Annuel_MAD'].quantile(0.25)
        Q3 = df['Salaire_Annuel_MAD'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df['Salaire_Annuel_MAD'] = np.clip(df['Salaire_Annuel_MAD'], lower_bound, upper_bound)
        
        # S'assurer que les scores sont dans les bonnes plages
        df['Score_Performance_N-1'] = np.clip(df['Score_Performance_N-1'], 0, 100)
        df['Satisfaction_Travail'] = np.clip(df['Satisfaction_Travail'], 1, 10)
        
        return df
    
    def save_to_files(self, df, base_filename="synthetic_employees"):
        """
        Sauvegarde les données générées dans différents formats
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # CSV
        csv_path = f"{base_filename}_{timestamp}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        self.logger.info(f"💾 Données sauvegardées en CSV: {csv_path}")
        
        # Excel
        excel_path = f"{base_filename}_{timestamp}.xlsx"
        df.to_excel(excel_path, index=False, engine='openpyxl')
        self.logger.info(f"💾 Données sauvegardées en Excel: {excel_path}")
        
        return csv_path, excel_path

if __name__ == "__main__":
    generator = SyntheticDataGenerator()
    synthetic_data = generator.generate_employees(100)
    generator.save_to_files(synthetic_data)
    print(f"Dataset généré avec {len(synthetic_data)} employés")
    print(synthetic_data.head())