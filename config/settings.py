# Configuration du Pipeline HR Analytics
# =====================================

import os
from pathlib import Path

# Chemins du projet
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"
LOGS_DIR = PROJECT_ROOT / "logs"

# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic_name': 'hr_analytics_topic',
    'client_id': 'hr_analytics_producer',
    'batch_size': 100,
    'linger_ms': 10
}

# Configuration AWS S3
S3_CONFIG = {
    'bucket_name': 'hr-analytics-gepec',
    'region': 'us-east-1',
    'folder_prefix': 'raw_data/',
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
}

# Configuration Spark
SPARK_CONFIG = {
    'app_name': 'HR_Analytics_Pipeline',
    'master': 'local[*]',
    'driver_memory': '4g',
    'executor_memory': '2g',
    'max_result_size': '2g'
}

# Configuration des modèles ML
ML_CONFIG = {
    'salary_model': {
        'type': 'linear_regression',
        'features': ['Age', 'Annees_Experience_Totale', 'Score_Performance_N-1'],
        'target': 'Salaire_Annuel_MAD',
        'test_size': 0.2,
        'random_state': 42
    },
    'turnover_model': {
        'type': 'random_forest',
        'features': ['Age', 'Satisfaction_Travail', 'Annees_Experience_Entreprise', 'Score_Performance_N-1'],
        'target': 'Risque_Depart',
        'n_estimators': 100,
        'max_depth': 10,
        'test_size': 0.2,
        'random_state': 42
    }
}

# Mapping des valeurs catégorielles
CATEGORICAL_MAPPINGS = {
    'Genre': {'Homme': 1, 'Femme': 0},
    'Niveau_Seniorite': {
        'Stagiaire': 0, 'Junior': 1, 'Confirmé': 2, 
        'Senior': 3, 'Lead': 4, 'Manager': 5, 'Directeur': 6
    },
    'Potentiel_Promotion': {'Faible': 0, 'Moyen': 1, 'Élevé': 2},
    'Risque_Depart': {'Faible': 0, 'Moyen': 1, 'Élevé': 2},
    'Statut_Teletravail': {'Présentiel': 0, 'Hybride': 1, 'Télétravail complet': 2}
}

# Villes marocaines pour la génération de données
MOROCCAN_CITIES = [
    'Casablanca', 'Rabat', 'Marrakech', 'Fès', 'Tanger', 
    'Agadir', 'Oujda', 'Tétouan', 'Salé', 'Meknès'
]

# Établissements de formation
EDUCATION_INSTITUTIONS = [
    'ENSIAS Rabat', 'ENSA Tanger', 'HEM Casablanca', 'ENCG Casablanca',
    'ISCAE Casablanca', 'INPT Rabat', 'ESMT Casablanca', 'EHTP Casablanca',
    'EMSI Casablanca', 'FST Fès', 'Université Mohammed V', 'Université Hassan II',
    'ESITH Casablanca', 'Sup\'Management', '1337 School', 'ENSAM Casablanca'
]

# Départements et postes
DEPARTMENTS_JOBS = {
    'Infrastructure & Cloud': [
        'Ingénieur Cloud GCP', 'Ingénieur Cloud AWS', 'Ingénieur Cloud Azure',
        'Ingénieur Réseau', 'Architecte Cloud', 'Spécialiste Kubernetes',
        'Spécialiste Virtualisation', 'Administrateur Système Windows'
    ],
    'Développement Logiciel': [
        'Développeur Frontend Junior', 'Développeur Backend Senior', 'Développeur Python',
        'Développeur Vue.js', 'Développeur React', 'Tech Lead', 'Architecte Logiciel',
        'Développeur Fullstack', 'Développeur .NET', 'Développeur PHP',
        'Développeur Angular', 'Développeur Mobile', 'Ingénieur Logiciel'
    ],
    'Data Science & IA': [
        'Data Scientist Senior', 'Data Analyst', 'AI/ML Specialist',
        'Big Data Engineer', 'Data Engineer', 'Business Intelligence Analyst',
        'Analytics Manager', 'Research Scientist', 'Data Scientist Junior',
        'Ingénieur Data', 'Statisticien'
    ],
    'DevOps & Automation': [
        'CI/CD Specialist', 'Build Engineer', 'Release Manager',
        'Monitoring Specialist', 'Site Reliability Engineer', 'Automation Engineer',
        'Infrastructure as Code Engineer', 'Platform Engineer', 'Ingénieur DevOps'
    ],
    'Cybersécurité': [
        'Security Architect', 'Incident Response Specialist', 'Ethical Hacker',
        'Analyste Sécurité', 'CISO', 'Spécialiste Conformité', 'Analyste SOC'
    ]
}

# Compétences techniques
TECHNICAL_SKILLS = [
    'Python', 'JavaScript', 'Java', 'C#', 'PHP', 'Ruby', 'Go', 'Rust', 'Swift', 'Kotlin',
    'React', 'Vue.js', 'Angular', 'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP',
    'Terraform', 'Jenkins', 'GitLab CI/CD', 'Prometheus', 'Grafana', 'Helm',
    'Pandas', 'NumPy', 'TensorFlow', 'PyTorch', 'Scikit-learn', 'Hadoop', 'Spark',
    'SQL', 'MongoDB', 'PostgreSQL', 'Redis', 'Cassandra', 'DynamoDB'
]