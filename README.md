# Pipeline HR Analytics Gepec 2.0 🚀

## 📋 Description

Pipeline complet d'analyse RH utilisant les technologies Big Data et Machine Learning pour prédire les salaires et les risques de départ des employés au Maroc.

## 🏗️ Architecture

```
Pipeline HR Analytics
├── 🎲 Génération de données synthétiques
├── 🤖 Modèles ML (Régression Linéaire + Random Forest)
├── 📡 Streaming temps réel (Apache Kafka)
├── ☁️ Stockage cloud (AWS S3)
├── ⚡ Traitement Big Data (Apache Spark)
└── 📊 Analytics et prédictions
```

## 🛠️ Technologies Utilisées

- **Python 3.10** - Langage principal
- **Apache Spark** - Traitement Big Data
- **Apache Kafka** - Streaming temps réel
- **AWS S3** - Stockage cloud
- **Scikit-learn** - Machine Learning
- **Pandas/NumPy** - Manipulation de données
- **MLflow** - Tracking des modèles

## ⚙️ Installation

### 1. Prérequis
- Python 3.10+
- Java 8+ (pour Spark)
- Apache Kafka (optionnel, pour le streaming)
- Compte AWS (optionnel, pour S3)

### 2. Installation des dépendances
```bash
# Créer l'environnement virtuel
python -m venv venv_py310

# Activer l'environnement (Windows)
venv_py310\Scripts\activate.bat

# Installer les dépendances
pip install -r requirements.txt
```

### 3. Configuration
1. Copier `.env` et ajuster les variables selon votre environnement
2. Configurer vos credentials AWS (optionnel)
3. Vérifier que Kafka est accessible (optionnel)

## 🚀 Utilisation

### Démarrage Rapide (Windows)
```bash
# Lancer le script de démarrage interactif
start_pipeline.bat
```

### Exécution Manuelle
```bash
# Pipeline complet
python main.py

# Entraînement des modèles uniquement
python -c "from src.ml_models import MLModelTrainer; trainer = MLModelTrainer(); trainer.train_all_models()"

# Génération de données synthétiques
python -c "from src.data_generator import SyntheticDataGenerator; gen = SyntheticDataGenerator(); data = gen.generate_employees(100); gen.save_to_files(data)"
```

## 📊 Modèles ML Inclus

### 1. Modèle de Prédiction de Salaire
- **Algorithme** : Régression Linéaire
- **Features** : Âge, Expérience, Niveau d'études, Département, etc.
- **Objectif** : Prédire le salaire annuel en MAD

### 2. Modèle de Risque de Départ
- **Algorithme** : Random Forest Classifier
- **Features** : Satisfaction, Performance, Ancienneté, Âge, etc.
- **Objectif** : Prédire le risque de départ (Faible/Élevé)

## 📁 Structure du Projet

```
gepec2.0/
├── 📄 main.py                    # Script principal
├── 📄 requirements.txt           # Dépendances Python
├── 📄 .env                       # Variables d'environnement
├── 📄 start_pipeline.bat         # Script de démarrage Windows
├── 📄 README.md                  # Documentation
├── 📄 employees_morocco_2024.csv # Dataset principal
│
├── 📁 config/
│   └── 📄 settings.py            # Configuration centrale
│
├── 📁 src/
│   ├── 📄 ml_models.py           # Modèles Machine Learning
│   ├── 📄 data_generator.py      # Générateur de données synthétiques
│   ├── 📄 kafka_producer.py      # Producteur Kafka
│   ├── 📄 s3_handler.py          # Gestionnaire AWS S3
│   └── 📄 spark_processor.py     # Processeur Apache Spark
│
├── 📁 data/                      # Données générées
├── 📁 models/                    # Modèles ML sauvegardés
├── 📁 logs/                      # Fichiers de log
└── 📁 venv_py310/               # Environnement virtuel
```

## 🔧 Configuration Avancée

### Variables d'Environnement (.env)
```bash
# AWS S3
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET_NAME=hr-analytics-gepec

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_NAME=hr_analytics_topic

# Spark
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=2g
```

### Personnalisation des Modèles
Modifiez les paramètres dans `config/settings.py` :
```python
ML_CONFIG = {
    'salary_model': {
        'features': ['Age', 'Annees_Experience_Totale', ...],
        'test_size': 0.2,
        'random_state': 42
    },
    'turnover_model': {
        'n_estimators': 100,
        'max_depth': 10,
        ...
    }
}
```

## 📈 Fonctionnalités Principales

### ✅ Génération de Données Synthétiques
- Données d'employés réalistes basées sur le contexte marocain
- Cohérence des relations entre variables
- Export en CSV, Excel et JSON

### ✅ Machine Learning
- Entraînement automatique des modèles
- Évaluation et métriques de performance
- Sauvegarde et chargement des modèles
- Visualisations des résultats

### ✅ Streaming Temps Réel
- Intégration Apache Kafka
- Traitement en batch et streaming
- Gestion des erreurs et retry automatique

### ✅ Stockage Cloud
- Upload automatique vers AWS S3
- Support multi-formats (CSV, Parquet, JSON)
- Métadonnées et versioning
- URLs pré-signées pour partage

### ✅ Analytics Big Data
- Traitement distribué avec Apache Spark
- Métriques d'analytics en temps réel
- Prédictions sur large volume
- Optimisations de performance

## 🔍 Monitoring et Logs

Les logs sont automatiquement générés dans le dossier `logs/` avec :
- Horodatage des opérations
- Métriques de performance
- Erreurs et warnings
- Résultats des prédictions

## 🤝 Contribution

1. Fork le projet
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📞 Support

Pour toute question ou problème :
- 📧 Email : support@gepec.ma
- 🐛 Issues : [GitHub Issues](https://github.com/gepec/hr-analytics-2.0/issues)
- 📖 Documentation : [Wiki](https://github.com/gepec/hr-analytics-2.0/wiki)

## 📜 Licence

Ce projet est sous licence MIT - voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 🙏 Remerciements

- Équipe Data Science Gepec
- Communauté Apache Spark
- Contributeurs open source

---

**Gepec 2.0 HR Analytics Pipeline** - Transformez vos données RH en insights actionnables ! 🚀