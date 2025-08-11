# HR Analytics Pipeline Gepec 2.0 🚀

## 📋 Description

Complete HR analytics pipeline using Big Data and Machine Learning technologies to predict salaries and turnover risks of employees 

## 🏗️ Architecture

```
HR Analytics Pipeline
├── 🎲 Synthetic data generation
├── 🤖 ML Models (Linear Regression + Random Forest)
├── 📡 Real-time streaming (Apache Kafka)
├── ☁️ Cloud storage (AWS S3)
├── ⚡ Big Data processing (Apache Spark)
└── 📊 Analytics and predictions
```

## 🛠️ Technologies Used

- **Python 3.10** - Main language
- **Apache Spark** - Big Data processing
- **Apache Kafka** - Real-time streaming
- **AWS S3** - Cloud storage
- **Scikit-learn** - Machine Learning
- **Pandas/NumPy** - Data manipulation
- **MLflow** - Model tracking

## ⚙️ Installation

### 1. Prerequisites
- Python 3.10+
- Java 8+ (for Spark)
- Apache Kafka (optional, for streaming)
- AWS Account (optional, for S3)

### 2. Installing dependencies
```bash
# Create virtual environment
python -m venv venv_py310

# Activate environment (Windows)
venv_py310\Scripts\activate.bat

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration
1. Copy `.env` and adjust variables according to your environment
2. Configure your AWS credentials (optional)
3. Verify that Kafka is accessible (optional)

## 🚀 Usage

### Quick Start (Windows)
```bash
# Launch the interactive startup script
start_pipeline.bat
```

### Manual Execution
```bash
# Complete pipeline
python main.py

# Train models only
python -c "from src.ml_models import MLModelTrainer; trainer = MLModelTrainer(); trainer.train_all_models()"

# Generate synthetic data
python -c "from src.data_generator import SyntheticDataGenerator; gen = SyntheticDataGenerator(); data = gen.generate_employees(100); gen.save_to_files(data)"
```

## 📊 Included ML Models

### 1. Salary Prediction Model
- **Algorithm**: Linear Regression
- **Features**: Age, Experience, Education level, Department, etc.
- **Objective**: Predict annual salary in MAD

### 2. Turnover Risk Model
- **Algorithm**: Random Forest Classifier
- **Features**: Satisfaction, Performance, Tenure, Age, etc.
- **Objective**: Predict turnover risk (Low/High)

## 📁 Project Structure

```
gepec2.0/
├── 📄 main.py                    # Main script
├── 📄 requirements.txt           # Python dependencies
├── 📄 .env                       # Environment variables
├── 📄 start_pipeline.bat         # Windows startup script
├── 📄 README.md                  # Documentation
├── 📄 employees_morocco_2024.csv # Main dataset
│
├── 📁 config/
│   └── 📄 settings.py            # Central configuration
│
├── 📁 src/
│   ├── 📄 ml_models.py           # Machine Learning models
│   ├── 📄 data_generator.py      # Synthetic data generator
│   ├── 📄 kafka_producer.py      # Kafka producer
│   ├── 📄 s3_handler.py          # AWS S3 handler
│   └── 📄 spark_processor.py     # Apache Spark processor
│
├── 📁 data/                      # Generated data
├── 📁 models/                    # Saved ML models
├── 📁 logs/                      # Log files
└── 📁 venv_py310/                # Virtual environment
```

## 🔧 Advanced Configuration

### Environment Variables (.env)
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

### Model Customization
Modify parameters in `config/settings.py`:
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

## 📈 Main Features

### ✅ Synthetic Data Generation
- Realistic employee data based on Moroccan context
- Consistency of relationships between variables
- Export to CSV, Excel and JSON

### ✅ Machine Learning
- Automatic model training
- Evaluation and performance metrics
- Model saving and loading
- Result visualizations

### ✅ Real-Time Streaming
- Apache Kafka integration
- Batch and streaming processing
- Error handling and automatic retry

### ✅ Cloud Storage
- Automatic upload to AWS S3
- Multi-format support (CSV, Parquet, JSON)
- Metadata and versioning
- Pre-signed URLs for sharing

### ✅ Big Data Analytics
- Distributed processing with Apache Spark
- Real-time analytics metrics
- Large-volume predictions
- Performance optimizations

## 🔍 Monitoring and Logs

Logs are automatically generated in the `logs/` folder with:
- Operation timestamps
- Performance metrics
- Errors and warnings
- Prediction results

## 🤝 Contribution

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📞 Support


## 📜 License

This project is under MIT license - see the [LICENSE](LICENSE) file for more details.

## 🙏 Acknowledgments

- Gepec Data Science Team
- Apache Spark Community
- Open source contributors

---

**Gepec 2.0 HR Analytics Pipeline** - Transform your HR data into actionable insights! 🚀