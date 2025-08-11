@echo off
REM Script de démarrage du Pipeline HR Analytics Gepec 2.0
REM ====================================================

echo.
echo =====================================================
echo    Pipeline HR Analytics Gepec 2.0
echo    Script de demarrage automatique
echo =====================================================
echo.

REM Vérifier si l'environnement virtuel existe
if not exist "venv_py310\Scripts\activate.bat" (
    echo ❌ Environnement virtuel non trouve!
    echo Veuillez d'abord creer l'environnement virtuel:
    echo python -m venv venv_py310
    echo venv_py310\Scripts\activate.bat
    echo pip install -r requirements.txt
    pause
    exit /b 1
)

REM Activer l'environnement virtuel
echo 🔧 Activation de l'environnement virtuel...
call venv_py310\Scripts\activate.bat

REM Vérifier les dépendances critiques
echo 🔍 Verification des dependances...
python -c "import pandas, numpy, sklearn, pyspark" 2>nul
if errorlevel 1 (
    echo ❌ Dependencies manquantes! Installation...
    pip install -r requirements.txt
)

REM Créer les dossiers nécessaires
echo 📁 Creation des dossiers...
if not exist "data" mkdir data
if not exist "models" mkdir models
if not exist "logs" mkdir logs

REM Afficher le menu
:menu
echo.
echo =====================================================
echo    MENU PRINCIPAL
echo =====================================================
echo 1. Executer le pipeline complet
echo 2. Entrainer seulement les modeles ML
echo 3. Generer des donnees synthetiques
echo 4. Tester la connexion S3
echo 5. Tester Kafka
echo 6. Quitter
echo.
set /p choice="Votre choix (1-6): "

if "%choice%"=="1" goto run_full_pipeline
if "%choice%"=="2" goto train_models
if "%choice%"=="3" goto generate_data
if "%choice%"=="4" goto test_s3
if "%choice%"=="5" goto test_kafka
if "%choice%"=="6" goto exit
goto menu

:run_full_pipeline
echo.
echo 🚀 Execution du pipeline complet...
python main.py
if errorlevel 1 (
    echo ❌ Erreur lors de l'execution!
    pause
) else (
    echo ✅ Pipeline execute avec succes!
    pause
)
goto menu

:train_models
echo.
echo 📊 Entrainement des modeles ML...
python -c "from src.ml_models import MLModelTrainer; trainer = MLModelTrainer(); trainer.train_all_models()"
if errorlevel 1 (
    echo ❌ Erreur lors de l'entrainement!
    pause
) else (
    echo ✅ Modeles entraines avec succes!
    pause
)
goto menu

:generate_data
echo.
set /p num_records="Nombre d'employes a generer (defaut: 100): "
if "%num_records%"=="" set num_records=100
echo 🎲 Generation de %num_records% employes...
python -c "from src.data_generator import SyntheticDataGenerator; gen = SyntheticDataGenerator(); data = gen.generate_employees(%num_records%); gen.save_to_files(data)"
if errorlevel 1 (
    echo ❌ Erreur lors de la generation!
    pause
) else (
    echo ✅ Donnees generees avec succes!
    pause
)
goto menu

:test_s3
echo.
echo ☁️ Test de la connexion S3...
python -c "from src.s3_handler import S3DataHandler; handler = S3DataHandler(); handler.create_bucket_if_not_exists(); print('✅ Connexion S3 OK')"
if errorlevel 1 (
    echo ❌ Erreur de connexion S3!
    echo Verifiez vos credentials AWS dans le fichier .env
    pause
) else (
    echo ✅ Connexion S3 reussie!
    pause
)
goto menu

:test_kafka
echo.
echo 📡 Test de la connexion Kafka...
python -c "from src.kafka_producer import KafkaDataProducer; producer = KafkaDataProducer(); producer.create_topic_if_not_exists(); print('✅ Kafka OK')"
if errorlevel 1 (
    echo ❌ Erreur de connexion Kafka!
    echo Assurez-vous que Kafka est demarre sur localhost:9092
    pause
) else (
    echo ✅ Connexion Kafka reussie!
    pause
)
goto menu

:exit
echo.
echo 👋 Au revoir!
echo Desactivation de l'environnement virtuel...
deactivate
pause
exit /b 0