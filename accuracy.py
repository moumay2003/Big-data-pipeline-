import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, r2_score, mean_squared_error
import joblib

def calculate_real_accuracy():
    """
    Calcule l'accuracy réelle sur vos données marocaines
    """
    # 1. Charger vos données réelles
    df = pd.read_csv('employees_morocco_2024.csv')
    
    # 2. Preprocessing (comme dans votre pipeline)
    df_encoded = df.copy()
    
    # Mapping Gender
    df_encoded['Genre'] = df_encoded['Genre'].map({'Homme': 1, 'Femme': 0})
    
    # Mapping Département
    dept_mapping = {
        'Infrastructure & Cloud': 0,
        'Marketing Digital': 1, 
        'Développement Logiciel': 2,
        'Cybersécurité': 3,
        'Data Science & IA': 4,
        'Gestion de Produit': 5
    }
    df_encoded['Departement'] = df_encoded['Departement'].map(dept_mapping)
    
    # Mapping Niveau Séniorité
    seniorite_mapping = {'Junior': 0, 'Intermédiaire': 1, 'Senior': 2, 'Expert': 3}
    df_encoded['Niveau_Seniorite'] = df_encoded['Niveau_Seniorite'].map(seniorite_mapping)
    
    # Mapping Ville
    ville_mapping = {'Casablanca': 0, 'Tanger': 1, 'Marrakech': 2, 'Rabat': 3, 'Fès': 4}
    df_encoded['Ville'] = df_encoded['Ville'].map(ville_mapping)
    
    # Mapping Risque Départ
    df_encoded['Risque_Depart'] = df_encoded['Risque_Depart'].map({'Faible': 0, 'Élevé': 1})
    
    # Nettoyer les valeurs manquantes
    df_encoded = df_encoded.fillna(0)
    
    # 3. CORRIGER LES FEATURES SELON L'ENTRAÎNEMENT
    # D'après l'erreur, les modèles attendent 'Score_Performance_N-1'
    
    # Features pour le modèle SALAIRE (basées sur l'erreur)
    salary_features = ['Score_Performance_N-1']
    
    # Features pour le modèle TURNOVER (basées sur l'erreur) 
    turnover_features = ['Score_Performance_N-1']
    
    print("🔍 Features utilisées:")
    print(f"Salaire: {salary_features}")
    print(f"Turnover: {turnover_features}")
    
    # Vérifier si les colonnes existent
    available_cols = df_encoded.columns.tolist()
    print(f"\n📋 Colonnes disponibles: {available_cols}")
    
    # Préparer les données
    X_salary = df_encoded[salary_features]
    y_salary = df_encoded['Salaire_Annuel_MAD']
    
    X_turnover = df_encoded[turnover_features]
    y_turnover = df_encoded['Risque_Depart']
    
    print(f"\n📊 Shape des données:")
    print(f"X_salary: {X_salary.shape}")
    print(f"X_turnover: {X_turnover.shape}")
    
    # 4. Split Train/Test (80/20)
    X_sal_train, X_sal_test, y_sal_train, y_sal_test = train_test_split(
        X_salary, y_salary, test_size=0.2, random_state=42
    )
    
    X_turn_train, X_turn_test, y_turn_train, y_turn_test = train_test_split(
        X_turnover, y_turnover, test_size=0.2, random_state=42
    )
    
    # 5. Charger vos modèles pré-entraînés
    try:
        salary_model = joblib.load('models/salary_model.pkl')
        turnover_model = joblib.load('models/turnover_model.pkl')
        
        print("\n✅ Modèles chargés avec succès")
        
        # 6. CALCUL DES MÉTRIQUES RÉELLES
        
        # --- MODÈLE SALAIRE ---
        print("\n🔄 Test du modèle salaire...")
        sal_train_pred = salary_model.predict(X_sal_train)
        sal_test_pred = salary_model.predict(X_sal_test)
        
        print("\n" + "="*50)
        print("📊 MÉTRIQUES MODÈLE SALAIRE")
        print("="*50)
        print(f"R² Score (Train): {r2_score(y_sal_train, sal_train_pred):.4f}")
        print(f"R² Score (Test):  {r2_score(y_sal_test, sal_test_pred):.4f}")
        print(f"RMSE (Train): {np.sqrt(mean_squared_error(y_sal_train, sal_train_pred)):,.0f} MAD")
        print(f"RMSE (Test):  {np.sqrt(mean_squared_error(y_sal_test, sal_test_pred)):,.0f} MAD")
        print(f"MAE (Test):   {np.mean(np.abs(y_sal_test - sal_test_pred)):,.0f} MAD")
        
        # --- MODÈLE TURNOVER ---
        print("\n🔄 Test du modèle turnover...")
        turn_train_pred = turnover_model.predict(X_turn_train)
        turn_test_pred = turnover_model.predict(X_turn_test)
        
        print("\n" + "="*50)
        print("⚠️  MÉTRIQUES MODÈLE TURNOVER")
        print("="*50)
        print(f"Accuracy (Train): {accuracy_score(y_turn_train, turn_train_pred):.4f} ({accuracy_score(y_turn_train, turn_train_pred)*100:.1f}%)")
        print(f"Accuracy (Test):  {accuracy_score(y_turn_test, turn_test_pred):.4f} ({accuracy_score(y_turn_test, turn_test_pred)*100:.1f}%)")
        
        print("\n📋 RAPPORT DE CLASSIFICATION (Test):")
        print(classification_report(y_turn_test, turn_test_pred, 
                                  target_names=['Faible Risque', 'Risque Élevé']))
        
        # 7. Analyse de la distribution
        print("\n" + "="*50)
        print("📈 ANALYSE DES DONNÉES")
        print("="*50)
        print(f"Nombre total d'employés: {len(df)}")
        print(f"Répartition Risque de Départ:")
        print(f"  - Faible: {(y_turnover == 0).sum()} ({(y_turnover == 0).mean()*100:.1f}%)")
        print(f"  - Élevé:  {(y_turnover == 1).sum()} ({(y_turnover == 1).mean()*100:.1f}%)")
        
        print(f"\nSalaire moyen: {y_salary.mean():,.0f} MAD")
        print(f"Salaire médian: {y_salary.median():,.0f} MAD")
        print(f"Écart-type salaire: {y_salary.std():,.0f} MAD")
        
        return {
            'salary_r2_test': r2_score(y_sal_test, sal_test_pred),
            'salary_rmse_test': np.sqrt(mean_squared_error(y_sal_test, sal_test_pred)),
            'turnover_accuracy_test': accuracy_score(y_turn_test, turn_test_pred),
            'turnover_accuracy_train': accuracy_score(y_turn_train, turn_train_pred)
        }
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None

# Exécuter le calcul
if __name__ == "__main__":
    metrics = calculate_real_accuracy()
    
    if metrics:
        print(f"\n🏆 RÉSUMÉ FINAL:")
        print(f"   Accuracy Turnover (Test): {metrics['turnover_accuracy_test']*100:.1f}%")
        print(f"   R² Score Salaire (Test):  {metrics['salary_r2_test']:.3f}")
        print(f"   RMSE Salaire (Test):      {metrics['salary_rmse_test']:,.0f} MAD")