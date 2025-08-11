import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Tâches et périodes (ordre naturel : première en haut, dernière en bas)
tasks = [
    "Étude documentaire sur la GPEC",
    "Recherche et définition de la problématique",
    "Recherche et sélection du dataset externe",
    "Simulation dataset interne",
    "Prétraitement et exploration données externes",
    "Application modèles prédiction",
    "Analyse des données internes",
    "Création dashboards",
    "Rédaction rapport final"
]

start_dates = [
    "2025-04-10", "2025-04-12", "2025-04-14", "2025-04-22",
    "2025-04-24", "2025-04-29", "2025-05-06", "2025-05-20", "2025-05-26"
]

end_dates = [
    "2025-04-11", "2025-04-13", "2025-04-21", "2025-04-23",
    "2025-04-28", "2025-05-06", "2025-05-20", "2025-05-26", "2025-05-28"
]

# Convertir en datetime
start_dates = [datetime.strptime(date, "%Y-%m-%d") for date in start_dates]
end_dates = [datetime.strptime(date, "%Y-%m-%d") for date in end_dates]

durations = [(end - start).days + 1 for start, end in zip(start_dates, end_dates)]

# Inverser la liste des tâches pour l’affichage car matplotlib barh affiche la première en bas par défaut
tasks_display = tasks[::-1]
start_dates_display = start_dates[::-1]
durations_display = durations[::-1]

# Plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(tasks_display, durations_display, left=start_dates_display, color='skyblue')

# Format date sur axe X
ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%m-%Y"))
plt.xticks(rotation=45)

# Titres et labels
plt.title("Diagramme de GANTT du projet GPEC")
plt.xlabel("Dates")
plt.ylabel("Tâches")

plt.tight_layout()
plt.show()
