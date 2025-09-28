# Pipeline ETL GTFS Temps Réel pour Dashboard Power BI

## Description du projet

Ce projet consiste à construire un **pipeline ETL** pour traiter des données de transport en commun en temps réel, afin de générer un tableau de bord Power BI permettant de suivre les retards et la qualité du service du réseau urbain Lignes d’Azur (Métropole de Nice).  

Les données utilisées proviennent du standard **GTFS** (General Transit Feed Specification) en open data sur [data.gouv.fr](https://www.data.gouv.fr).

Le pipeline extrait les données, les transforme avec **DuckDB** et les charge au format CSV pour une visualisation interactive sur Power BI Desktop.

---

## Objectifs

- Fournir une vue en temps réel des retards.
- Générer des KPI pertinents pour la gestion du réseau.
- Démontrer l'orchestration d'un pipeline avec **Apache Airflow**.
- Créer un produit MVP utilisable par les équipes opérationnelles.

---

## Technologies utilisées

- **Python**
- **DuckDB** (traitement OLAP)
- **Pandas**
- **Apache Airflow** (orchestration des tâches ETL)
- **Power BI Desktop** (visualisation des KPI)
- **gtfs-realtime-bindings**
- **Requests**, **PyArrow**
- **Docker**, **Docker Compose**
- **Logging** pour traçabilité

---

## Structure du projet

tp-airflow-gtfs-duckdb/
├─ docker-compose.yml
├─ requirements.txt
├─ logs/
├─ dags/
│ ├─ fused_static_realtime.py
│ ├─ gtfs_cleanup_dag.py
│ ├─ gtfs_realtime_dag.py
│ ├─ gtfs_static_dag.py
│ └─ kpi_dag.py
├─ data/
│ └─ scripts/
│ ├─ download_gtfs.py
│ ├─ duckdb_utils.py
│ ├─ fused_gtfs.py
│ ├─ parse_trip_updates.py
│ ├─ parse_vehicule_positions.py
│ ├─ utils.py
│ ├─ validate_data.py
│ └─ kpi/
│ ├─ kpi1_avg_delay.py
│ ├─ kpi2_bus_map.py
│ ├─ ...
│ └─ kpi10_problematic_stops.py
├─ warehouse/
└─ exports/


---

## KPI calculés

1. **Retards moyens dans le temps**  
2. **Carte des bus en temps réel**  
3. **Carte des arrêts avec état de service**  
4. **Lignes les plus en retard**  
5. **Heatmap heures × jours**  
6. **Taux de ponctualité**  
7. **Évolution du retard par arrêt**  
8. **Temps de parcours réel vs théorique**  
9. **Distribution des retards**  
10. **Top N arrêts les plus problématiques**

---

## Architecture du pipeline

Le pipeline suit le schéma ETL :

Extract → Transform → Load


- **Extract** : récupération des fichiers GTFS statiques et temps réel (protocole protobuf).  
- **Transform** : traitement des données via DuckDB (jointures, agrégats, calculs de retards).  
- **Load** : export des résultats horodatés au format CSV/Parquet pour Power BI.

💡 L’orchestration est gérée par **Apache Airflow** avec plusieurs DAGs pour chaque étape.

*(Voir diagramme d’architecture dans `/docs/architecture.png` )*

---

##  Installation

### Prérequis

- Python >= 3.9
- Docker (pour Airflow)
- Power BI Desktop

### Installation

1. Cloner le dépôt :
```bash 
git clone <URL_DU_REPO>
cd tp-airflow-gtfs-duckdb
```

2. Installer les dépendances :
pip install -r requirements.txt

3. Lancer Airflow :
docker-compose up -d

4. Accéder à l’interface Airflow :
http://localhost:8537

5. Lancer les DAGs :

- gtfs_static_dag

- gtfs_realtime_dag

- kpi_dag

- gtfs_cleanup_dag

### Visualisation dans Power BI

Connecter les fichiers exportés dans /exports/ à Power BI Desktop pour créer un tableau de bord interactif avec les KPI définis.

### Nettoyage

Le DAG gtfs_cleanup_dag supprime les fichiers plus anciens que 7 jours dans les dossiers /data et /exports afin d’éviter l’accumulation excessive de données.

### Requirements
apache-airflow==2.9.3
pandas
duckdb
pyarrow
requests
gtfs-realtime-bindings
