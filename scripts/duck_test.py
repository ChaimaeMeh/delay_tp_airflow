import duckdb

# 1. Connexion (crée un fichier .duckdb pour stocker tes tables)
con = duckdb.connect("warehouse/gtfs.duckdb")

# 2. Exemple simple : lire un CSV (ici stops.txt de ton GTFS statique)
#    adapte le chemin à ton dossier
con.execute("""
    CREATE TABLE stops AS
    SELECT * FROM read_csv_auto('data/gtfs_static/stops.txt', HEADER=TRUE);
""")

# 3. Vérifier que ça marche
print(con.execute("SELECT COUNT(*) FROM stops").fetchall())

# 4. Sauvegarder en Parquet pour Power BI
con.execute("COPY stops TO 'warehouse/stops_test.parquet' (FORMAT PARQUET);")

con.close()
