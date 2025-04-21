import duckdb
from dagster import sensor, RunRequest

# Fonction pour vérifier s'il y a de nouvelles données dans DuckDB
def new_data_available():
    # Connexion à la base de données DuckDB
    conn = duckdb.connect(database="./tmp2xky_gzc/earthquakes.duckdb")
    
    # Exécuter une requête pour compter le nombre d'enregistrements dans une table spécifique
    result = conn.execute("SELECT COUNT(*) FROM earthquakes").fetchone()  
    
    # Fermer la connexion
    conn.close()
    
    # Si le nombre d'enregistrements a changé (par exemple, augmentation depuis la dernière exécution)
    # On suppose qu'un nombre plus élevé indique de nouvelles données
    # du coup on peut sauvegarder un "last_run_count" quelque part (par exemple dans un fichier ou base de données)
    if result[0] > 1000:  # 1000 represente le seuil qu'on a defini
        return True
    return False

@sensor(job="earthquake_pipeline")
def new_earthquake_data_sensor(context):
    """
    Ce sensor vérifie si de nouvelles données sont disponibles dans la base de données DuckDB.
    """
    # Vérifie si de nouvelles données sont disponibles
    if new_data_available():
        context.log.info("Nouvelles données détectées, lancement du job.")
        return RunRequest(run_key=None, run_config={})
    
    context.log.info("Pas de nouvelles données.")
    return None
