from dagster import Definitions, load_assets_from_modules, job, op, schedule
from . import assets

# Création d'une étape (op) pour illustrer un job avec de la logique
@op
def fetch_earthquake_data(context):
    context.log.info("Fetching earthquake data...")
    # Logique pour récupérer les données d'un tremblement de terre
    return {"earthquake": "data fetched"}

@op
def process_earthquake_data(context, earthquake_data):
    context.log.info(f"Processing earthquake data: {earthquake_data}")
    # Logique pour traiter les données
    return {"earthquake": "processed"}

@job
def earthquake_pipeline():
    # Enchaînement des étapes du job
    earthquake_data = fetch_earthquake_data()
    processed_data = process_earthquake_data(earthquake_data)

# Crée un schedule qui exécute 'earthquake_pipeline' tous les jours à minuit
@schedule(cron_schedule="0 0 * * *", job=earthquake_pipeline)
def daily_earthquake_job(_context):
    return {}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[earthquake_pipeline],  
    schedules=[daily_earthquake_job]  
)
