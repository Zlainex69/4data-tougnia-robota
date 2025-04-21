from dagster import Definitions, load_assets_from_modules, job, op, schedule, sensor, RunRequest
from . import assets  

# Opérations (op) du job
@op
def fetch_earthquake_data(context):
    context.log.info("Fetching earthquake data...")
    # Logique pour récupérer les données
    return {"earthquake": "data fetched"}

@op
def process_earthquake_data(context, earthquake_data):
    context.log.info(f"Processing earthquake data: {earthquake_data}")
    # Logique pour traiter les données
    return {"earthquake": "processed"}

# Job utilisant les étapes (ops)
@job
def earthquake_pipeline():
    earthquake_data = fetch_earthquake_data()
    processed_data = process_earthquake_data(earthquake_data)

# Schedule pour exécuter le job tous les jours à minuit
@schedule(cron_schedule="0 0 * * *", job=earthquake_pipeline)
def daily_earthquake_job(_context):
    return {}

# Définition du sensor
@sensor(job=earthquake_pipeline)
def earthquake_data_sensor(context):
    # Logique pour déterminer s'il y a un événement de tremblement de terre
    earthquake_event_detected = True  # Simuler qu'un événement a été détecté
    if earthquake_event_detected:
        # Si un événement est détecté, exécute le job
        yield RunRequest(run_key=None, run_config={})

# Définition des assets, jobs, schedules et sensors
defs = Definitions(
    assets=load_assets_from_modules([assets]),  
    jobs=[earthquake_pipeline],  
    schedules=[daily_earthquake_job],  
    sensors=[earthquake_data_sensor] 
)
