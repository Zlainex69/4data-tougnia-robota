from dagster import job
from etl_project.assets import (
    extract_earthquake_data,
    load_earthquake_data,
    transform_earthquake_data,
    compute_region_statistics,
    detect_critical_earthquakes,
    plot_magnitude_class_distribution,
    plot_earthquake_map
)


@job
def earthquake_pipeline():
    # Extraction des données
    data = extract_earthquake_data()
    
    # Chargement dans la base de données DuckDB
    load = load_earthquake_data(data)
    
    # Transformation des données
    transformed_data = transform_earthquake_data(load)
    
    # Calcul des statistiques par région
    stats = compute_region_statistics(transformed_data)
    
    # Détection des séismes critiques
    critical = detect_critical_earthquakes(transformed_data)
    
    # Visualisation de la distribution des magnitudes
    plot_magnitude_class_distribution(transformed_data)
    
    # Visualisation sur une carte interactive
    plot_earthquake_map(transformed_data)
