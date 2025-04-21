from dagster import PartitionedConfig, partitioned
from .pipeline import earthquake_pipeline

@partitioned(
    partitions_def=TimePartitionedConfig(start_date="2023-01-01", end_date="2023-12-31", cron_schedule="0 0 * * *")
)
def partitioned_earthquake_pipeline(date_partition: str):
    """
    Pipeline partitionné par date.
    """
    data = extract_earthquake_data(date_partition)  # Récupère les données pour une date spécifique
    load = load_earthquake_data(data)
    transformed_data = transform_earthquake_data(load)
    stats = compute_region_statistics(transformed_data)
    critical = detect_critical_earthquakes(transformed_data)
    plot_magnitude_class_distribution(transformed_data)
    plot_earthquake_map(transformed_data)
