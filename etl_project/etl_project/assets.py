import requests
import json
from dagster import asset
import duckdb
import pandas as pd

@asset
def extract_earthquake_data() -> dict:
    """
    Extrait les données de l'API USGS Earthquakes (dernier jour).
    """
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Erreur lors de la récupération des données : {response.status_code}")

    data = response.json()

    # sauvegarde locale (utile pour debug ou stockage temporaire)
    with open("tmp2xky_gzc/earthquakes_raw.json", "w") as f:
        json.dump(data, f)


    return data

@asset
def load_earthquake_data(extract_earthquake_data: dict) -> str:
    """
    
    Transforme les données extraites et les charge dans une base DuckDB.
    """
    features = extract_earthquake_data.get("features", [])

    records = []
    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])

        records.append({
            "id": feature.get("id"),
            "mag": props.get("mag"),
            "place": props.get("place"),
            "time": props.get("time"),
            "longitude": coords[0],
            "latitude": coords[1],
            "depth": coords[2],
            "type": props.get("type")
        })

    df = pd.DataFrame(records)

    # Connexion à DuckDB et chargement des données
    con = duckdb.connect("tmp2xky_gzc/earthquakes.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS earthquakes AS SELECT * FROM df")
    con.execute("DELETE FROM earthquakes")
    con.execute("INSERT INTO earthquakes SELECT * FROM df")

    con.close()
    return f"{len(df)} enregistrements insérés dans la base DuckDB"

from dagster import asset
import pandas as pd

def detect_region(place):
    if pd.isna(place):
        return "Inconnu"
    place = place.lower()

    regions = {
        "Amérique du Nord": ["alaska", "california", "ca", "texas", "nevada", "puerto rico", "hawaii", "mexico"],
        "Amérique du Sud": ["chile", "peru", "argentina", "ecuador", "colombia"],
        "Asie": ["japan", "china", "philippines", "taiwan", "india"],
        "Asie du Sud-Est": ["indonesia", "papua", "timor", "andaman", "myanmar"],
        "Europe": ["greece", "italy", "france", "portugal", "iceland", "turkey", "spain"],
        "Moyen-Orient": ["iran", "iraq", "syria", "afghanistan", "pakistan"],
        "Océanie": ["new zealand", "vanuatu", "fiji", "tonga", "samoa"],
        "Afrique": ["algeria", "morocco", "ethiopia", "kenya", "madagascar"]
    }

    for region, keywords in regions.items():
        if any(p in place for p in keywords):
            return region

    return "Autre / Inconnu"

def classify_magnitude(mag):
    if pd.isna(mag):
        return "Inconnu"
    if mag < 4.0:
        return "Très faible (1.5 - 3.9)"
    elif mag < 5.0:
        return "Modéré (4.0 - 4.9)"
    elif mag < 6.0:
        return "Fort (5.0 - 5.9)"
    elif mag < 7.0:
        return "Très fort (6.0 - 6.9)"
    elif mag < 8.0:
        return "Violent (7.0 - 7.9)"
    else:
        return "Catastrophique (8.0 et plus)"


@asset
def transform_earthquake_data(load_earthquake_data: str) -> pd.DataFrame:
    """
    Lit les données DuckDB, transforme les timestamps, régions et classes de magnitude.
    """
    con = duckdb.connect("tmp2xky_gzc/earthquakes.duckdb")
    df = con.execute("SELECT * FROM earthquakes").df()

    df["datetime"] = pd.to_datetime(df["time"], unit="ms")
    df["date"] = df["datetime"].dt.date
    df["region"] = df["place"].apply(detect_region)
    df["magnitude_class"] = df["mag"].apply(classify_magnitude)

    con.close()
    return df


@asset
def compute_region_statistics(transform_earthquake_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques (nombre, moyenne de magnitude et profondeur) par région.
    """
    region_counts = transform_earthquake_data["region"].value_counts().reset_index()
    region_counts.columns = ["region", "nb_seismes"]

    region_means = transform_earthquake_data.groupby("region")[["mag", "depth"]].mean().reset_index()
    region_means.columns = ["region", "magnitude_moy", "profondeur_moy"]

    stats = pd.merge(region_counts, region_means, on="region", how="left")
    return stats


@asset
def detect_critical_earthquakes(transform_earthquake_data: pd.DataFrame) -> pd.DataFrame:
    """
    Identifie les séismes critiques (magnitude >= 6.0).
    """
    critical = transform_earthquake_data[transform_earthquake_data["mag"] >= 6.0]
    return critical


import matplotlib.pyplot as plt
import folium
from dagster import asset
from branca.element import Template, MacroElement

# Visualisation par classe de magnitude
@asset
def plot_magnitude_class_distribution(transform_earthquake_data: pd.DataFrame) -> None:
    """
    Crée un histogramme du nombre de séismes par classe de magnitude.
    """
    magnitude_class_counts = transform_earthquake_data["magnitude_class"].value_counts().sort_index()

    # Créeation l'histogramme
    plt.figure(figsize=(12, 7))

    # Définition des couleurs et les bordures des barres
    colors = ["#32CD32", "#4682B4", "#FFA500", "#FF6347", "#8B0000"]  # Couleurs harmonieuses
    magnitude_class_counts.plot(kind="bar", color=colors, edgecolor="black", width=0.8)

    # Ajouter un titre et des labels
    plt.title("Nombre de Séismes par Classe de Magnitude", fontsize=16, fontweight='bold', color='darkblue')
    plt.xlabel("Classe de Magnitude", fontsize=12, fontweight='bold', color='darkblue')
    plt.ylabel("Nombre de Séismes", fontsize=12, fontweight='bold', color='darkblue')

    # Rotation des ticks et ajout de la grille
    plt.xticks(rotation=45, ha='right', fontsize=11)
    plt.yticks(fontsize=11)
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)

    # Ajuste l'affichage
    plt.tight_layout()
    plt.show()


# Visualisation des séismes sur une carte interactive
@asset
def plot_earthquake_map(transform_earthquake_data: pd.DataFrame) -> folium.Map:
    """
    Crée une carte interactive des séismes.
    """
    # Créeation la carte centrée sur l'équateur
    map = folium.Map(location=[0, 0], zoom_start=2)

    # Fonction pour déterminer la couleur du cercle selon la magnitude
    def get_color(magnitude):
        if magnitude < 4.0:
            return "green"
        elif 4.0 <= magnitude < 5.0:
            return "blue"
        elif 5.0 <= magnitude < 6.0:
            return "orange"
        elif 6.0 <= magnitude < 7.0:
            return "red"
        else:
            return "darkred"

    # Ajouter un cercle pour chaque séisme
    for _, row in transform_earthquake_data.iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=row["mag"] * 1.5,  # Taille proportionnelle à la magnitude
            popup=f"{row['place']} - Magnitude: {row['mag']}",
            color=get_color(row["mag"]),  # Couleur selon la magnitude
            fill=True,
            fill_opacity=0.6
        ).add_to(map)

    # Ajouter la légende
    legend_html = '''
         <div style="position: fixed; 
                     bottom: 50px; left: 50px; width: 220px; height: 130px; 
                     background-color: white; border:2px solid grey; z-index:9999; font-size:14px; 
                     padding: 10px;">
     <b> Magnitude des Séismes </b><br>
     <i style="background:green; width: 18px; height: 18px; display: inline-block; opacity: 0.6"></i> < 4.0<br>
     <i style="background:blue; width: 18px; height: 18px; display: inline-block; opacity: 0.6"></i> 4.0 - 5.0<br>
     <i style="background:orange; width: 18px; height: 18px; display: inline-block; opacity: 0.6"></i> 5.0 - 6.0<br>
     <i style="background:red; width: 18px; height: 18px; display: inline-block; opacity: 0.6"></i> 6.0 - 7.0<br>
     <i style="background:darkred; width: 18px; height: 18px; display: inline-block; opacity: 0.6"></i> > 7.0
     </div>
    '''

    # Ajouter la légende à la carte
    map.get_root().html.add_child(folium.Element(legend_html))

    # Retourner la carte
    return map


from dagster import asset

@asset
def save_to_csv(transform_earthquake_data: pd.DataFrame) -> str:
    """
    Sauvegarde les données transformées dans un fichier CSV.
    """
    output_path = "tmp2xky_gzc/earthquakes_transformed.csv"
    transform_earthquake_data.to_csv(output_path, index=False)
    return f"Données sauvegardées dans {output_path}"



import sqlite3

@asset
def save_to_sqlite(transform_earthquake_data: pd.DataFrame) -> str:
    """
    Sauvegarde les données dans une base de données SQLite.
    """
    conn = sqlite3.connect("tmp2xky_gzc/earthquakes.sqlite")
    transform_earthquake_data.to_sql("earthquakes", conn, if_exists="replace", index=False)
    conn.close()
    return "Données sauvegardées dans earthquakes.sqlite (table: earthquakes)"

