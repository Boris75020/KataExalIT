import pycountry_convert as pc
import geopy.distance
import logging

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType

from extract import Extract

# Configuration de la journalisation
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

maClasse = Extract()
flights_df = maClasse.flights_extraction()
airports_df = maClasse.airports_extraction()
airlines_df = maClasse.airlines_extraction()

flights_df_filter = (flights_df.filter((col("origin_airport_iata") != "N/A") &
                                       (col("destination_airport_iata") != "N/A") &
                                       (col('airline_icao') != 'N/A')))

def get_continent(country_name):
    # Créez un dictionnaire de mappage pour les pays spécifiques
    custom_country_mapping = {
        'Democratic Republic Of The Congo': 'CD',
        'Curacao': 'CW',
        'Trinidad And Tobago': 'TT',
        "Cote D'ivoire (ivory Coast)": 'CI',
        'Myanmar (burma)': 'MM',
        'Reunion': 'RE',
        'Antigua And Barbuda': 'AG',
        'Bosnia And Herzegovina': 'BA',
        'Saint Helena': 'SH',
        'Saint Kitts And Nevis': 'KN',
        'Saint Vincent And The Grenadines': 'VC',
        'Myanmar (Burma)': 'MM',
        'Virgin Islands British': 'VG',
        'Turks And Caicos Islands': 'TC',
        'Wallis And Futuna': 'WF',
        'Isle Of Man': 'IM',
        'Timor-Leste (East Timor)': 'TP',
        'Antarctica': 'QO',
        'Kosovo': 'XK',
        'Saint Pierre And Miquelon': 'PM',
        'Sao Tome And Principe': 'ST',
        'Virgin Islands Us': 'VI',
        'United States Minor Outlying Islands': 'QO'
    }

    # Vérifiez si le pays est dans le dictionnaire de mappage personnalisé
    if country_name in custom_country_mapping:
        country_code_alpha2 = custom_country_mapping[country_name]
    else:
        # Si le pays n'est pas dans le mappage personnalisé, essayez de l'obtenir à partir du nom complet du pays
        try:
            country_code_alpha2 = pc.country_name_to_country_alpha2(country_name)
        except:
            return None

    try:
        continent_code = pc.country_alpha2_to_continent_code(country_code_alpha2)
        return pc.convert_continent_code_to_continent_name(continent_code)
    except:
        return None

# Enregistrer la fonction UDF
udf_get_continent = udf(get_continent, StringType())
# Ajouter une nouvelle colonne "Continent" à partir de la colonne "country"
airports_df_with_zones = (airports_df.withColumn("continent", udf_get_continent(airports_df["country"]))
                          .withColumnRenamed("name", "name_airport")
                          .filter(col("continent").isNotNull()))

def flights_with_distance():
    def calculate_distance(lat1, lon1, lat2, lon2):
        coords_1 = (lat1, lon1)
        coords_2 = (lat2, lon2)
        return geopy.distance.geodesic(coords_1, coords_2).kilometers

    udf_calculate_distance = udf(calculate_distance, DoubleType())

    res_flights_with_distance = (((flights_df_filter.join(airports_df_with_zones,
                                                  flights_df_filter["origin_airport_iata"] == airports_df_with_zones["iata"],
                                                  "left")
                                  .join(airports_df_with_zones.withColumnRenamed("iata", "dest_iata")
                                        .withColumnRenamed("latitude", "dest_latitude")
                                        .withColumnRenamed("longitude", "dest_longitude")
                                        .withColumnRenamed("name_airport", "dest_airport_name")
                                        .drop(col("continent")),
                                        flights_df_filter["destination_airport_iata"] == col("dest_iata"), "left"))
                                 .withColumn("distance",
                                             udf_calculate_distance(airports_df_with_zones["latitude"],
                                                                    airports_df_with_zones["longitude"],
                                                                    col("dest_latitude"), col("dest_longitude"))))
                                 .filter(col("continent").isNotNull()))

    return res_flights_with_distance

def join_between_3_df():
    # Jointure entre les 3 dataframes pour obtenir le continent de départ et de destination, ainsi que le nom de la compagnie
    df_joined = (((flights_df_filter.withColumnRenamed("latitude", "lat").withColumnRenamed("longitude", "log")
                   .join(airports_df_with_zones,
                         flights_df_filter["origin_airport_iata"] == airports_df_with_zones["iata"], "left"))
                  .join(airlines_df, flights_df_filter["airline_icao"] == airlines_df["icao"], "left"))
                 .join(airports_df_with_zones
                       .withColumnRenamed("iata", "dest_iata").withColumnRenamed("country", "dest_country")
                       .withColumnRenamed("continent", "dest_continent").withColumnRenamed("latitude", "dest_latitude")
                       .withColumnRenamed("longitude", "dest_longitude"),
                       flights_df_filter["destination_airport_iata"] == col("dest_iata"), "left"))
    return df_joined

class Transform:
    @staticmethod
    def get_first_compagny_in_progress():
        flights_in_progress = (flights_df_filter
                               .select(col('airline_icao')).groupby(col('airline_icao'))
                               .count().sort(col('count').desc()))

        ## jointure avec airlines table pour avoir le nom de compagnie:
        compagny_flights_in_progress = flights_in_progress.join(airlines_df, col('airline_icao') == col('icao'), 'left')

        first_compagny_with_flight_in_progress = (compagny_flights_in_progress
                                                  .sort(col('count').desc()).select('name', 'icao', 'count').first())

        return first_compagny_with_flight_in_progress

    @staticmethod
    def compagny_with_most_reginals_flight():
        df_joined = join_between_3_df()
        # Filtrer les vols régionaux actifs (continent d'origine == continent de destination)
        df_regional_flights = df_joined.filter(col("continent") == col("dest_continent"))
        # df_regional_flights.show(6)
        # Compter le nombre de vols régionaux par compagnie et continent
        result_df = df_regional_flights.groupBy("continent", "Name").agg(count("*").alias("num_regional_flights"))
        window_spec = Window.partitionBy("continent").orderBy(col("num_regional_flights").desc())

        result_df = result_df.withColumn("rank", dense_rank().over(window_spec)) \
            .filter(col("rank") == 1).drop("rank")
        return result_df

    @staticmethod
    def flight_max_distance():
        # Ajouter une colonne "distance" au DataFrame df_flights en utilisant les coordonnées des aéroports
        flights_df_with_distance = flights_with_distance()

        # Trouver le vol avec la distance maximale (trajet le plus long)
        max_distance_flight = (flights_df_with_distance.orderBy(col("distance").desc())
                               .select("aircraft_code", "name_airport", "dest_airport_name", "distance").first())
        return max_distance_flight

    @staticmethod
    def flights_avg_per_continent():
        flights_df_with_distance = flights_with_distance()
        flights_avg_per_continent = flights_df_with_distance.groupBy("continent").agg(avg("distance"))
        return flights_avg_per_continent

    @staticmethod
    def get_constructor_with_most_active_flight():
        joined_df = (flights_df_filter
                     .join(airlines_df,
                           flights_df_filter["airline_icao"] == airlines_df["icao"], "left"))
        constructor_with_most_active_flights = joined_df.groupBy("Name").agg(
            count("*").alias("active_flights_count")).orderBy(col("active_flights_count").desc()).first()
        return constructor_with_most_active_flights

    @staticmethod
    def get_country_constructor_top_3_airplane_model():
        df_joined = join_between_3_df()
        grouped_df = df_joined.groupBy("country", "aircraft_code").agg(count("*").alias("num_active_flights"))
        window_spec = Window.partitionBy("country").orderBy(col("num_active_flights").desc())
        res_df = grouped_df.withColumn("rank", dense_rank().over(window_spec)).filter(col("rank") <= 3).orderBy(
            "country", "rank")
        return res_df


if __name__ == "__main__":
    try:
        # Exécutez les méthodes de la classe Transform pour obtenir les résultats
        res1 = Transform.get_first_compagny_in_progress()
        logger.info("La compagnie avec le + de vols en cours : %s", res1)

        res2 = Transform.compagny_with_most_reginals_flight()
        logger.info("Pour chaque continent, la compagnie avec le + de vols régionaux actifs :")
        res2.show()

        res3 = Transform.flight_max_distance()
        logger.info("Le vol en cours avec le trajet le plus long : %s", res3)

        res4 = Transform.flights_avg_per_continent()
        logger.info("Pour chaque continent, la longueur de vol moyenne :")
        res4.show()

        res5 = Transform.get_constructor_with_most_active_flight()
        logger.info("L'entreprise constructeur d'avions avec le plus de vols actifs : %s", res5)

        res6 = Transform.get_country_constructor_top_3_airplane_model()
        logger.info("Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage :")
        res6.show()

    except Exception as e:
        logger.error("Une erreur s'est produite : %s", str(e))

## vu le rendu des résultats je ne pense pas que se soit pertinent de charger les données dans un espace

## on pourrait ordonancer et écrire le resultat partitionné par année, mois, jour et heure
# Ajoutez des colonnes pour l'année, le mois, le jour et l'heure à partir de la colonne de timestamp
# qui elle-meme est récupéré à partir de time dans flights_df
# df = df.withColumn("timestamp", from_unixtime("time"))
# df = df.withColumn("annee", year("timestamp"))
# df = df.withColumn("mois", month("timestamp"))
# df = df.withColumn("jour", dayofmonth("timestamp"))
# df = df.withColumn("heure", hour("timestamp"))

# Concaténez les colonnes "annee", "mois", "jour" pour créer la colonne "situationdate"
# df = df.withColumn("situationdate", concat_ws("/", "annee", "mois", "jour"))


# Écrivez le DataFrame partitionné au format Parquet
# Assurez-vous de spécifier le chemin où vous souhaitez enregistrer les partitions
# df.write.partitionBy("situationdate", "heure").parquet("chemin/vers/le/dossier/parquet")
