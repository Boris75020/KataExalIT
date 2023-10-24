import logging

from transform import *
from FlightRadar24 import FlightRadar24API

from extract import Extract
from cleaning import Clean

from pyspark.sql import SparkSession

# Initialisez SparkSession
spark = SparkSession.builder.appName("lancement du traitement").getOrCreate()

fr_api = FlightRadar24API()
maClasseExtract = Extract(fr_api, spark)
flights_df = maClasseExtract.flights_extraction()
airports_df = maClasseExtract.airports_extraction()
airlines_df = maClasseExtract.airlines_extraction()

# maClasseClean = Clean(flights_df, airlines_df, airports_df)
flights_df_filter = Clean.clean_flights(flights_df)
airlines_df_filter = Clean.clean_airlines(airlines_df)
airports_df_with_zones = Clean.clean_and_add_continent_to_airports(airports_df)

# Configuration de la journalisation
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # Exécutez les méthodes de la classe Transform pour obtenir les résultats
        res1 = Transform.get_first_compagny_in_progress(flights_df_filter, airlines_df_filter)
        logger.info("La compagnie avec le + de vols en cours : %s", res1)

        # Spécifiez le chemin du fichier de sortie en local
        output_file_path = "D:/Users/Joseph/Documents/exercice_python/KataExalIT/DataFlightRadar/result"

        # Créez un DataFrame à partir du résultat
        res1_df = spark.createDataFrame([res1], ["name", "icao", "count"])

        # Écrivez le DataFrame dans le fichier CSV en local
        # res1_df.write.csv(output_path, header=True, mode="overwrite")

        res2 = Transform.compagny_with_most_reginals_flight(flights_df_filter, airlines_df_filter,
                                                            airports_df_with_zones)
        logger.info("Pour chaque continent, la compagnie avec le + de vols régionaux actifs :")

        # res2.write.csv(output_file_path, header=True, mode="append")
        res2.show()

        res3 = Transform.flight_max_distance(flights_df_filter, airports_df_with_zones)
        logger.info("Le vol en cours avec le trajet le plus long : %s", res3)

        # Créez un DataFrame à partir du résultat
        res3_df = spark.createDataFrame([res3], ["aircraft_code", "name_airport", "dest_airport_name", "distance"])

        # Écrivez le DataFrame dans le fichier CSV en local
        # res3_df.write.csv(output_file_path, header=True, mode="append")

        res4 = Transform.flights_avg_per_continent(flights_df_filter, airports_df_with_zones)
        logger.info("Pour chaque continent, la longueur de vol moyenne :")
        res4.show()

        # res4.write.csv(output_file_path, header=True, mode="append")

        res5 = Transform.get_constructor_with_most_active_flight(flights_df_filter, airlines_df_filter)
        logger.info("L'entreprise constructeur d'avions avec le plus de vols actifs : %s", res5)

        # Créez un DataFrame à partir du résultat
        res5_df = spark.createDataFrame([res5], ["name", "active_flights_count"])

        # Écrivez le DataFrame dans le fichier CSV en local
        # res5_df.write.csv(output_file_path, header=True, mode="append")

        res6 = Transform.get_country_constructor_top_3_airplane_model(flights_df_filter, airlines_df_filter,
                                                                      airports_df_with_zones)
        logger.info("Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage :")
        # res6.write.csv(output_file_path, header=True, mode="append")
        res6.show()

    except Exception as e:
        logger.error("Une erreur s'est produite : %s", str(e))
        raise e
