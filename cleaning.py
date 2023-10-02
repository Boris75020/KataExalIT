from pyspark.sql import dataframe
from pyspark.sql import functions as F
from pyspark.sql import types as T

from functions import get_continent


class Clean:
    @staticmethod
    def clean_flights(flights_df: dataframe):
        flights_df_filter = (flights_df.filter((F.col("origin_airport_iata") != "N/A") &
                                               (F.col("destination_airport_iata") != "N/A") &
                                               (F.col('airline_icao') != 'N/A')))
        return flights_df_filter

    @staticmethod
    def clean_airlines(airlines_df: dataframe):
        airlines_df_filter = (airlines_df.filter((F.col("icao") != "N/A")))
        return airlines_df_filter

    @staticmethod
    def clean_and_add_continent_to_airports(airports_df: dataframe):
        # Enregistrer la fonction UDF
        udf_get_continent = F.udf(get_continent, T.StringType())

        # Ajouter une nouvelle colonne "Continent" à partir de la colonne "country" et filtre
        airports_df_with_zones = (airports_df.filter((F.col("icao") != "N/A"))
                                  .withColumn("continent", udf_get_continent(airports_df["country"]))
                                  .withColumnRenamed("name", "name_airport")
                                  .filter(F.col("continent").isNotNull()))

        return airports_df_with_zones
