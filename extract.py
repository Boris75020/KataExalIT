from pyspark.sql.types import StructType, StructField, StringType

from functions import add_datetime_in_dataframe


class Extract:
    def __init__(self, fr_api, spark):
        self.fr_api = fr_api
        self.spark = spark

    def flights_extraction(self):
        # chargement des données de vols et création du dataframe
        flights = self.fr_api.get_flights()
        flights_df = self.spark.createDataFrame(flights)
        flights_df = add_datetime_in_dataframe(flights_df)
        flights_parquet = (((flights_df.write
                             .mode("append"))
                            .partitionBy("situationdate", "heure"))
                           .parquet("D:/Users/Joseph/Documents/exercice_python/KataExalIT/DataFlightRadar/flights/"))
        return flights_df

    def airports_extraction(self):
        # chargement des données d'aéroports et création du dataframe
        airports = self.fr_api.get_airports()
        airports_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("iata", StringType(), True),
                StructField("icao", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("country", StringType(), True),
                StructField("altitude", StringType(), True),
            ]
        )
        airports_df = self.spark.createDataFrame(airports, airports_schema)
        airports_df = add_datetime_in_dataframe(airports_df)
        airports_parquet = (((airports_df.write
                              .mode("append"))
                             .partitionBy("situationdate", "heure"))
                            .parquet("D:/Users/Joseph/Documents/exercice_python/KataExalIT/DataFlightRadar/airports/"))
        return airports_df

    def airlines_extraction(self):
        # chargement des données compagnies et création du dataframe
        airlines = self.fr_api.get_airlines()
        airlines_df = self.spark.createDataFrame(airlines)
        airlines_df = add_datetime_in_dataframe(airlines_df)
        airlines_parquet = (((airlines_df.write
                              .mode("append"))
                             .partitionBy("situationdate", "heure"))
                            .parquet("D:/Users/Joseph/Documents/exercice_python/KataExalIT/DataFlightRadar/airlines/"))
        return airlines_df
