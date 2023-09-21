from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from FlightRadar24 import FlightRadar24API

spark = SparkSession.builder.appName("Chargement des données FlightRadar").getOrCreate()

fr_api = FlightRadar24API()

class Extract:

    def flights_extraction(self):
        # chargement des données de vols et création du dataframe
        flights = fr_api.get_flights()
        flights_df = spark.createDataFrame(flights)
        return flights_df

    def airports_extraction(self):
    # chargement des données d'aéroports et création du dataframe
        airports = fr_api.get_airports()
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
        airports_df = spark.createDataFrame(airports, airports_schema)
        return airports_df

    def airlines_extraction(self):
        # chargement des données compagnies et création du dataframe
        airlines = fr_api.get_airlines()
        airlines_df = spark.createDataFrame(airlines)
        return airlines_df
