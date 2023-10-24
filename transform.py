from pyspark.sql.window import Window

from functions import *


class Transform:
    @staticmethod
    def get_first_compagny_in_progress(flights_df_filter: dataframe, airlines_df_filter: dataframe):
        flights_in_progress = (flights_df_filter
                               .select(F.col('airline_icao')).groupby(F.col('airline_icao'))
                               .count().sort(F.col('count').desc()))

        # jointure avec airlines table pour avoir le nom de compagnie:
        compagny_flights_in_progress = flights_in_progress.join(airlines_df_filter,
                                                                F.col('airline_icao') == F.col('icao'),
                                                                'left')

        first_compagny_with_flight_in_progress = (compagny_flights_in_progress
                                                  .sort(F.col('count').desc()).select('name', 'icao', 'count').first())

        return first_compagny_with_flight_in_progress

    @staticmethod
    def compagny_with_most_reginals_flight(flights_df_filter: dataframe, airlines_df_filter: dataframe,
                                           airports_df_with_zones: dataframe):
        df_joined = join_between_3_df(flights_df_filter, airlines_df_filter,
                                      airports_df_with_zones).persist()
        # Filtrer les vols régionaux actifs (continent d'origine == continent de destination)
        df_regional_flights = df_joined.filter(F.col("continent") == F.col("dest_continent"))

        # Compter le nombre de vols régionaux par compagnie et continent
        result_df = df_regional_flights.groupBy("continent", "Name").agg(F.count("*").alias("num_regional_flights"))
        window_spec = Window.partitionBy("continent").orderBy(F.col("num_regional_flights").desc())

        result_df = result_df.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1).drop("rank")
        return result_df

    @staticmethod
    def flight_max_distance(flights_df_filter: dataframe, airports_df_with_zones: dataframe):
        # Ajouter une colonne "distance" au DataFrame df_flights en utilisant les coordonnées des aéroports
        flights_df_with_distance = flights_with_distance(flights_df_filter, airports_df_with_zones).persist()

        # Trouver le vol avec la distance maximale (trajet le plus long)
        max_distance_flight = (flights_df_with_distance.orderBy(F.col("distance").desc())
                               .select("aircraft_code", "name_airport", "dest_airport_name", "distance").first())
        return max_distance_flight

    @staticmethod
    def flights_avg_per_continent(flights_df_filter: dataframe, airports_df_with_zones: dataframe):
        flights_df_with_distance = flights_with_distance(flights_df_filter, airports_df_with_zones)
        flights_avg_per_continent = flights_df_with_distance.groupBy("continent").agg(F.avg("distance"))
        return flights_avg_per_continent

    @staticmethod
    def get_constructor_with_most_active_flight(flights_df_filter: dataframe, airlines_df_filter: dataframe):
        joined_df = (flights_df_filter
                     .join(airlines_df_filter,
                           flights_df_filter["airline_icao"] == airlines_df_filter["icao"], "left"))
        constructor_with_most_active_flights = joined_df.groupBy("Name").agg(
            F.count("*").alias("active_flights_count")).orderBy(F.col("active_flights_count").desc()).first()
        return constructor_with_most_active_flights

    @staticmethod
    def get_country_constructor_top_3_airplane_model(flights_df_filter: dataframe, airlines_df_filter: dataframe,
                                                     airports_df_with_zones: dataframe):
        df_joined = join_between_3_df(flights_df_filter, airlines_df_filter,
                                      airports_df_with_zones)
        grouped_df = df_joined.groupBy("country", "aircraft_code").agg(F.count("*").alias("num_active_flights"))
        window_spec = Window.partitionBy("country").orderBy(F.col("num_active_flights").desc())
        res_df = grouped_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= 3).orderBy(
            "country", "rank")
        return res_df
