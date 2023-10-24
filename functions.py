import pycountry_convert as pc
import datetime

from pyspark.sql import dataframe
from pyspark.sql import functions as F

# Obtenir la date et l'heure actuelles
date_heure_actuelle = datetime.datetime.now()


def add_datetime_in_dataframe(df: dataframe):
    df = (((df.withColumn("current_datetime", F.lit(date_heure_actuelle))
           .withColumn("situationdate", F.date_format(F.col("current_datetime"), "yyyy-MM-dd")))
          .withColumn("heure", F.date_format(F.col("current_datetime"), "HH")))
          .persist())
    return df


def get_continent(country_name: F.col):
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


def calculate_distance(lat1: F.col, long1: F.col, lat2: F.col, long2: F.col):
    # Rayon de la Terre en kilomètres
    r = 6373.0

    lat1 = F.radians(lat1)
    lon1 = F.radians(long1)
    lat2 = F.radians(lat2)
    lon2 = F.radians(long2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    distance = r * c
    return distance


def flights_with_distance(flights_df_filter: dataframe, airports_df_with_zones: dataframe):

    res_flights_with_distance = (((flights_df_filter.join(airports_df_with_zones,
                                                          flights_df_filter["origin_airport_iata"] ==
                                                          airports_df_with_zones["iata"],
                                                          "left")
                                   .join(airports_df_with_zones.withColumnRenamed("iata", "dest_iata")
                                         .withColumnRenamed("latitude", "dest_latitude")
                                         .withColumnRenamed("longitude", "dest_longitude")
                                         .withColumnRenamed("name_airport", "dest_airport_name")
                                         .drop(F.col("continent")),
                                         flights_df_filter["destination_airport_iata"] == F.col("dest_iata"), "left"))
                                  .withColumn("distance",
                                              calculate_distance(airports_df_with_zones["latitude"],
                                                                 airports_df_with_zones["longitude"],
                                                                 F.col("dest_latitude"), F.col("dest_longitude"))))
                                 .filter(F.col("continent").isNotNull()))

    return res_flights_with_distance


def join_between_3_df(flights_df_filter: dataframe, airlines_df_filter: dataframe, airports_df_with_zones: dataframe):
    # Jointure entre les 3 dataframes pour obtenir le continent de départ et de destination,
    # ainsi que le nom de la compagnie
    df_joined = (((flights_df_filter.withColumnRenamed("latitude", "lat").withColumnRenamed("longitude", "log")
                   .join(airports_df_with_zones,
                         flights_df_filter["origin_airport_iata"] == airports_df_with_zones["iata"], "left"))
                  .join(airlines_df_filter, flights_df_filter["airline_icao"] == airlines_df_filter["icao"], "left"))
                 .join(airports_df_with_zones
                       .withColumnRenamed("iata", "dest_iata").withColumnRenamed("country", "dest_country")
                       .withColumnRenamed("continent", "dest_continent").withColumnRenamed("latitude", "dest_latitude")
                       .withColumnRenamed("longitude", "dest_longitude"),
                       flights_df_filter["destination_airport_iata"] == F.col("dest_iata"), "left"))
    return df_joined
