# KataExalIT
Ingestion FlightRadar24 with Pyspark

### Extraction
Récupération des données de l'API via la class extract
et création de 3 dataFrames :
Flights, airports et airlines

### Transformation
Nettoyage de la donnée, transformation pour répondre aux questions suivnates:

1 - La compagnie avec le + de vols en cours

2 - Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)

3 - Le vol en cours avec le trajet le plus long

4 - Pour chaque continent, la longueur de vol moyenne

5 - L'entreprise constructeur d'avions avec le plus de vols actifs

6 - Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage

affichage des resultats

### Partie chargement non traité aux vues des résultats
Une explication a été donné dans la partie transform si besoin de chargement.

