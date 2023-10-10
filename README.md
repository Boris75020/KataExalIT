# KataExalIT
Ingestion FlightRadar24 with Pyspark

## pré-requis :
lancer la commande :
pip install requirements.txt
dans le terminal pour avoir les bons packages

### Extraction
Récupération des données de l'API via la class extract
création de 3 dataFrames :
Flights, airports et airlines
stockage des données dans un espace partitionné par {année-mois-jour}/heure.

### Cleaning
filtre pour retirer des valeurs qui entrainent des inchorénces dans l'analyse

### functions
les fonctions dont j'ai besoin pour répondre aux questions et qui peuvent etre réutilisées

### Transformation
Nettoyage de la donnée, transformation pour répondre aux questions suivnates:

1 - La compagnie avec le + de vols en cours

2 - Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)

3 - Le vol en cours avec le trajet le plus long

4 - Pour chaque continent, la longueur de vol moyenne

5 - L'entreprise constructeur d'avions avec le plus de vols actifs

6 - Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage

affichage des resultats

### main
session pour l'affichage des résultats et logging pour d'éventuels erreurs
