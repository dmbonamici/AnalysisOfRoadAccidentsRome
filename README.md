# AnalysisOfRoadAccidentsRome
Project analysis of road accidents in the municipality of Rome. Using technologies to the environment BigData. 

Case Study
==============
Among all forms of transport systems, the road is the most dangerous and causes the most fatalities. The safety car was penalized by
misconduct driving (not the subject of our analysis): Excess speed, alcohol or drug abuse, falling asleep or driver distraction, which in most cases is carried out for the use of cell phones while driving.
Analyzing the ISTAT data and the local police of Roma Capitale, there are no doubts: drive a car, a motorcycle or a bicycle in Rome means putting their lives at risk. The data do not improve if you decide to walk: the rate of Mortality Roman pedestrians is among the highest in Italy.

Having this data, we realized that the problem of the road accidents in Rome was an issue that deserved greater attention and that if we had as a detection unit single incident with the various information we have a deeper analysis, not only quantitative but also qualitative.


The Dataset
==============
At our end, we came to the [Open Data Rome](http://dati.comune.roma.it/) for help: a project promoted by Roma Capitale aimed at the promotion and dissemination in digital and open format the databases of the Public Administration. They have found a dataset accidents in json format, from January 2006 to May 2016. For each incident we have 4 distinct json, each with ID unique protocol, which contain information on:
- information on the incident (nature, paving, signage, conditions weather etc.).
- information on the people involved in a vehicle (age, sex, driver/passenger etc.)
- Information on pedestrians (if involved)
- Information on the vehicles involved (model, type, etc.)

Target
==============
In early work, looking at the data in the dataset, we have set ourselves some very specific questions:
- WHY 'all these incidents occur? What have been the causes that led to the left? atmospheric factors? Uneven road surface? Or simply, the inability of the driver?

- WHO are the people most likely to carry the incidents in Rome? Is there any correlation between them?
- HOW the accident occurred? What was your type.
- WHEN did it happen? At what time and day end, it has the highest percentage of accidents in Rome?
- And most importantly WHERE?

We have focused on where, why we felt it important to make a study of analysis, doing an aggregation of the causes and reasons of accidents in the capital, for the various municipalities with which Rome is divided.
Just to do the analysis and comparison of the 15 municipalities of Rome, our work has been divided into three main steps, which were carried out in a sequential manner to achieve our goal:

1. Storage of open data in MongoDB.
2. Basic Analytics
3. Advanced Analytics


STEP 1 - Storage in MongoDB
==============
At this stage we had some difficulties due to two main issues:

1. .json files downloaded were not formatted properly. Within the values ​​they were used the 'double quotes', which in the json file are used to define key and value. To overcome this problem has been implemented a script, which was to perform the cleaning operation.

2. The "Group" of the various "Incidenti.json" file is an extremely important area for the development of our project, in fact refers to the municipal group that intervened in the scene, and that in 95% of cases is the actual town hall where the accident occurred. The initial problem due to this field, is that referred to the old division of the municipalities of Rome for all the incidents that had occurred before 2013 (the date on which entered into force the new subdivision). To solve this problem, we used the MongoDB operators to bring each incident to the current municipal subdivision.

![Suddivione](http://i.imgur.com/vrVutBP.png)

STEP 2 - Basic Analytics
==============

The second step was to develop and transform the data stored inside of the database in order to make them suitable for display operations and basic monitoring. For this purpose was used [Apache Spark](http://spark.apache.org/), for its efficiency thanks to the ability to exploit the main memory to process large amounts of data. Thanks to "[MongoDB Connector for Hadoop](https://github.com/mongodb/mongo-hadoop)" connector was possible to interact the two systems in the stage of reading input and output writing.


STEP 3 - Advanced Analytics
==============
Work in progress...
