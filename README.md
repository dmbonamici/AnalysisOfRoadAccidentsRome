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

to do this, the part of the code called "CleanJson" was used.

STEP 2 - Basic Analytics
==============

The second step was to develop and transform the data stored inside of the database in order to make them suitable for display operations and basic monitoring. For this purpose was used [Apache Spark](http://spark.apache.org/), for its efficiency thanks to the ability to exploit the main memory to process large amounts of data. Thanks to "[MongoDB Connector for Hadoop](https://github.com/mongodb/mongo-hadoop)" connector was possible to interact the two systems in the stage of reading input and output writing.

In the basic analysis are 7 classes:

- AccidentsMunicipi: answers to the first question, how many accidents occur in each municipality? What is the municipality with the highest number of accidents?

- AccidentsCause: answers the question, how the accident occurred? What were the causes?

- AccidentsFasce: Which age group is more likely to have accidents in Rome?

- AccidentsGiorni: When accidents happen in Rome? He responds by breaking into days of the week and different time zones.

- AccidentsCond: A small study on road conditions at the time of the accident.

- AccidentsPedonali: Study on the causes that led to pedestrian accidents, broken down by municipality.

- Vehicles: class created to see what brands of vehicles, and in what quantity (cars, motorcycles and public transport), who carried out or have been involved in road accidents in Rome.



STEP 3 - Advanced Analytics
==============

Knowing the risk factors for the different zones (municipalities) that register in accident conditions with recursion characters or high volumes of incidents is important to be able to implement these corrective factors. In support of this we have decided to use [MLlib](http://spark.apache.org/mllib/), a library of machine learning algorithms for Apache Spark. Mllib has an implementation of FP-growth, a famous algorithm of Frequent Pattern Mining of which we used the "Association rule learning", a method to find interesting relationships between variables in large databases.

![Role](http://i.imgur.com/ncU6d5y.png)

X is called the "prior" and can have one or more items while Y is called the consequent and consists of a single item. The use of this technology was important to try to understand what are the main factors leading to the occurrence of certain types of accidents, two macro-categories were analyzed for these factors, in two different tasks both focused within a single municipality:

- The first concerning the combination lighting-floor;
- The second concerning sex-age drivers.

During a first analysis we realized that the probability of occurrence of a certain type of traffic accident seemed independent from both the driver's characteristics that the particular scenario where happened. In fact by the highest probability values obtained were always those regarding the types of claims with greater probability of occurrence (front-side collisions and side collisions), this is because the algorithm does not take into account the number of occurrences of the "consequent" in the calculation of "confidence". It was then carried out a normalization, taking into consideration the frequency of the various accidents. It is given a greater weight to the more rare incidents so as to determine how much influenced "effectively" the scenario considered in the occurrence of a certain type of event.

As regards the first task was calculated to normalize the probability (normConfidence) of each type of accident to occur with asphalt and paving daylight illumination (such as "antecedent" we would have the pair [asphalt, daytime hours] and as resulting in a different type of incident to every rule): factors considered optimal, and that they should not favor the occurrence of a particular type of incident. We realized that divide the confidence of each rule in the algorithm output, with the probability of occurrence of the type of accident in the "conseguent" (calculated for normalization) gave a too greater weight to the more rare accidents. So after several attempts it was realized that reasonable results were obtained using as the attenuation factor of the logarithm to the base 10 module.

![Role2](http://i.imgur.com/2TxLs0p.png)  with ruleConseguent = normConseguent. 

By analyzing the results it was possible to make the following considerations:
- For each municipality women between 40 and 69 years of age have a high rate to make a crash. It is observed, however, that in general women are more likely to carry these types of accidents than men.
- The boys between 18 and 25 years have a high incidental rate for front or side crashes (inexperience or high speed?)
- Women aged over 70 years have a predisposition toward incidents against parked car and fixed obstacles (decreased reflexes and eyesight?)


As for the second task as there is no agreement, in order to define an age or a sex for which a driver may be considered more suitable to the guide, it was decided to simply normalize for the frequency of occurrence of an accident, without taking into consider any other parameter. The formula is always the same as used for the previous task.
Three different observations found, we find:
- In Hall 7 almost all types of accidents occur due to insufficient lighting.
- Paved road and inadequate lighting are the main causes of pedestrian investment (in both conditions it has been shown that the driver has difficulty seeing the pedestrian crossing).
- The main cause of accidental collisions with obstacles is the road surface uneven (the problem of the holes in Rome is in fact one of the main problems with which they have to fight every day drivers of two- and four-wheel).
- In Hall 8 the presence of poor lighting and paved road on some routes has caused incidents involving the leakage from the road (lack of good illumininazione).

You can study these analyzes (and possibly find more correlations), using the "StatPers" classes (first task) and "StatIllumPav" (second task).
