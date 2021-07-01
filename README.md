# Flight data aggregation

There is batch flight data coming in every month and needs to be enriched with the following features:  
a) Impose proper schema to the raw dataset with proper types.  
b) All datetime formats should be in the following format yyyy-mm-dd HH:mm:ss  
c) Can you calculate the KPI when the arrival of aircraft is delayed for more than 20
minutes, what caused that delay?  
d) Can you also calculate, how many flight arrivals are cancelled & delayed?  
e) Upon re-run data can’t be duplicated in the sql table, please make sure while
implementing this as part of your code?  
f) Store the enriched datasets in sql server using spark-scala?  

# Data Structure - Important points

As per the use case, here is the description of important data fields:
ArrDelay - Difference in minutes between scheduled and actual arrival time. Early arrivals show negative numbers, in minutes
DepDelay - Difference in minutes between scheduled and actual departure time.Early departures show negative numbers, in minutes

ArrDelay = CarrierDelay + WeatherDelay + NASDelay + SecurityDelay + LateAircraftDelay

# Pre-requisites

- IntelliJ IDE
- Spark 2.4.0
- Scala 2.11.6
- SQL server
  - server_name = "jdbc:sqlserver:///localhost:1433"
  - database_name = "test"
  - table_name = "flight_data"
  - username = "admin"
  - password = "admin123"

# Setup Instructions

1.Begin by cloning or downloading the tutorial GitHub project https://github.com/priyanshuHasija/codetest.  
2. Import project into IntelliJ IDE and let the Maven download all the dependencies.  
3. Once completed, check for if there are any error in the dependency resolution.  
4. After project is successfully imported. You can execute the project by running following command:  

spark-submit --class org.example.CodeTest --deploy-mode local[*] CodeAssignment.jar  
OR  
spark-submit --class org.example.CodeTest --deploy-mode client CodeAssignment.jar  
OR  
spark-submit --class org.example.CodeTest --deploy-mode cluster CodeAssignment.jar  

# Logic for calculation KPI, cancelled flights, derived flights

- KPI - Calculate percentage for each factor for each row. Then, calculate the highest percentage column as kpi column. Group the data for each date and get the kpi column value which is present most times.
- cancelled flights - Group each day data and calculate the count of flights where cancelled flag is true
- diverted flights - Group each day data and calculate the count of flights where diverted flag is true

To calculate the final dataset, Join above three dataframes.

# Logic for upsert into sql server

1. Read the previously loaded data from sql server.
2. Match the incoming data with previous data to get the updated and new records and consolidate it into one dataframe.
3. Match the incoming data with previous data to get previous records that are not update
4. Union all 3 and overwrite into database

# Future changes that needs to be implemented
- Create config file and read input path, target details, sql server details from config


