# Sample Flink Project


This project is a sample project for data stream processing using Apache Flink. Main application parses the 
Sales Record from csv files aggregates the total revenue and updates values in MongoDB. This all happens in 
streaming mode and database is updated after processing each records.

##Dataset
Sample [csv data](https://github.com/rklamichhane/apache-flink-example-project/blob/master/data/50000%20Sales%20Records.csv) 
is in data folder and about 6MB in size.

##Usage
This project is tested with flink-1.9.3. [Download](https://flink.apache.org/downloads.html) this version of flink and add jar files inside lib folder of flink as 
dependency. MongoDB driver `org.mongodb:mongodb-driver-sync:3.7` is also required.
###Configuration Required
Following configuration in [config.properties](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/resources/config.properties) is required for MongoDB. Custom 
properties file could be passed from command line with `--config`. If not passed property file from resources folder is used.
```$xslt
db.url=mongodb://localhost:27017
db.user=rk
db.password=Secure@123!
db.database=admin
db.collection=country_with_revenue
```
Mapping for csv headers to columns in file is defined [here](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/resources/csv-column-map.properties) for flexible input. You can pass your own mapping with command line argument
`--mapping`. If not provided, default mapping in resources folder is used.
Main application for this project is [CountryRevenueCalculator.java](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/java/streamapp/CountryRevenueCalculator.java) .
