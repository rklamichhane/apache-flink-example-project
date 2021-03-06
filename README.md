# Sample Flink Project #


This project is a sample project for data stream processing using Apache Flink. Main application parses the 
Sales Record from csv files aggregates the total revenue and updates values in MongoDB. This all happens in 
streaming mode and database is updated after processing each records.

## Dataset ##
Sample [csv data](https://github.com/rklamichhane/apache-flink-example-project/blob/master/data/50000%20Sales%20Records.csv) 
is in data folder and about 6MB in size.

## Usage ##
This project is tested with flink-1.9.3. [Download](https://flink.apache.org/downloads.html) this version of flink and add jar files inside lib folder of flink as 
dependency. MongoDB driver `org.mongodb:mongodb-driver-sync:3.7` is also required.
### Configuration Required ##
Following configuration in [config.properties](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/resources/config.properties) is required for MongoDB. Custom 
properties file should be passed from command line with `--config`. 
```$xslt
db.url=mongodb://localhost:27017
db.user=rk
db.password=Secure@123!
db.database=admin
db.collection=country_with_revenue
```
Mapping for csv headers to columns in file is defined [here](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/resources/csv-column-map.properties) for flexible input. You should pass your own mapping with command line argument
`--mapping`.
Main application for this project is [CountryRevenueCalculator.java](https://github.com/rklamichhane/apache-flink-example-project/blob/master/src/main/java/streamapp/CountryRevenueCalculator.java) .

Example command:
```>flink run out\artifacts\flink_example_project_jar\flink-example-project.jar --config "C:\Users\acer\Desktop\flink-example-project\src\main\r
   esources\config.properties" --mapping "C:\Users\acer\Desktop\flink-example-project\src\main\resources\csv-column-map.properties" --input "C:\Users\acer\Desktop\flink-example-project\da
   ta\records.csv"```