### Java test code for Appsflyer

#### Two directories with individual projects for read and write.

* Set AEROSPIKE_HOST and AEROSPIKE_PORT as environment variables

* To write records
``` 
cd write
mvn clean compile assembly:single
java -jar target/ClientWrite-1.0-SNAPSHOT-jar-with-dependencies.jar
```
* Can specify the number of eventloop threads and the number of records to write
* as commandline arguments

* To read records
``` 
cd read
mvn clean compile assembly:single
java -jar target/ClientRead-1.0-SNAPSHOT-jar-with-dependencies.jar
```
* Can specify the number of eventloop threads and the number of records to write
* as commandline arguments
