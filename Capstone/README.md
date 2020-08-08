# Udacity's DEND Capstone Project 

This repository depicts the final Project of Udacity's Data Engineering Nanodegree. The Project's goal is to built an ETL Pipeline using [Spark][spark link] 
on provided Datasets of [immigration][immigration data link] and [temperature][temperature data link] data. A main focus, 
besides the fulfillement of the project goals, is to show the ability to cope with proper dependency management, 
testing, containerization and building of generic functions and objects which could be re-used.

Project requirements are listed and explained in zeppelin/Capstone.zpln in the same manner
as the provided Jupyter Notebook Project Template from Udacity.  

## Overview & Architecture
The project is structured as Maven Project, using Scala as programming language and Spark API. 
```
Capstone
    |-- data
    |-- src
         |-- main
              |-- scala
         |-- test
              |-- resources
              |-- scala
    |-- target
    |-- zeppelin
         |--Capstone.zpln
    |-- Dockerfile
    |-- pom.xml
```
* data - datasets go into this folder (not pushed to git due size)
* src - Scala production & testing code go here, Data Snippets for tests go underneath test/resources
* target - Maven builded Jar is created in target in package lifecycle
* pom.xml - All dependencies and plugins are to build Jar go here
* zeppelin - holds the Zeppelin Notebook (Capstone.zpln) file to be imported to Zeppelin instance
* Dockerfile - Dockerfile to built Image with Spark and Zeppelin dependencies



## Prerequisites
In order to build the Project on your local machine, you need to install
* [Maven 3.6.3][maven link] 
* [Docker Desktop][docker link]
* [Scala][scala link] (2.12 with JDK 8 was used for development)

## Setup

### Add Maven Dependency

Add https://spark-packages.org/package/saurfang/spark-sas7bdat to local .m2 Maven Repository (since Artifactory is different from Maven Central).
This dependency is needed to read .sas7bdat files from the immigration dataset.

```bash
mvn install:install-file \
   -Dfile=path/spark-sas7bdat-2.1.0-s_2.11.jar \
   -DgroupId=saurfang \
   -DartifactId=spark-sas7bdat \
   -Dversion=2.1.0 \
   -Dpackaging=jar \
   -DgeneratePom=true
```

### Build Project Jar
In order to build the Jar with all transitive dependencies in your target directory, run
```bash
mvn clean install/package
```
If you just want to run all tests in Capstone/src/test/scala, you can run
```bash
mvn clean test
```
from root path of project.

### Build Docker Image
A Dockerfile is provided to spin up a Container with [Spark][spark link] dependencies and a running [Zeppelin][zeppelin link] Notebook instance.
To build the according Docker image, please run 
```bash
docker build --tag <tagName> .
```
from the root path of the project directory.

### Run Docker Image

Following command will run a Docker Container out of your previously build image. This will mount port 8080 on your local machine 
and mount the /data and /target directory to your container (Note: Data not included in repo due to size)
```bash
docker run \
  -p 8080:8080 \ 
  -v `pwd`/data:/usr/local/data/ \
  -v `pwd`/target:/opt/lib/ \
  -d \ 
  <tagName or imageID> 
```

### Configuring Zeppelin inside Docker Container
After Docker Container is running, you can access the Zeppelin Instance in your Browser in 
http://localhost:8080/ (or different port you forwarded on your local machine).

#### Import Zeppelin Notebook Template

Import the Note in zeppelin/Capstone.zpln to the running Zeppelin instance in http://localhost:8080/.
You can use the Zeppelin UI to do this:
![Alt text](images/zeppelin_import_note.png?raw=true "zeppelin import note")

#### Provide project Jar to Zeppelin Instance

You need to provide the Spark Interpreter inside the Zeppelin, running in the Docker Container,
the project Jar, you have built before using Maven.

    | name                   | value                                                       | type     |
    | ---------------------- |-------------------------------------------------------------| -------- |
    | spark.jars             | /opt/lib/capstone-project-jar-with-dependencies.jar         | textarea |

To adjust the Jars for your Spark Interpreter, you can use the Zeppelin UI as depicted:
![Alt text](images/zeppelin_spark_jar.png?raw=true "Spark Interpreter Jars added")

 
[maven link]: https://maven.apache.org/download.cgi
[docker link]: https://www.docker.com/products/docker-desktop
[zeppelin link]: https://zeppelin.apache.org/
[spark link]: https://spark.apache.org/
[scala link]: https://www.scala-lang.org/download/
[immigration data link]: https://travel.trade.gov/research/reports/i94/historical/2016.html
[temperature data link]: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data
