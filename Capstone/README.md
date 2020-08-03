# Udacity's DEND Capstone Project 


## Prerequisites
In order to build the Project on your local machine, you need to install
* [Maven 3.6.3][maven link] 
* [Docker Desktop][docker link]

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

### Build Docker Image
A Dockerfile is provided to spin up a Container with [Spark][spark link] dependencies and a running [Zeppelin][zeppelin link] Notebook instance.
To build the according Docker image, please run 
```bash
docker build --tag <tagName> .
```
from the root path of the project directory.

### Run Docker Image

Following command will run a Docker Container out of your previously build image. This will mount port 8080 on your local machine 
and mount the /data directory to your container (Note: Data not included in repo due to size)
```bash
docker run \
  -p 8080:8080 \ 
  -v `pwd`/data:/usr/local/data/ \
  -d \ 
  <tagName or imageID> 
```

### Configuring Zeppelin inside Docker Container

You need to provide the Spark Interpreter inside the Zeppelin, running in the Docker Container,
the following Jar, you have built before using Maven.

    | name                   | value                                         | type     |
    | ---------------------- |-----------------------------------------------| -------- |
    | spark.jars             | /opt/lib/capstone.jar                         | textarea |
    
Import the Note in zeppelin/Capstone.zpln to the Running Zeppelin instance in http://localhost:8080/


[maven link]: https://maven.apache.org/download.cgi
[docker link]: https://www.docker.com/products/docker-desktop
[zeppelin link]: https://zeppelin.apache.org/
[spark link]: https://spark.apache.org/
