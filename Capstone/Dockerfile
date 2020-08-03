FROM openjdk:8u262-jdk

ARG ZEPPELIN_VERSION="0.9.0-preview1"
ARG SPARK_VERSION="2.3.2"
ARG ZEPPELIN_NOTEBOOK_PATH="zeppelin/Capstone.zpln"

ENV ZEPPELIN_ADDR="0.0.0.0" \
 ZEPPELIN_HOME="/usr/local/zeppelin" \
 ZEPPELIN_NOTEBOOKS="/usr/local/zeppelin/notebook" \
 SPARK_HOME="/usr/local/spark"

# install Zeppelin
RUN wget --quiet http://archive.apache.org/dist/zeppelin/zeppelin-${ZEPPELIN_VERSION}/zeppelin-${ZEPPELIN_VERSION}-bin-all.tgz && \
 tar -zxf zeppelin-${ZEPPELIN_VERSION}-bin-all.tgz -C /usr/local/ && \
 mv /usr/local/zeppelin* ${ZEPPELIN_HOME}

# install Spark
RUN wget --quiet https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
 tar -zxf spark-${SPARK_VERSION}-bin-hadoop2.6.tgz -C /usr/local/ && \
 mv /usr/local/spark* ${SPARK_HOME}

EXPOSE 8080

COPY ${ZEPPELIN_NOTEBOOK_PATH} ${ZEPPELIN_NOTEBOOKS}
WORKDIR ${ZEPPELIN_HOME}

ENTRYPOINT [ "./bin/zeppelin.sh" ]




