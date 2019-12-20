FROM alexsni/spark:palin_v2.4.0
MAINTAINER Alexander.Snisar@gmail.com

ENV HOME /root

COPY target/spark-job-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/spark/jars/spark-job-1.0-SNAPSHOT-jar-with-dependencies.jar