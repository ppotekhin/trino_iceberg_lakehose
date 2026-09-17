# Используем официальный образ Spark 4.1.3 с Python 3
FROM apache/spark:4.1.3-python3

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

ENV ICEBERG_VERSION=1.11.0
ENV SPARK_RUNTIME_VERSION=4.1
ENV AWS_SDK_VERSION=1.12.261
ENV AWS_SDK_V2_VERSION=2.29.1
ENV MAVEN_URL=https://repo1.maven.org/maven2

RUN curl -s ${MAVEN_URL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    && curl -s ${MAVEN_URL}/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.0.jar

RUN curl -s ${MAVEN_URL}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_RUNTIME_VERSION}_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_RUNTIME_VERSION}_2.13-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime${SPARK_RUNTIME_VERSION}_2.13-${ICEBERG_VERSION}.jar

RUN curl -s ${MAVEN_URL}/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar \
    -o /opt/spark/jars/bundle-${AWS_SDK_V2_VERSION}.jar \
    && curl -sSLf ${MAVEN_URL}/software/amazon/awssdk/url-connection-client/${AWS_SDK_V2_VERSION}/url-connection-client-${AWS_SDK_V2_VERSION}.jar \
    -o /opt/spark/jars/url-connection-client-${AWS_SDK_V2_VERSION}.jar

USER spark

EXPOSE 15002

ENV SPARK_NO_DAEMONIZE=1
