# Compile the Java MapReduce jobs with a JDK, then add the jar to the Hadoop image (which has only a JRE).
FROM eclipse-temurin:8-jdk AS build
COPY --from=apache/hadoop:3.4.1 /opt/hadoop/share/hadoop/common /h/common
COPY --from=apache/hadoop:3.4.1 /opt/hadoop/share/hadoop/mapreduce /h/mapreduce
COPY TaxiJobs.java /src/TaxiJobs.java
RUN mkdir /classes && \
    javac -nowarn -cp "/h/common/*:/h/common/lib/*:/h/mapreduce/*" -d /classes /src/TaxiJobs.java && \
    jar cf /taxi-mr.jar -C /classes .

FROM apache/hadoop:3.4.1
COPY --from=build /taxi-mr.jar /opt/jobs/taxi-mr.jar
