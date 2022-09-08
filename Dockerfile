FROM openjdk:13
EXPOSE 5000

COPY target/*.jar .
COPY keystore.jks .
CMD java -jar *.jar

