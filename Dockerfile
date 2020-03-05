FROM openjdk:13
EXPOSE 5000

COPY target/*.jar .
COPY target/classes/keystore.jks .
CMD java -jar *.jar

