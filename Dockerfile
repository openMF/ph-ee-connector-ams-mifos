FROM openjdk:13
EXPOSE 5000

COPY build/libs/*.jar .
COPY build/resources/main/keystore.jks .
CMD java -jar *.jar

