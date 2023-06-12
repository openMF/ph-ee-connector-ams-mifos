FROM azul/zulu-openjdk-alpine:17
EXPOSE 5000

COPY build/libs/*.jar .
COPY build/resources/main/keystore.jks .
CMD java -jar *.jar
