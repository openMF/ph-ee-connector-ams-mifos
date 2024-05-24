FROM openjdk:13

RUN addgroup --system <group>
RUN adduser --system <user> --ingroup <group>
USER <user>:<group>

EXPOSE 5000

COPY build/libs/*.jar .
COPY build/resources/main/keystore.jks .
CMD java -jar *.jar

