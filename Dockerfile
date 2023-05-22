FROM openjdk:17-bullseye
EXPOSE 5000

RUN apt-get update && apt-get install -y vim less iputils-ping telnet && apt-get autoclean
WORKDIR /app

COPY keystore.jks /app/
COPY target/*.jar /app/
CMD java -jar *.jar 
