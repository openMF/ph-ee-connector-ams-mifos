FROM eclipse-temurin:17-jre
EXPOSE 5000

COPY build/libs/*.jar app.jar
COPY build/resources/main/keystore.jks .
CMD ["java", "-jar", "app.jar"]

