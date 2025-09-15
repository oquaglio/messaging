FROM eclipse-temurin:17-jdk

# Copy source
WORKDIR /app
#COPY . .
RUN git clone https://github.com/your-org/solace-jms-app.git .

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    mvn -B package

#CMD ["sh", "-c", "java -jar target/solace-jms-1.0.0-jar-with-dependencies.jar"]

CMD ["java", "-jar", "target/solace-jms-1.0.0-jar-with-dependencies.jar"]

