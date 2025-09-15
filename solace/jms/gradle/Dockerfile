FROM eclipse-temurin:17-jdk

# Install Gradle
RUN apt-get update && apt-get install -y curl unzip
RUN curl -s https://services.gradle.org/distributions/gradle-8.5-bin.zip -o gradle.zip \
    && unzip gradle.zip -d /opt \
    && ln -s /opt/gradle-8.5/bin/gradle /usr/bin/gradle \
    && rm gradle.zip

# Set up app
WORKDIR /app
COPY . .

# Build the app
RUN gradle build --no-daemon

# Run the publisher
CMD ["java", "-cp", "build/libs/solace-jms-docker.jar", "SolaceJmsPublisher"]
