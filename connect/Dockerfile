FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Install Confluent JDBC Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

# Copy Oracle JDBC Driver
# COPY ojdbc8.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/ojdbc8.jar

