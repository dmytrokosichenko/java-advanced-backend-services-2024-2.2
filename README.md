# java-advanced-backend-services-2024-2.2
Avro part (60 points)
In this task, you need to create a Kafka producer and consumer using Avro to serialize messages.
Create a simple Kafka producer that sends a simple message to a topic serializing it using Avro.
Create a simple Kafka consumer which listens to a topic for a message, deserializes the message, and prints it.
Start Kafka, create a topic, and run consumer and producer.
Try to use different Avro schemas for serialization and deserialization. Observe that
schema version/id has changed e.g. in schema registry and kafka message payload(bytes 1-4).

