# kantor-kafka-streams

Develop a Kafka Streams application that simulates a Currency Exchange.

1. Used the high-level Streams DSL
1. Use `StreamsBuilder.stream` to create a `KStream` for amounts to convert
1. Use `StreamsBuilder.table` to create a `KTable` for rates
1. Use `KStream.join` to join amounts with rates
