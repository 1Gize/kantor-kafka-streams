import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties

object Kantor extends App {

  val streamBuilder = new StreamsBuilder()

  val amounts = streamBuilder.stream[String, String]("kantor-amounts")
  val rates = streamBuilder.table[String, String]("kantor-rates")
  amounts.print(Printed.toSysOut[String, String].withLabel("[Amounts]"))
  amounts
    .join(rates, (amt: String, rate: String) => s"Amount is: ${(amt.toDouble * rate.toDouble).toString}")
    .to("out")
  val topology = streamBuilder.build()

  val props = new Properties()
  val appId = this.getClass.getSimpleName.replace("$", "")
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  val streamsConfig = new StreamsConfig(props)

  val kafkaStream = new KafkaStreams(topology, streamsConfig)
  kafkaStream.start()
}
