import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp

import java.util.Properties
import scala.sys.Prop
import org.apache.kafka.streams.kstream.{Consumed, Printed}
import org.apache.kafka.streams.scala.ImplicitConversions.wrapKStream

object Kantor extends App{
  val streamBuilder = new StreamsBuilder()
  val amounts = streamBuilder.stream[String,String]("kantor-amounts")
  val rates = streamBuilder.table[String,String]("kantor-rates")
  amounts.print(Printed.toSysOut[String,String].withLabel("[Amounts]"))
  //rates.toStream().print(Printed.toSysOut[String,String].withLabel("[Rates]"))
  amounts.join(rates,(a: String,r: String)=>s"Amount is: ${(a.toDouble*r.toDouble).toString}").to("out")
  val topology = streamBuilder.build()
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ks-kantor-exchange")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,":9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  val streamsConfig = new StreamsConfig(props)
  val kafkaStream = new KafkaStreams(topology,streamsConfig)
  kafkaStream.start()
}
