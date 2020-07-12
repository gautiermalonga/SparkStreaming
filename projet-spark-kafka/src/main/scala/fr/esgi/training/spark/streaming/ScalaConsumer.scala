import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties

import scala.collection.JavaConverters._




object ScalaConsumer extends App {

  val props:Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG,"test")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer])

  // props.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range")

  val consumer:KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
  val topics = List("json").asJava
  try {
    consumer.subscribe(List("json").asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())

      }

    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }

}