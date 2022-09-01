package ir.sharif.math.story

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exporter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))
    val groupId = "exporter"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean),
    )
    val topics = Array("events_topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(x => (x.key(), x.value())).foreachRDD(rdd => rdd.saveAsTextFile(s"hdfs://name-node:9000/backup/${System.currentTimeMillis() + ""}.txt"))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
