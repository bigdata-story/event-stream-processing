package ir.sharif.math.story

import ir.sharif.math.story.Utils.getKafkaStream
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exporter {
  def main(args: Array[String]): Unit = {
    val groupId = "exporter"
    val topics = Array("events_topic")

    val (stream, streamingContext) = getKafkaStream(groupId, topics, "earliest")

    stream.map(x => (x.key(), x.value())).foreachRDD(rdd => rdd.saveAsTextFile(s"hdfs://name-node:9000/backup/${System.currentTimeMillis() + ""}.txt"))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
