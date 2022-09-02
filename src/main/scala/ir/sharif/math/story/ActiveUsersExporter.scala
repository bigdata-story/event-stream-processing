package ir.sharif.math.story

import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object ActiveUsersExporter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "active_users"
    val spark = SparkSession.builder
      .config("spark.cassandra.connection.host", "cassandra-cluster")
      .config("spark.cores.max", "1")
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    val groupId = "active-users-writer"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean),
    )
    val topics = Array("events_by_user_id")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
      .map(x => (ujson.read(x.value()).obj, x.partition()))
      .map(x => (x._1("user_id").str.toInt, x._2))
      .foreachRDD(rdd =>
        rdd.groupBy(x => x._2)
          .map(x => (1, x._1, x._2.map(y => y._1).toSet.size))
          .saveToCassandra(keyspace, table, SomeColumns("tof", "partition", "user_count")))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }

}
