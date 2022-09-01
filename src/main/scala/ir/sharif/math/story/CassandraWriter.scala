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

object CassandraWriter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "events"
    val spark = SparkSession.builder
      .config("spark.cassandra.connection.host", "cassandra-cluster")
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    val groupId = "cassandra-writer"
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
    stream
      .map(x => ujson.read(x.value()).obj)
      .map(x => (x("course_id").str, x("user_id").str.toInt, x("session_id").str,
        x("event_type").str, parseDate(x("event_time").str)))
      .foreachRDD(rdd => rdd.saveToCassandra(keyspace, table,
        SomeColumns("course_id", "user_id", "session_id", "event_type", "event_time")))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }

}
