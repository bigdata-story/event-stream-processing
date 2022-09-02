package ir.sharif.math.story

import com.datastax.spark.connector._
import ir.sharif.math.story.Utils.getKafkaStream
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object ActiveUsersExporter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "active_users"

    val groupId = "active-users-writer"
    val topics = Array("events_by_user_id")

    val (stream, streamingContext) = getKafkaStream(groupId, topics, "latest")

    stream
      .map(x => (ujson.read(x.value()).obj, x.partition()))
      .window(Minutes(1), Seconds(10))
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
