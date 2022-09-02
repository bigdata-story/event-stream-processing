package ir.sharif.math.story

import com.datastax.spark.connector._
import ir.sharif.math.story.Utils.getKafkaStream

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object CassandraWriter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "events"

    val groupId = "cassandra-writer"
    val topics = Array("events_topic")

    val (stream, streamingContext) = getKafkaStream(groupId, topics, "earliest")

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
