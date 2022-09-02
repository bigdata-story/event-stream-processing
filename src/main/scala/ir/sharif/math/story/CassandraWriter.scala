package ir.sharif.math.story

import com.datastax.spark.connector._
import ir.sharif.math.story.Utils.getKafkaStream

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object CassandraWriter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val groupId = "cassandra-writer"
    val topics = Array("events_by_user_id_course_id")
    val (stream, streamingContext) = getKafkaStream(groupId, topics, "earliest")
    stream
      .map(x => ujson.read(x.value()).obj)
      .map(x => (java.util.UUID.randomUUID.toString, x("course_id").str, x("user_id").str.toInt, x("session_id").str,
        x("event_type").str, parseDate(x("event_time").str)))
      .foreachRDD(rdd => {
        rdd.saveToCassandra(keyspace, "events_by_user_course",
          SomeColumns("uuid", "course_id", "user_id", "session_id", "event_type", "event_time")
        )
        rdd.saveToCassandra(keyspace, "events_by_user",
          SomeColumns("uuid", "course_id", "user_id", "session_id", "event_type", "event_time"))
        rdd.saveToCassandra(keyspace, "events_by_course",
          SomeColumns("uuid", "course_id", "user_id", "session_id", "event_type", "event_time"))
      })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }

}
