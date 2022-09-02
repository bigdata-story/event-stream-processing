package ir.sharif.math.story

import com.datastax.spark.connector._
import ir.sharif.math.story.Utils.getKafkaStream
import org.apache.spark.streaming.{Minutes, Seconds}

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object ActiveUsersInCourseExporter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "active_users_in_course"

    val groupId = "active-users-in-course-writer"
    val topics = Array("events_by_user_id_course_id")

    val (stream, streamingContext) = getKafkaStream(groupId, topics, "latest")

    stream
      .map(x => (x.partition(), ujson.read(x.value()).obj))
      .window(Minutes(1), Seconds(10))
      .map(x => (x._2("course_id").str, x._2("user_id").str.toInt, x._2("session_id").str,
        x._2("event_type").str, parseDate(x._2("event_time").str), x._1)
      ).foreachRDD(rdd => rdd
      .groupBy(x => (x._1, x._6))
      .map(x => (x._1._1, x._1._2, x._2.map(y => y._2).toSet.size))
      .saveToCassandra(keyspace, table, SomeColumns("course_id", "partition", "user_count"))
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }

}
