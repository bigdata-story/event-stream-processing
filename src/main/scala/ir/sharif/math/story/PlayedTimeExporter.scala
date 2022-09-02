package ir.sharif.math.story

import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Minutes}

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object PlayedTimeExporter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "course_played_time"
    val spark = SparkSession.builder
      .config("spark.cassandra.connection.host", "cassandra-cluster")
      .config("spark.cores.max", "1")
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    val groupId = "course-played-time-writer"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean),
    )
    val topics = Array("video_events_by_user_id_course_id")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(x => (x.partition(), ujson.read(x.value()).obj))
      .window(Minutes(60), Minutes(1))
      .map(x => (x._2("course_id").str, x._2("event_type").str,
        parseDate(x._2("event_time").str).getTime, x._1)
      ).foreachRDD(rdd => {
      val play_list = List("play_video")
      val stop_list = List("pause_video", "stop_video")
      rdd.groupBy(x => (x._1, x._2, x._4))
        .map(x => {
          if (play_list.contains(x._1._2)) {
            (x._1._1, x._1._3, -1 * x._2.map(y => y._3).sum, x._2.size)
          }
          else {
            (x._1._1, x._1._3, x._2.map(y => y._3).sum, -1 * x._2.size)
          }
        })
        .groupBy(x => (x._1, x._2))
        .map(x => (x._1._1, x._1._2, x._2.map(y => y._3).sum, x._2.map(y => y._4).sum))
        .map(x => (x._1, x._2, ((x._3 + x._4 * System.currentTimeMillis()) / 1000).toInt))
        .saveToCassandra(keyspace, table, SomeColumns("course_id", "partition", "play_time"))
    }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }
}
