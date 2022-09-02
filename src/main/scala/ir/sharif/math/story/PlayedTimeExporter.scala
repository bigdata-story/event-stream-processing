package ir.sharif.math.story

import com.datastax.spark.connector._
import ir.sharif.math.story.Utils.{getKafkaStream, parseDate}
import org.apache.spark.streaming.Minutes


object PlayedTimeExporter {
  def main(args: Array[String]): Unit = {
    val keyspace = "story"
    val table = "course_played_time"

    val groupId = "course-played-time-writer"
    val topics = Array("video_events_by_user_id_course_id")

    val (stream, streamingContext) = getKafkaStream(groupId, topics, "latest")

    stream
      .map(x => (x.partition(), ujson.read(x.value()).obj))
      .window(Minutes(60), Minutes(1))
      .map(x => (x._2("course_id").str, x._2("event_type").str,
        parseDate(x._2("event_time").str).getTime / 1000, x._2("user_id").str.toInt, x._1)
      ).foreachRDD(rdd =>
      rdd.groupBy(x => (x._1, x._4, x._5))
      .map(x => (x._1, x._2.toList.sortBy(y => y._3)))
      .map(x =>{
        println(x._2)
        val max = x._2.map(y => y._3).max
        val min = x._2.map(y => y._3).min
        val iter = x._2.iterator
        var ans: Long = 0
        var ch: Boolean = false
        var open: Boolean = false
        while (iter.hasNext) {
          val y = iter.next()
          if (y._2.equals("play_video")) {
            if (!open) {
              ans -= y._3
              open = true
            }
          }
          else {
            if (open) {
              ans += y._3
              open = false
            }
            else {
              if (!ch) {
                val tmp = y._3 - min
                if (tmp > 0) {
                  ans += tmp
                }
                ch = true
              }
            }
          }
        }
        if (open) {
          ans += max
        }
        (x._1._1, x._1._3, ans)
      })
      .groupBy(x => (x._1, x._2))
      .map(x => (x._1._1, x._1._2, x._2.map(y => y._3).sum))
      .saveToCassandra(keyspace, table, SomeColumns("course_id", "partition", "play_time"))
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
