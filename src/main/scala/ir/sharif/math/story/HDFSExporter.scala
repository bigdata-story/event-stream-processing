package ir.sharif.math.story

import ir.sharif.math.story.Utils.getKafkaStream

object HDFSExporter {
  def main(args: Array[String]): Unit = {
    val groupId = "exporter"
    val topics = Array("events_by_user_id_course_id")
    val (stream, streamingContext) = getKafkaStream(groupId, topics, "earliest")
    stream.map(x => (x.key(), x.value())).foreachRDD(rdd => rdd.saveAsTextFile(s"hdfs://name-node:9000/backup/${System.currentTimeMillis() + ""}.txt"))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
