package ir.sharif.math.story

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.util.Date

object Utils {
  val cassandraHost = scala.util.Properties.envOrElse("CASSANDRA_HOST", "cassandra-cluster")
  val sparkMaxCores = scala.util.Properties.envOrElse("SPARK_MAX_CORES", "1")
  val kafkaBootstrapServers = scala.util.Properties.envOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")


  def parseDate(dateString: String): Date = {
    Date.from(LocalDateTime.parse(dateString, ISO_DATE_TIME).atZone(ZoneId.systemDefault).toInstant)
  }

  def getKafkaStream(groupId: String, topics: Array[String], offset: String): (InputDStream[ConsumerRecord[String, String]], StreamingContext) = {
    val spark = SparkSession.builder
      .config("spark.cassandra.connection.host", cassandraHost)
      .config("spark.cores.max", sparkMaxCores)
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (true: java.lang.Boolean),
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    (stream, streamingContext)
  }
}
