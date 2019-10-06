package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { ’class’ : ’SimpleStrategy’, ’replication_factor’ : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it

    val conf = new SparkConf().setAppName("Spark Streaming Example").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(10))
    sparkStreamingContext.checkpoint("/Users/kaima/School/Data Intensive/Lab_2/sparkstreaming")


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Array("avg")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sparkStreamingContext, kafkaConf, topics)

    val pairs = stream.map(record => (record._2.split(",")(0), record._2.split(",")(1).toDouble))

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      if (state.exists()) {
        val existingState = state.get()
        val existingAvg = existingState._1
        val existingCount = existingState._2
        val newAvg = (existingAvg * existingCount + value.getOrElse(existingAvg)) / (existingCount + 1)
        val newCount = existingCount + 1
        state.update((newAvg, newCount))
        Some(key, (newAvg, newCount))
      } else if (value.isDefined) {
        val initialValue = value.get.toDouble
        state.update((initialValue, 1))
        Some(key, (initialValue, 1))
      }
    }
  }
}

}
val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc))
)

// store the result in Cassandra
stateDstream.map(result => (result._1, result._2)).print()
stateDstream.map(result => (result.get._1, result.get._2)).saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

ssc.start()
ssc.awaitTermination()
}
}
