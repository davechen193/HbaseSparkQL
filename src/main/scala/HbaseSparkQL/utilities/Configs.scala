package HbaseSparkQL.utilities

// general imports
import scala.io.Source
import java.io._
import com.typesafe.config.ConfigFactory
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._

object Configs{
    val conf = ConfigFactory.load("config");

    def getConf() = {
        conf
    }
    
    def get(path: String) : String = {
        conf.getString(path)
    }

    def getList(path: String) : List[String] = {
        conf.getStringList(path).asScala.toList
    }

    // Set Date args.
    case class DateConfig(
        year: Int = 0,
        month: Int = 0,
        day: Int = 0
    )

    // Set Kafka args.
    case class KafkaConfig(
        sparkMaster: String = get("sparkMaster"),
        outputHdfs: String = get("hadoop") + get("KafkaDefaultConfig.outputHdfs"),
        kafkaBroker: String = get("KafkaDefaultConfig.kafkaIP") + ":" + get("KafkaDefaultConfig.kafkaPort"),
        kafkaTopic: String = get("KafkaDefaultConfig.kafkaTopic"),
        batchInterval: Int = getConf.getInt("KafkaDefaultConfig.batchInterval")
    )
    // HDFS Data Ingest args.
    case class IngestConfig(
        backupDir: String,
        trainDataDir: String
    )
}
