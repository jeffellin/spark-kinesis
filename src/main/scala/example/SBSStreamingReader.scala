package example

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import org.json4s.jackson.JsonMethods.parse;
import org.json4s.{DefaultFormats}
/**
  * Created by jellin on 8/18/17.
  */
object SBSStreamingReader {

  def main(args: Array[String]) {

    // Get or create a streaming context.
    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    // This starts the streaming context in the background.
    ssc.start()
    ssc.awaitTermination;

  }


  val awsAccessKeyId = ""
  val awsSecretKey = ""
  val kinesisStreamName = "sbs-data"
  val kinesisEndpointUrl = "kinesis.us-east-1.amazonaws.com"
  val kinesisAppName = "SBSStreamingReader"
  val kinesisCheckpointIntervalSeconds = 1
  val batchIntervalSeconds = 1


  case class Beer(deviceParameter:String, deviceValue:Int, deviceId:String,dateTime:String);


  def creatingFunc(): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("SBSStreamingReader")

    // Create a StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(batchIntervalSeconds))


    // Creata a Kinesis stream
    val kinesisStream = KinesisUtils.createStream(ssc,
      kinesisAppName, kinesisStreamName,
      kinesisEndpointUrl, RegionUtils.getRegionMetadata.getRegionByEndpoint(kinesisEndpointUrl).getName(),
      InitialPositionInStream.LATEST, Seconds(kinesisCheckpointIntervalSeconds),
      StorageLevel.MEMORY_AND_DISK_SER_2, awsAccessKeyId, awsSecretKey)

    // Convert the byte array to a string
    val iot = kinesisStream.map { byteArray => new String(byteArray)}


    // Create output csv file at every batch interval
    iot.foreachRDD { rdd =>

      val sqlContext = new SQLContext(SparkContext.getOrCreate())

      import sqlContext.implicits._

      val jobs = rdd.map(jstr => {

        implicit val formats = DefaultFormats

        val parsedJson = parse(jstr)
        val j = parsedJson.extract[Beer]
        j
      })

      //output the rdd to csv
      jobs.toDF()
        .write.mode(SaveMode.Append).csv("/tmp/data/csv")

    }

    println("Creating function called to create new StreamingContext")
    ssc
  }
}
