package example

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
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


  val awsAccessKeyId = "AKIAIGY5ZRLFHS3DCGBQ"
  val awsSecretKey = "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"
  val kinesisStreamName = "iot-stream"
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
    val kinesisStream =  (0 until 1).map { i =>
      KinesisUtils.createStream(ssc,
        kinesisAppName, kinesisStreamName,
        kinesisEndpointUrl, RegionUtils.getRegionMetadata.getRegionByEndpoint(kinesisEndpointUrl).getName(),
        InitialPositionInStream.LATEST, Seconds(kinesisCheckpointIntervalSeconds),
        StorageLevel.MEMORY_AND_DISK_SER_2, awsAccessKeyId, awsSecretKey)
    }

    val unionStreams =ssc.union(kinesisStream)
    //Schema of the incoming data on Kinesis Stream
    val schemaString = "deviceParameter,deviceValue,deviceId,dataTime"

    //Parse the data
    val tableSchema = StructType( schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))


    val sqlContext = new SQLContext(SparkContext.getOrCreate())
    //import sqlContext.implicits._


    //Processing each RDD and storing it in temporary table
    unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time) => {
      val rowRDD = rdd.map(jstr => new String(jstr))
      val df = sqlContext.read.json(rowRDD)
      df.createOrReplaceTempView("realTimeTable")
      })

    println("Creating function called to create new StreamingContext")
    ssc
  }
}
