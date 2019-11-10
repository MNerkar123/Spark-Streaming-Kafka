import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.DataTypes

case class apiInfo(requestTime: String,
                   requestType: String,
                   incomingUrl: String,
                   microservice: String)

object Invoker {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.streaming.StreamingContext

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val appLogger = LoggerFactory.getLogger("Spark Application Logger")

    val sparkSessionObj = SparkSession
      .builder()
      .master("local[*]")
      .appName("Demo Application")
      .getOrCreate()

    appLogger.info("setting up the interval of 20 seconds")

    val streamingContext =
      new StreamingContext(sparkSessionObj.sparkContext, Seconds(60))

    appLogger.info("setting up paramter for kafka consumer")

    val kafkaParameters: scala.collection.immutable.Map[String, Object] =
      Map(
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "demogroupid",
        "enable.auto.commit" -> java.lang.Boolean.TRUE
      )

    appLogger.info("initializing the topics for consumption")
    val kafkaTopicName = Array("apidata2")

    appLogger.info("creating stream for consumption ")
    val feedStreams = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](kafkaTopicName,
                                                   kafkaParameters))

    feedStreams
      .map(consumerRecordFunction => consumerRecordFunction.value())
      .foreachRDD(consumerRDDFunction => {

        val sqlContextObj =
          SQLContext.getOrCreate(consumerRDDFunction.sparkContext)
        import sqlContextObj.implicits._

        val apiDataDf = consumerRDDFunction
          .map(
            recordConvert =>
              apiInfo(recordConvert.split(",")(0),
                      recordConvert.split(",")(1),
                      recordConvert.split(",")(2),
                      recordConvert.split(",")(3)))
          .toDF()

        val transformedApiDf = apiDataDf
          .select(apiDataDf("requestTime").cast(DataTypes.DateType),
                  apiDataDf("requestType"),
                  apiDataDf("incomingUrl"),
                  apiDataDf("microservice"))

        transformedApiDf.show(20)
        transformedApiDf.printSchema()

        appLogger.info(
          "counting the counts of api calls within window of 1 minutes")
        val minuteAggCalls = transformedApiDf
          .groupBy(window(transformedApiDf.col("requestTime"), "1 minute"),
                   transformedApiDf.col("requestType"))
          .count()

        minuteAggCalls.show(10)
        minuteAggCalls.write.mode(SaveMode.Append).json("E:\\by1minutes")

        appLogger.info(
          "counting the counts on api calls in window frame of 5 minutes")

        val minutesAggCalls = transformedApiDf
          .groupBy(window(transformedApiDf("requestTime"), "5 minute"),
                   transformedApiDf("requestType"))
          .count()

        minutesAggCalls.show(10)
        minutesAggCalls.write.mode(SaveMode.Append).json("E:\\by5minutes")

      })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
