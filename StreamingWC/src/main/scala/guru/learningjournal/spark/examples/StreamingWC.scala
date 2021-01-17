package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingWC extends Serializable {

  /**
   * Here logger gets initialized only when it is accessed and transient makes it not to get serialized , and recomputed only once
   */
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Streaming word count")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown","true") // To stop the process gracefully
      .config("spark.sql.shuffle.partitions",3)
      //.enableHiveSupport()
      .getOrCreate()

    val linesDf = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999").load()

    //linesDf.printSchema()

    val wordsDf = linesDf.select(expr("explode(split(value,' ')) as words"))
    val countsDf = wordsDf.groupBy("words").count()

    val wordCountQuery = countsDf.writeStream
      .format("console")
      .option("checkpointLocation","chk-point-dir")
      .outputMode("complete")
      .start()

    logger.info("listing to TCP port 9999")
    wordCountQuery.awaitTermination()


  }
}