package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingFile {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()


    val rawDF = spark.readStream
      .format("json")
      .option("path", "/Users/p0s00o1/Documents/projects/Spark-Streaming-In-Scala/StreamingWC/SampleData")
      .option("maxFilesPerTrigger", 1)
      .load()

    rawDF.printSchema()
    //rawDF.explain(extended = true)

    val explodeDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State",
      "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    explodeDF.printSchema()


    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "/Users/p0s00o1/Documents/projects/Spark-Streaming-In-Scala/StreamingWC/OutputData")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
  }


}









