package guru.learningjournal.spark.examples

import guru.learningjournal.spark.examples.StreamingWC.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger

object StreamingKafka extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming word count")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown","true") // To stop the process gracefully
      .config("spark.sql.shuffle.partitions",3)
      //.enableHiveSupport()
      .getOrCreate()


    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-events")
      .option("startingOffsets", "earliest")
      .load()

    //kafkaDF.printSchema()

    val valueDF = kafkaDF.select(from_json(col("value").cast("string"), schema).alias("value"))

    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")


    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")


    flattenedDF.printSchema()

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to Kafka")
    invoiceWriterQuery.awaitTermination()


  }


}
