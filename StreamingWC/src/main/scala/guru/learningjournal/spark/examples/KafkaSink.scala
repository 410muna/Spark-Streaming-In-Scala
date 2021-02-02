package guru.learningjournal.spark.examples

import guru.learningjournal.spark.examples.StreamingKafka.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger;
object KafkaSink extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
      val spark = SparkSession.builder()
        .appName("Streaming word count")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true") // To stop the process gracefully
        .config("spark.sql.shuffle.partitions", 3)
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


    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart-events")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.printSchema()
    val invoiceDF = kafkaDF.select(from_json(col("value").cast("string"),schema).alias("value"))

    val explodeDF = invoiceDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    val kafkaSinkDF = explodeDF.selectExpr("InvoiceNumber",
      "CreatedTime",
      "LineItem.ItemCode",
      "LineItem.ItemDescription",
      "LineItem.ItemPrice",
      "LineItem.ItemQty",
      "LineItem.TotalValue")
    val kafkaKeyVal = kafkaSinkDF.selectExpr("InvoiceNumber as key",
      """to_json(named_struct('InvoiceNumber','ItemCode',
        |'ItemDescription',
        |'ItemPrice',
        |'ItemQty',
        |'TotalValue')
        |) as value""".stripMargin)

   // kafkaKeyVal.show(10,false)

    val queryWriter = kafkaKeyVal.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "quickstart-evennts-kafkasink")
        .option("checkpointLocation", "chk-point-dir")
        .trigger(Trigger.ProcessingTime("1 minute"))
        .start()
    queryWriter.awaitTermination()

    //kafkaSinkDF.show(10,false)



    //invoiceDF.printSchema()

    //invoiceDF.show(10,false)



  }
}
