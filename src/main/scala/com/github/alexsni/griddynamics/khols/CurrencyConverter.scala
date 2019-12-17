package com.github.alexsni.griddynamics.khols

import com.softwaremill.sttp.{HttpURLConnectionBackend, sttp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import org.apache.spark.sql.functions.{col, struct, udf}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import com.softwaremill.sttp._
import org.apache.spark.sql.streaming.Trigger


object CurrencyConverter extends App {
  println("Hello World!")

  val session = SparkSession.builder().appName("CurrencyConverter").master("local[*]").getOrCreate();

  val SOURCE_TOPIC = "khols_source_topic"
  val TARGET_TOPIC = "khols_target_topic"
  val CURRENCY_CONVERTER_URL = "http://localhost:8080/converter/"
  val TO_CURRENCY = "PLN"

  val sourceDF = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .load()

  val sourceSchemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL         -> "http://127.0.0.1:8081/",
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC       -> SOURCE_TOPIC,
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY  -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID             -> "latest"
  )

  val targetSchemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL         -> "http://127.0.0.1:8081/",
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC       -> TARGET_TOPIC,
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY  -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID             -> "latest"
  )

  import za.co.absa.abris.avro.functions.from_confluent_avro

  val converted = sourceDF.select(from_confluent_avro(col("value"), sourceSchemaRegistryConfig) as 'value).select("value.*")
  println(converted.schema)

  case class Exchange(price: Double)

  val getExchangeRateUdf = udf[Option[Double], Double]((d) => {
    val request = sttp
      .get(uri"${CURRENCY_CONVERTER_URL}?amount=${d}&fromCurrency=USD&toCurrency=${TO_CURRENCY}")

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()
    implicit val formats = DefaultFormats

    response.body match {
      case Right(b: String) => Some(read[Double](b))
      case Left(_) => None
    }
  })

  val transformed = converted.withColumn("to_currency", getExchangeRateUdf(col("price")))

//  transformed.show()

  import za.co.absa.abris.avro.functions.to_confluent_avro
  def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    dataFrame.select(to_confluent_avro(allColumns, schemaRegistryConfig) as 'value)
  }

  writeAvro(transformed, targetSchemaRegistryConfig)
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", TARGET_TOPIC)
    .option("checkpointLocation", "checkpoint")
    .trigger(Trigger.Continuous(1))
    .start()
    .awaitTermination()
}
