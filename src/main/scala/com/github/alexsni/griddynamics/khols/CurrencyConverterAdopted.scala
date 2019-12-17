package com.github.alexsni.griddynamics.khols

import com.github.alexsni.griddynamics.khols.inputs.ConfluentKafkaInput
import com.github.alexsni.griddynamics.khols.outputs.ConfluentKafkaOutput
import com.github.alexsni.griddynamics.khols.readers.{ConfluentKafkaReader, Reader}
import com.github.alexsni.griddynamics.khols.writers.{ConfluentKafkaWriter, Writer}
import com.softwaremill.sttp.{HttpURLConnectionBackend, sttp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf, lit}
import com.softwaremill.sttp._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object CurrencyConverterAdopted extends SparkJob[ConfluentKafkaInput, ConfluentKafkaOutput] {
  override def jobName: String = "KholsCurrencyConverter"

  override def local: Boolean =
    if (checkInputParams(inputParams, jobParams) && jobParams("locality") == "true") true else false

  override def inputReader: Reader[ConfluentKafkaInput] = {
    if (checkInputParams(inputParams, jobParams)) {
      ConfluentKafkaReader(ConfluentKafkaInput(
        "source_topic",
        jobParams("brokers"),
        jobParams("sourceTopic"),
        jobParams("schemaRegistryUrl")
      ))
    } else {
      throw new RuntimeException("Invalid arguments")
    }
  }

  override def outputWriter: Writer[ConfluentKafkaOutput] = {
    if (checkInputParams(inputParams, jobParams)) {
      ConfluentKafkaWriter(ConfluentKafkaOutput(
        jobParams("brokers"),
        jobParams("targetTopic"),
        jobParams("schemaRegistryUrl"),
        "KholsTargetMessage", // ClassName used in schema registry
        "com.github.alexsni.griddynamics.khols" // namespace used in schema registry
      ))
    } else {
      throw new RuntimeException("Invalid arguments")
    }
  }

  override def inputParams: List[String] = List(
    "brokers",
    "sourceTopic",
    "schemaRegistryUrl",
    "targetTopic",
    "currencyConverterURL",
    "toCurrency",
    "locality"
  )

  override def transform(sourceDF: DataFrame): DataFrame = {
    val getExchangeRateUdf = udf[Option[Double], Double]((d) => {
      val request = sttp
        .get(uri"${jobParams("currencyConverterURL")}?amount=${d}&fromCurrency=USD&toCurrency=${jobParams("toCurrency")}")

      implicit val backend = HttpURLConnectionBackend()
      val response = request.send()
      implicit val formats = DefaultFormats

      response.body match {
        case Right(b: String) => Some(read[Double](b))
        case Left(_) => None
      }
    })

    sourceDF
      .withColumn("to_currency", getExchangeRateUdf(col("price")))
      .select(col("itemId"), col("to_currency") as 'price )
      .withColumn("currency", lit(jobParams("toCurrency")))
  }
}
