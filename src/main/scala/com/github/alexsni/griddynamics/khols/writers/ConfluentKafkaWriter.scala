package com.github.alexsni.griddynamics.khols.writers

import com.github.alexsni.griddynamics.khols.outputs.ConfluentKafkaOutput
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent

final case class ConfluentKafkaWriter(output: ConfluentKafkaOutput) extends Writer[ConfluentKafkaOutput] {
  val targetSchemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL           -> output.targetSchemaRegistryUrl,
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC         -> output.targetTopic,
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY  -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID               -> "latest",
    SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> output.schemaNameForRecord,
    SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> output.namespaceNameForRecord
  )
  override def write(dataFrame: DataFrame)(implicit session: SparkSession): Unit = {
    writeAvro(dataFrame, targetSchemaRegistryConfig)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", output.bootstrapServers)
      .option("topic", output.targetTopic)
      .option("checkpointLocation", "checkpoint")
      .trigger(Trigger.Continuous(100))
      .start()
      .awaitTermination()
  }

  def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    dataFrame.select(to_confluent_avro(allColumns, schemaRegistryConfig) as 'value)
  }
}
