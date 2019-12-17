package com.github.alexsni.griddynamics.khols.readers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.github.alexsni.griddynamics.khols.inputs.ConfluentKafkaInput
import za.co.absa.abris.avro.read.confluent.SchemaManager

final case class ConfluentKafkaReader(input: ConfluentKafkaInput) extends Reader[ConfluentKafkaInput] {
  val sourceSchemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL           -> input.sourceSchemaRegistryUrl,
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC         -> input.sourceTopic,
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY  -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID               -> "latest"
  )

  override def read()(implicit session: SparkSession): DataFrame = {
    import za.co.absa.abris.avro.functions.from_confluent_avro
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", input.brokers)
      .option("subscribe", input.sourceTopic)
      .load()
      .select(from_confluent_avro(col("value"), sourceSchemaRegistryConfig) as 'value).select("value.*")
  }


}
