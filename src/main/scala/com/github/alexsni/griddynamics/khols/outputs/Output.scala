package com.github.alexsni.griddynamics.khols.outputs

sealed trait Output {}

final case class ConfluentKafkaOutput(bootstrapServers: String,
                                      targetTopic: String,
                                      targetSchemaRegistryUrl: String,
                                      schemaNameForRecord: String,
                                      namespaceNameForRecord: String) extends Output
