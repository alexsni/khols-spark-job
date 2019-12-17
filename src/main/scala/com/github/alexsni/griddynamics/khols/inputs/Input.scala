package com.github.alexsni.griddynamics.khols.inputs

sealed trait Input {
  def name: String
}

final case class ConfluentKafkaInput(name: String,
                                     brokers: String,
                                     sourceTopic: String,
                                     sourceSchemaRegistryUrl: String) extends Input




