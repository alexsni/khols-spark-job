package com.github.alexsni.griddynamics.khols.readers

import com.github.alexsni.griddynamics.khols.inputs.Input
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[+I <: Input] {
  def input: I
  def read()(implicit session: SparkSession): DataFrame
}
