package com.github.alexsni.griddynamics.khols.writers

import com.github.alexsni.griddynamics.khols.outputs.Output
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer[+O <: Output] {
  def output: O
  def write(dataFrame: DataFrame)(implicit session: SparkSession): Unit
}
