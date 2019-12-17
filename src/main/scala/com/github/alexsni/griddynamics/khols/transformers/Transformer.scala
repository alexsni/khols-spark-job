package com.github.alexsni.griddynamics.khols.transformers

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(sourceDF: DataFrame): DataFrame
}
