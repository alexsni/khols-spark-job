package com.github.alexsni.griddynamics.khols

import com.github.alexsni.griddynamics.khols.inputs.Input
import com.github.alexsni.griddynamics.khols.outputs.Output
import com.github.alexsni.griddynamics.khols.readers.Reader
import com.github.alexsni.griddynamics.khols.transformers.Transformer
import com.github.alexsni.griddynamics.khols.writers.Writer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

abstract class SparkJob[I <: Input, O <: Output] extends Logging with Transformer {
  def jobName: String
  def inputReader: Reader[I]
  def outputWriter: Writer[O]
  def inputParams: List[String]
  def local: Boolean

  private var inputArgs: Array[String] = Array()

  lazy val jobParams: Map[String, String] = parseParams(inputArgs)

  private def parseParams(a: Array[String]): Map[String, String] = {
    a.foreach(println)
    a.map(s => {
      val split: Array[String] = s.split("=")
      split(0).substring(2) -> split(1)
    }).foldLeft(Map[String, String]())((m, t) => m + (t._1 -> t._2))
  }

  private def createSparkSession(jobName: String, local: Boolean): SparkSession = {
    val builder = SparkSession.builder().appName(jobName)
    if (local)
      builder.master("local[*]")

    builder.getOrCreate()
  }

  def checkInputParams(requiredArgs: List[String], params: Map[String, String]): Boolean = {
    requiredArgs.foldLeft(true)((a,p) => {if (params.contains(p) && a) true else false})
  }

  def main(args: Array[String]): Unit = {
    inputArgs = args
    logInfo(s"Starting a job $jobName...")
    if (!checkInputParams(inputParams, jobParams))
      throw new RuntimeException("Invalid arguments")
    println(jobParams)

    implicit val session: SparkSession = createSparkSession(jobName, local)
    outputWriter.write(transform(inputReader read))
  }
}
