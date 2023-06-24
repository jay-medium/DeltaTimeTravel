package com.delta.demo

import io.delta.tables._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import scala.reflect.io.Directory


object TimeTravel {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    // spark session
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("TimeTravelUsingVersion")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // input data
    import spark.implicits._
    val data: Dataset[Person] = Seq(
      Person("person1", 25),
      Person("person2", 26)
    ).toDS()


    // DeltaLake table creation
    val deltaTablePath = "/tmp/delta/tables/TimeTravelVersion"
    deleteDirectory(deltaTablePath)

    // create initial transaction
    data.coalesce(1).write.format("delta").mode(SaveMode.Append).save(deltaTablePath)

    // modify the first transaction to an older date.
    val f = new File("/tmp/delta/tables/TimeTravelVersion/_delta_log/00000000000000000000.json")
    val currentDate = LocalDate.now()
    val daysOlder: Int = 31
    val targetDate = currentDate.minus(daysOlder, ChronoUnit.DAYS)
    f.setLastModified(targetDate.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli)

    println(s"Today's Date => $currentDate \n")
    println("Log files generated after the first write into the delta table")
    listDeltaLogFiles(spark, deltaTablePath)

    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)

    println()
    deltaTable.history().show(false)

    /*
    * create 10 transactions on the same table
    * By default on every 10th commit Delta does checkpointing which
    * create checkpoint.parquet comprising the metadata as on the 10th commit & also cleans up the
    * log files older than 'delta.logRetentionDuration' (default 30 days)"
    * */
    0 to 10 foreach { _ =>
      data.coalesce(1).write.format("delta").mode(SaveMode.Append).save(deltaTablePath)
    }

    println("\nLog files after the above 10 writes into the delta table")
    listDeltaLogFiles(spark, deltaTablePath)

    // print history of operations on delta table
    deltaTable.history().show(false)


    /*
    * TimeTravel to an older version
    * version not found exception
    * Exception in thread "main" org.apache.spark.sql.delta.VersionNotFoundException: Cannot time travel Delta table to version 2. Available versions: [10, 11].
    * This exception has occurred as delta could not find the 0th commit log file.
    */
    spark.read
      .format("delta")
      .option("versionAsOf", "2")
      .load(deltaTablePath)
      .show(false)

  }

  private def listDeltaLogFiles(spark: SparkSession, deltaTablePath: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val logFiles = fs.listFiles(new Path(s"$deltaTablePath/_delta_log/"), true)
    while (logFiles.hasNext) {
      val logFile = logFiles.next()
      val instant: Instant = Instant.ofEpochMilli(logFile.getModificationTime)
      val localDateTime: LocalDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
      val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dateString: String = localDateTime.format(dateFormat)
      println(s"$dateString ${logFile.getPath} ")
    }
  }

  private def deleteDirectory(deltaTablePath: String): Boolean = {
    val dir = new Directory(new File(deltaTablePath))
    dir.deleteRecursively()
  }
}
