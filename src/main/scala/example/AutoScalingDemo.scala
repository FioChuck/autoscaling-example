import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.ml.linalg.SQLDataTypes

object AutoScalingDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Autoscaling Demo")
      .config("spark.master", "local[16]") // local dev
      .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      )
      .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/Users/chasf/Desktop/cf-data-analytics-f8ccb6c85b39.json"
      )
      .getOrCreate()

    spark.conf.set("viewsEnabled", "true")
    spark.conf.set(
      "materializationDataset",
      "spark_autoscaling"
    )

    import spark.implicits._

    // val sql1 =
    //   """ SELECT * FROM `bigquery-public-data.wikipedia.wikidata` LIMIT 1000 """

    // val wiki = spark.read
    //   .format("bigquery")
    //   .load(sql1)
    //   .select($"id", $"en_label", $"en_description")
    //   .withColumn("wiki_id", $"id")
    //   .withColumn("en_label_lower", lower($"en_label"))
    //   .drop("id")
    // // .repartition(1000)

    // val sql2 =
    //   """ SELECT * FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 1000 """

    // val stack = spark.read
    //   .format("bigquery")
    //   .load(sql2)
    //   .filter($"title".isNotNull)
    //   .select($"id", $"title", $"body", $"view_count")
    //   .withColumn("stack_id", $"id")
    //   .withColumn("title_lower", lower(col("title")))
    //   .withColumn("keyword", explode(split($"title_lower", "[ ]")))
    //   .drop("id")

    val wiki =
      spark.read
        .format("bigquery")
        .option("table", "bigquery-public-data.wikipedia.wikidata")
        .load()
        .select($"id", $"en_label", $"en_description")
        .withColumn("wiki_id", $"id")
        .withColumn("en_label_lower", lower($"en_label"))
        .drop("id")
        .repartition(1000)

    val stack =
      spark.read
        .format("bigquery")
        .option(
          "table",
          "bigquery-public-data.stackoverflow.posts_questions"
        )
        .option(
          "filter",
          "view_count > 10000"
        )
        .load()
        .filter($"title".isNotNull)
        .select($"id", $"title", $"body", $"view_count")
        .withColumn("stack_id", $"id")
        .withColumn("title_lower", lower(col("title")))
        .withColumn("keyword", explode(split($"title_lower", "[ ]")))
        .drop("id")
        .repartition(1000)

    val out =
      wiki
        .join(
          stack,
          wiki("en_label_lower") === stack("keyword"),
          "inner"
        )
        .hint("SHUFFLE_HASH")
        .repartition(1000)

    out.write
      .format("bigquery")
      // .option(
      //   "writeMethod",
      //   "direct"
      // )
      .option("temporaryGcsBucket", "cf-spark-temp")
      .mode("overwrite")
      .save(
        "cf-data-analytics.spark_autoscaling.output"
      )

    // stack.write
    //   .format("bigquery")
    //   // .option(
    //   //   "writeMethod",
    //   //   "direct"
    //   // )
    //   .option("temporaryGcsBucket", "cf-spark-temp")
    //   .mode("overwrite")
    //   .save(
    //     "cf-data-analytics.spark_autoscaling.stack_skew"
    //   )
  }
}
