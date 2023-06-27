import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object AutoScalingDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Autoscaling Demo")
      // .config("spark.master", "local[16]") // local dev
      // .config(
      //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
      //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      // )
      // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      // .config(
      //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
      //   "/Users/chasf/Desktop/cf-data-analytics-f8ccb6c85b39.json"
      // )
      .getOrCreate()

    import spark.implicits._

    val wiki =
      spark.read
        .format("bigquery")
        .option("table", "bigquery-public-data.wikipedia.wikidata")
        .load()
        .select($"id", $"en_label", $"en_description")
        .withColumn("wiki_id", $"id")
        .withColumn("en_label_lower", lower($"en_label"))
        .drop("id")
        .limit(10000000)

    val stack =
      spark.read
        .format("bigquery")
        .option(
          "table",
          "bigquery-public-data.stackoverflow.stackoverflow_posts"
        )
        .load()
        .filter($"title".isNotNull)
        .select($"id", $"title", $"body")
        .withColumn("stack_id", $"id")
        .withColumn("title_lower", lower(col("title")))
        .withColumn("keyword", explode(split($"title_lower", "[ ]")))
        .drop("id")
        .limit(10000000)

    val out =
      wiki
        .join(
          stack,
          wiki("en_label_lower") === stack("keyword"),
          "inner"
        )

    out.write
      .format("bigquery")
      .option(
        "writeMethod",
        "direct"
      )
      .mode("overwrite")
      .save(
        "cf-data-analytics.spark_autoscaling.output"
      )
  }
}
