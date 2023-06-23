import scala.math.random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object BqDemo {
  def main(args: Array[String]): Unit = {

    // TODO how to pass arguments in debug? Not possible with Metals?
    val spark = SparkSession.builder
      .appName("Bq Demo")
      .config("spark.master", "local[*]") // local dev
      .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      )
      .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/Users/chasf/Desktop/cf-data-analytics-1ff73e9e3f7a.json"
      )
      .getOrCreate()

    val df = spark.sqlContext
      .range(0, 1370462701) // define the number of mock data rows

    val df2 = df
      .withColumn("even_distribution", rand(seed = 10))
      .withColumn("normal_distribution", randn(seed = 10))

    df2.describe().show() // dataframe summary statistics

    df2.printSchema()
    // read from bigquery example //

    // val df =
    //   (spark.read
    //     .format("bigquery")
    //     .option("table", "bigquery-public-data.stackoverflow.users")
    //     .load()
    //     .cache())

    df2.write
      .format("bigquery")
      .option(
        "temporaryGcsBucket",
        "cf-spark-temp"
      ) // indirect mode destination gcs bucket
      // .option("writeMethod", "direct")
      .mode("overwrite") // overwrite or append to destination table
      .save(
        "cf-data-analytics.dataproc.distribution"
      ) // define destination table
  }
}
