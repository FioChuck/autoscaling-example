import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.DataFrame

trait DataGenerator {

  def data_gen(spark: SparkSession): DataFrame = {

    val rows = 2222000

    val arr = Array(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
    val df = spark.sqlContext
      .range(0, rows) // define the number of mock data rows

    val df2 = df
      .withColumn(
        "partition",
        element_at(
          array(arr.map(lit(_)): _*),
          lit(ceil(rand() * arr.size)).cast("int")
        )
      )
      .withColumn("id", expr("uuid()"))

    df2
  }

  def data_write(spark: SparkSession, df: DataFrame, path: String): Unit = {
    df.write
      .format("bigquery")
      .option("temporaryGcsBucket", "cf-spark-temp")
      .option("preferredMinParallelism", 10)
      .mode("overwrite")
      .save(
        path
      )
  }

}
