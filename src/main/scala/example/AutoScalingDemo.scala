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

//     val rows = 500000000
//     // val rows = 1000

// //
//     val arr = Array(
//       "Spencer",
//       "Andrade",
//       "Buck",
//       "Frank",
//       "Bryan",
//       "Bonilla",
//       "Gillespie",
//       "Mcdaniel",
//       "Frazier",
//       "Nguyen",
//       "Haley",
//       "Hodges",
//       "Rosario",
//       "Suarez",
//       "Bryant",
//       "Hess",
//       "Poole",
//       "Molina",
//       "Riley",
//       "Mejia",
//       "Rowland",
//       "Schmitt",
//       "Dougherty",
//       "Garner",
//       "Payne",
//       "Shields",
//       "Sosa",
//       "Mcguire",
//       "Lam",
//       "Marks",
//       "Casey",
//       "Sanders",
//       "Huynh",
//       "Walls",
//       "Elliott",
//       "Mann",
//       "Wagner",
//       "Levy",
//       "Scott",
//       "Ramirez",
//       "Newman",
//       "Green",
//       "Floyd",
//       "Murray",
//       "Stephenson",
//       "Conway",
//       "Noble",
//       "Avery",
//       "Hanson",
//       "Gaines",
//       "Allen",
//       "Hubbard",
//       "Schwartz",
//       "Wall",
//       "Mora",
//       "Fernandez",
//       "Johnston",
//       "Sparks",
//       "Francis",
//       "Sampson",
//       "Gardner",
//       "Booth",
//       "Peck",
//       "Rocha",
//       "Mcintosh",
//       "Mcdowell",
//       "Campos",
//       "Bass",
//       "Spears",
//       "Flowers",
//       "Walter",
//       "Bowen",
//       "Lowe",
//       "Cunningham",
//       "Cooper",
//       "Christensen",
//       "Singh",
//       "Ho",
//       "Stevens",
//       "Mercer",
//       "Wyatt",
//       "Ware",
//       "Wallace",
//       "Orozco",
//       "Schaefer",
//       "Pittman",
//       "Atkins",
//       "Foster",
//       "Choi",
//       "Osborn",
//       "Rojas",
//       "Olsen",
//       "Maynard",
//       "Sawyer",
//       "Zimmerman",
//       "Simpson",
//       "Moreno",
//       "Gould",
//       "Figueroa",
//       "Meadows",
//       "Marsh",
//       "Boyer",
//       "Potter",
//       "Cantu",
//       "Weaver",
//       "Wong",
//       "Barker",
//       "Kaiser",
//       "Proctor",
//       "Franklin",
//       "Berger",
//       "Valenzuela",
//       "Duarte",
//       "Schroeder",
//       "Tran",
//       "Goodwin",
//       "Mendez",
//       "Lang",
//       "Whitney",
//       "Jensen",
//       "Evans",
//       "Ali",
//       "Fleming",
//       "Mckenzie",
//       "Ruiz",
//       "Brewer",
//       "Frye",
//       "Bridges",
//       "Little",
//       "Waters",
//       "Black",
//       "Giles",
//       "Hogan",
//       "Knapp",
//       "Carson",
//       "Krueger",
//       "Berry",
//       "Cuevas",
//       "Glover",
//       "Ramsey",
//       "Austin",
//       "Solomon",
//       "Park",
//       "Mcmahon",
//       "Stuart",
//       "Hayden",
//       "Sullivan",
//       "Cantrell",
//       "Charles",
//       "Myers",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza",
//       "Fiorenza"
//     )

//     ///// Create Transactions Table /////
//     val df = spark.sqlContext
//       .range(0, rows) // define the number of mock data rows

//     val df2 = df
//       .withColumn("credit_score", rand(seed = 20) * 739)
//       .withColumn(
//         "name",
//         element_at(
//           array(arr.map(lit(_)): _*),
//           lit(ceil(rand() * arr.size)).cast("int")
//         )
//       )
//       .withColumn("transaction_id", expr("uuid()"))

//     // Add Skew to data
//     val df_skew = spark.sqlContext
//       .range(0, rows / 10)

//     val df_skew_out = df_skew
//       .withColumn("credit_score", rand(seed = 10) * 739)
//       .withColumn(
//         "name",
//         element_at(
//           array(arr.map(lit(_)): _*),
//           lit(ceil(rand() * arr.size)).cast("int")
//         )
//       )
//       .withColumn("transaction_id", lit("0a0a0aa0-000a-0a0a-0aa0-000aa000a00a"))

//     val transactions = df2.union(df_skew_out)

//     /// Create Fraud Predictions Table /////
//     val df3 = transactions
//       .select($"transaction_id")
//       .withColumn("wellsfargo", rand(seed = 1))
//       .withColumn("equifax", rand(seed = 2))
//       .withColumn("usbank", rand(seed = 3))

//     val df4 = df3
//       .select(
//         $"transaction_id",
//         expr(
//           "stack(3, 'wellsfargo', wellsfargo, 'equifax', equifax, 'usbank', usbank) as (fraud_model,score)"
//         )
//       )
//       .filter($"score" < .95)
//       .filter($"score" > 0.05)
//       .withColumn(
//         "fraud",
//         when($"score" > .8, "true")
//           .otherwise("false")
//       )
//       .filter($"transaction_id" !== "0a0a0aa0-000a-0a0a-0aa0-000aa000a00a")

//     val fraud_skew = spark.sqlContext
//       .range(0, 1) // define the number of mock data rows
//       .withColumn("transaction_id", lit("0a0a0aa0-000a-0a0a-0aa0-000aa000a00a"))
//       .withColumn("fraud_model", lit("equifax"))
//       .withColumn("score", lit(0))
//       .withColumn("fraud", lit("false"))
//       .select($"transaction_id", $"fraud_model", $"score", $"fraud")

//     val fraud = df4.union(fraud_skew)

//     fraud.write
//       .format("bigquery")
//       .option("temporaryGcsBucket", "cf-spark-temp")
//       .mode("overwrite")
//       .save(
//         "cf-data-analytics.spark_autoscaling.fraud_predictions"
//       )

//     transactions.write
//       .format("bigquery")
//       .option("temporaryGcsBucket", "cf-spark-temp")
//       .mode("overwrite")
//       .save(
//         "cf-data-analytics.spark_autoscaling.transactions"
//       )

    val transactions2 =
      spark.read
        .format("bigquery")
        .option("table", "cf-data-analytics.spark_autoscaling.transactions")
        .load()

    val fraud2 =
      spark.read
        .format("bigquery")
        .option(
          "table",
          "cf-data-analytics.spark_autoscaling.fraud_predictions"
        )
        .load()

    val out =
      transactions2
        .join(
          fraud2,
          Seq("transaction_id"),
          "inner"
        )
        .repartition(100)

    out.write
      .format("bigquery")
      .option("temporaryGcsBucket", "cf-spark-temp")
      .mode("overwrite")
      .save(
        "cf-data-analytics.spark_autoscaling.join_result"
      )

  }
}
