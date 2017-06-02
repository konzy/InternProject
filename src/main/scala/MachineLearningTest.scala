import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.ml.regression.LinearRegression

/**
  * Created by bkonzman on 6/2/17.
  */
object MachineLearningTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkCassandraApp")
      .config("spark.cassandra.connection.host", "localhost")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._ //for $ notation

    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "localhost") //for example

    val connector = CassandraConnector.apply(spark.sparkContext)
    val session = connector.openSession()

    val totalSalesByMonth = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> "total_sales_by_month", "keyspace" -> "sales_data"))
      .load()

    totalSalesByMonth.createOrReplaceTempView("total_sales")

    totalSalesByMonth.show()

    // Rename the Yearly Amount Spent Column as "label"
    // Also grab only the numerical columns from the data
    // Set all of this as a new dataframe called df
    val df = totalSalesByMonth.select("total_sales", "year", "month").withColumnRenamed("total_sales", "label")

    // An assembler converts the input values to a vector
    // A vector is what the ML algorithm reads to train a model

    // Use VectorAssembler to convert the input columns of df
    // to a single output column of an array called "features"
    // Set the input columns from which we are supposed to read the values.
    // Call this new object assembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month"))
      .setOutputCol("features")

    val output = assembler.transform(df).select("label", "features")

    val lr = new LinearRegression()

    // Fit the model to the data and call this model lrModel
    val lrModel = lr.fit(output)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    // Show the residuals, the RMSE, the MSE, and the R^2 Values.
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }

}
