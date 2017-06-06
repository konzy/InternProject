import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.regression.LinearRegression

/**
  * @author Brian Konzman
  *
  * This program loads data from the cassandra database and does a linear regression and a k-means clustering algorithm on it.
  */
object MachineLearningTest {
  def main(args: Array[String]): Unit = {

    //create spark session (context)
    val spark = SparkSession
      .builder()
      .appName("SparkCassandraApp")
      .config("spark.cassandra.connection.host", "localhost")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._ //for $ notation

    //set our cassandra cluster to our spark session(context)
    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "localhost") //for example

    //read data specified by the cluster, table, and keyspace into a DataFrame
    val totalSalesByMonth = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> "total_sales_by_month", "keyspace" -> "sales_data"))
      .load()

    totalSalesByMonth.createOrReplaceTempView("total_sales")

    //create a new DataFrame with our total sales column renamed label
    val df = totalSalesByMonth.select("total_sales", "year", "month").withColumnRenamed("total_sales", "label")

    //create a new column with our features as a collection
    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month"))
      .setOutputCol("features")

    //add the column and select only label and features (optional)
    val output = assembler.transform(df).select("label", "features")
    output.show()

    val lr = new LinearRegression()

    // Fit the model to the data
    val lrModel = lr.fit(output)

    println("year, month")
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    // Show the residuals, the RMSE, the MSE, and the R^2 Values.
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    //set our model to have 2 clusters
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(output)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(output)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

  }

}
