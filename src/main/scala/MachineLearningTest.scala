import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by bkonzman on 6/2/17.
  */
object MachineLearningTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_company")
      .config("spark.master", "local")
      .getOrCreate() //for fsql functions

    import spark.implicits._ //for $ notation


    val totalSalesByMonth = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> "total_sales_by_month", "keyspace" -> "sales_data"))



    val customersDataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv("/Users/bkonzman/IdeaProjects/TestScalaSpark/datasets/customers.csv")

    customersDataFrame.createOrReplaceTempView("customers")

    val ordersDataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv("/Users/bkonzman/IdeaProjects/TestScalaSpark/datasets/order.csv")

    ordersDataFrame.createOrReplaceTempView("orders")

    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "localhost") //for example



    val sales = totalSalesByMonth.withColumnRenamed("total_sales", "labels").withColumn("features", ($"year" * 10) + $"month")

    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month"))
      .setOutputCol("features")

    //sales.show

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model

    //val lrModel = lr.fit(sales)

    // Print the coefficients and intercept for logistic regression
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //
    //    // We can also use the multinomial family for binary classification
    //    val mlr = new LogisticRegression()
    //      .setMaxIter(10)
    //      .setRegParam(0.3)
    //      .setElasticNetParam(0.8)
    //      .setFamily("multinomial")
    //
    //    val mlrModel = mlr.fit(totalSalesByMonth)
    //
    //    // Print the coefficients and intercepts for logistic regression with multinomial family
    //    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    //    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    ///////////////////////
  }

}
