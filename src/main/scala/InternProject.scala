import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by bkonzman on 5/31/17.
  */
object InternProject {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_company")
      .config("spark.master", "local")
      .getOrCreate() //for fsql functions

    import spark.implicits._ //for $ notation

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

    val joinedDF = customersDataFrame.join(ordersDataFrame)
      .where(customersDataFrame("customer_id") === ordersDataFrame("customer_id"))
      .drop(ordersDataFrame("customer_id"))
    joinedDF.cache()

    val amountSpentAgg = joinedDF.select($"customer_id", $"amount").groupBy($"customer_id").sum("amount")
      .withColumnRenamed("sum(amount)", "sum_amount").sort($"sum_amount" desc).limit(10)

    System.out.println("amount_spent_agg")
    amountSpentAgg.show

    saveToCassandra("amount_spent_agg", amountSpentAgg)


    val aggregateSummary = joinedDF.select($"customer_id", $"amount")
      .agg(
        functions.count("customer_id").as("total_customers"),
        functions.sum("amount").as("total_spent"),
        functions.max("amount").as("max_spent"),
        functions.min("amount").as("least_spent"),
        functions.avg("amount").as("avg_spent"),
        functions.mean("amount").as("mean_spent"),
        functions.skewness("amount").as("skewness")
      )

    aggregateSummary.show()

    System.out.println("agg_summary")
    saveToCassandra("agg_summary", aggregateSummary)


    val mostCustomersByState = joinedDF.select($"state")
      .groupBy($"state")
      .count()
      .sort($"count".desc)

    mostCustomersByState.show()

    System.out.println("customers_by_state")
    saveToCassandra("customers_by_state", mostCustomersByState)


    val augTotalSales = ordersDataFrame.filter(month($"order_date").equalTo(8)).groupBy().sum("amount")
    .withColumnRenamed("sum(amount)", "sum_amount")

    System.out.println("total_sales_aug")
    saveToCassandra("total_sales_aug", augTotalSales)


    val totalSalesByMonth = joinedDF.groupBy(year($"order_date"), month($"order_date")).sum("amount")
      .withColumnRenamed("year(order_date)", "year")
      .withColumnRenamed("month(order_date)", "month")
      .withColumnRenamed("sum(amount)", "total_sales")
      .sort($"year", $"month")

    totalSalesByMonth.show()

    System.out.println("total_sales_by_month")
    saveToCassandra("total_sales_by_month", totalSalesByMonth)


    totalSalesByMonth.createOrReplaceTempView("totalSales")
    totalSalesByMonth.show()
    totalSalesByMonth.printSchema()

/////////////////

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



    joinedDF.unpersist()

  }

  def saveToCassandra(nameOfTable: String, table: DataFrame): Unit = {
    table.write.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> nameOfTable, "keyspace" -> "sales_data"))
      .mode(SaveMode.Append)
      .save()
  }
}
