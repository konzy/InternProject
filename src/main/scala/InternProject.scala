import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

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

    //    total amount spent by each customer
    val amountSpentAgg = joinedDF.select($"customer_id", $"amount").groupBy($"customer_id").sum("amount")
      .withColumnRenamed("sum(amount)", "sum_amount").sort($"sum_amount" desc).limit(10)

    amountSpentAgg.show

    System.out.println("amount_spent_agg")
    saveToCassandra("amount_spent_agg", amountSpentAgg)

//    customer ordering the most

    // TODO: min max avg median
    val mostOrderedCustomer = amountSpentAgg.select($"customer_id", $"sum_amount").groupBy($"customer_id").max("sum_amount")
      .withColumnRenamed("max(sum_amount)", "max_sum_amount")

    //mostOrderedCustomer.show

    System.out.println("customer_ordered_most")
    saveToCassandra("customer_ordered_most", mostOrderedCustomer)


//    state with the most customers

    // TODO: do by state sort by amount
    val mostCustomersByState = joinedDF.select($"state").groupBy($"state").count().select(first("state"), max("count"))
      .withColumnRenamed("max(count)", "max_count")
      .withColumnRenamed("first(state, false)", "best_state")

    System.out.println("most_customers_by_state")
    saveToCassandra("most_customers_by_state", mostCustomersByState)

    //    total sales in august
    val augTotalSales = ordersDataFrame.filter(month($"order_date").equalTo(8)).groupBy().sum("amount")
    .withColumnRenamed("sum(amount)", "sum_amount")

    System.out.println("total_sales_aug")
    saveToCassandra("total_sales_aug", augTotalSales)

//    growth from 2016-17
    val totalSalesByMonth = joinedDF.groupBy(year($"order_date"), month($"order_date")).sum("amount")
    .withColumnRenamed("year(order_date)", "year")
    .withColumnRenamed("month(order_date)", "month")
    .withColumnRenamed("sum(amount)", "total_sales")

    //totalSalesByMonth.sortBy(r => (r.recent, r.freq))

    System.out.println("total_sales_by_month")
    saveToCassandra("total_sales_by_month", totalSalesByMonth)


    totalSalesByMonth.createOrReplaceTempView("totalSales")
    totalSalesByMonth.show()
    totalSalesByMonth.printSchema()

/////////////////

    val sales = totalSalesByMonth.withColumnRenamed("total_sales", "labels").withColumn("features", ($"year" * 10) + $"month")

    import spark.implicits._
    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month"))
      .setOutputCol("features")

    sales.show

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(sales)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
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
