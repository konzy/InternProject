import org.apache.spark.sql._

/**
  * @author Brian Konzman
  * This program creates several tables in a Cassandra database from mock sales data, namely order.csv and customer.csv
  * These two tables are joined together and the resulting DataFrame is cached and used throughout the rest of the program
  * to create 5 more tables.  These tables are created in memory locally then stored back to the cassandra database.
  *
  * amount_spent_agg
  * total amount spent per customer over lifetime
  *
  * agg_summary
  * gives several aggregations, max, min, mean, skew...
  *
  * customers_by_state
  * gives the total customers by state
  *
  * total_sales_aug
  * gives the total sales in augusts
  *
  * total_sales_by_month
  * gives total sales by month and year
  *
  */

object InternProject {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_company")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._ //for $ notation

    //read data from mock csv customer
    val customersDataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv("/Users/bkonzman/IdeaProjects/TestScalaSpark/datasets/customers.csv")

    customersDataFrame.createOrReplaceTempView("customers")

    //read from mock csv order
    val ordersDataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv("/Users/bkonzman/IdeaProjects/TestScalaSpark/datasets/order.csv")

    ordersDataFrame.createOrReplaceTempView("orders")

    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "localhost") //for example

    //natural join on customers and order DataFrames
    val joinedDF = customersDataFrame.join(ordersDataFrame)
      .where(customersDataFrame("customer_id") === ordersDataFrame("customer_id"))
      .drop(ordersDataFrame("customer_id"))
    joinedDF.cache()

    //Create DataFrame that gives the total amount spent per Customer
    val amountSpentAgg = joinedDF.select($"customer_id", $"amount").groupBy($"customer_id").sum("amount")
      .withColumnRenamed("sum(amount)", "sum_amount").sort($"sum_amount" desc)

    //save DataFrame to cassandra database
    saveToCassandra("amount_spent_agg", amountSpentAgg)

    import org.apache.spark.sql.functions._ //allows you to use count() instead of functions.count()

    //create DataFrame with many reduce aggregations
    val aggregateSummary = joinedDF.select($"customer_id", $"amount")
      .agg(
        count("customer_id").as("total_customers"),
        sum("amount").as("total_spent"),
        max("amount").as("max_spent"),
        min("amount").as("least_spent"),
        avg("amount").as("avg_spent"),
        mean("amount").as("mean_spent"),
        skewness("amount").as("skewness")
      )

    saveToCassandra("agg_summary", aggregateSummary)

    //create DataFrame with total customers by state
    val mostCustomersByState = joinedDF.select($"state")
      .groupBy($"state")
      .count()
      .sort($"count".desc)

    saveToCassandra("customers_by_state", mostCustomersByState)

    //create DataFrame that gives the total amount ordered in dollars
    val augTotalSales = ordersDataFrame.filter(month($"order_date").equalTo(8)).groupBy().sum("amount")
    .withColumnRenamed("sum(amount)", "sum_amount")

    saveToCassandra("total_sales_aug", augTotalSales)

    //create DataFrame that gives total sales per month
    val totalSalesByMonth = joinedDF.groupBy(year($"order_date"), month($"order_date")).sum("amount")
      .withColumnRenamed("year(order_date)", "year")
      .withColumnRenamed("month(order_date)", "month")
      .withColumnRenamed("sum(amount)", "total_sales")
      .sort($"year", $"month")
      .withColumn("unique_id", monotonically_increasing_id())

    saveToCassandra("total_sales_by_month", totalSalesByMonth)

    joinedDF.unpersist()

  }

  /**
    * This is a helper method for writing to our cassandra cluster.  Given a DataFrame and the name of the table
    * this method will store the data in the already created cassandra table.
    *
    * @param nameOfTable
    * @param table
    */
  def saveToCassandra(nameOfTable: String, table: DataFrame): Unit = {
    table.write.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> nameOfTable, "keyspace" -> "sales_data"))
      .mode(SaveMode.Append)
      .save()
  }
}
