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
      .withColumnRenamed("sum(amount)", "sum_amount").sort($"sum_amount" desc)

    System.out.println("amount_spent_agg")
    amountSpentAgg.show

    saveToCassandra("amount_spent_agg", amountSpentAgg)

    import org.apache.spark.sql.functions._

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
      .withColumn("unique_id", monotonically_increasing_id())


    totalSalesByMonth.show()
    totalSalesByMonth.printSchema()

    System.out.println("total_sales_by_month")
    saveToCassandra("total_sales_by_month", totalSalesByMonth)


    totalSalesByMonth.createOrReplaceTempView("totalSales")
    totalSalesByMonth.show()
    totalSalesByMonth.printSchema()

    joinedDF.unpersist()

  }

  def saveToCassandra(nameOfTable: String, table: DataFrame): Unit = {
    table.write.format("org.apache.spark.sql.cassandra")
      .options(Map("cluster" -> "Cluster", "table" -> nameOfTable, "keyspace" -> "sales_data"))
      .mode(SaveMode.Append)
      .save()
  }
}
