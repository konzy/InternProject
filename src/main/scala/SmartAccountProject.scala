import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
  * Created by bkonzman on 6/19/17.
  */



object SmartAccountProject {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_company")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._ //for $ notation

    //1.6.2



//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:oracle:dbserver")
//      .option("dbtable", "schema.tablename")
//      .option("user", "username")
//      .option("password", "password")
//      .load()





    import org.apache.spark.sql._

    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "ccwa-cssdb-stg1-01")
    spark.sqlContext.setConf("Cluster/spark.cassandra.auth.username", "oprepusr")
    spark.sqlContext.setConf("Cluster/spark.cassandra.auth.password", "Rtp987")

    val b2bXml = spark.sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("cluster" -> "Cluster", "keyspace"->"test", "table"->"b2b_xml_data")).load()





    //val df = spark.sql("select body from test limit 3"); // body is a json encoded blob column
    val df2 = b2bXml.select(b2bXml("UNCOMPRESSED_BLOB").cast(StringType).as("XML"))

    df2.show()

    val rdd = df2.rdd.map {
      case Row(j: String) => j
    }

    rdd.foreach(println)


//    spark.read.format("com.databricks.spark.xml").load(rdd)



  }

}
