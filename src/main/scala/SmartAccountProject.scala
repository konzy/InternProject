import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
  * Created by bkonzman on 6/19/17.
  */

private val ORACLE_CONNECTION_URL: String = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)" +
  "(HOST=dbs-prd-vm-3034.cisco.com)(PORT=1551))(CONNECT_DATA=(SERVICE_NAME=EPICTRN.CISCO.COM)(Server=Dedicated))) "
private val ORACLE_USERNAME: String = "user"
private val ORACLE_PWD: String = "pwd"

object SmartAccountProject {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_company")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._ //for $ notation

    val connectionProperties = new Properties()
    connectionProperties.put("user", ORACLE_USERNAME)
    connectionProperties.put("password", ORACLE_PWD)


    import org.apache.spark.sql.Dataset

    val query = "select XXTMT.compressor.blob_decompress(tpv.pld) as \"UNCOMPRESSED_BLOB\", " +
      "tpv.CREATE_DT,tpv.COMPRESSION_FORMAT from tmt_tran_log ttl, tmt_tran_stat_log ttsl, tmt_tran_stat_mst tsm,tmt_pld_ver tpv " +
      "where ttl.tran_log_id=ttsl.tran_log_id and tsm.tran_stat_id=ttsl.tran_stat_id and ttl.tran_log_id=tpv.tran_log_id " +
      "and ttsl.tran_stat_id=tpv.tran_stat_id and lower(tsm.tran_stat_nm)='order request received.' " +
      "order by tpv.create_dt desc;"

    val jdbcDF = spark.read.jdbc(ORACLE_CONNECTION_URL, query, "UNCOMPRESSED_BLOB", 10001, 499999, 10, connectionProperties)

//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:oracle:dbserver")
//      .option("dbtable", "schema.tablename")
//      .option("user", "username")
//      .option("password", "password")
//      .load()

    val df = spark.sql("select XXTMT.compressor.blob_decompress(tpv.pld) as \"UNCOMPRESSED_BLOB\", " +
      "tpv.CREATE_DT,tpv.COMPRESSION_FORMAT from tmt_tran_log ttl, tmt_tran_stat_log ttsl, tmt_tran_stat_mst tsm,tmt_pld_ver tpv " +
      "where ttl.tran_log_id=ttsl.tran_log_id and tsm.tran_stat_id=ttsl.tran_stat_id and ttl.tran_log_id=tpv.tran_log_id " +
      "and ttsl.tran_stat_id=tpv.tran_stat_id and lower(tsm.tran_stat_nm)='order request received.' " +
      "order by tpv.create_dt desc;")

    import org.apache.spark.sql._

    spark.sqlContext.setConf("Cluster/spark.cassandra.connection.host", "localhost") //for example


    //val df = spark.sql("select body from test limit 3"); // body is a json encoded blob column
    val df2 = df.select(df("UNCOMPRESSED_BLOB").cast(StringType).as("XML"))

    val rdd = df2.rdd.map { case Row(j: String) => j }
    spark.read.json(rdd).show()



  }

}
