package org.example

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS
import org.apache.iceberg.hive.TestHiveMetastore
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import java.io.File

class SparkIcebergHiveCatalogSpec extends FlatSpec with BeforeAndAfterAll {

  protected var spark: SparkSession = _
  protected var metastore: TestHiveMetastore = _


  override def beforeAll(): Unit = {
    // Start hive metastore thrift service
    metastore = new TestHiveMetastore()
    metastore.start()

    val uris = metastore.hiveConf().get(METASTOREURIS.varname)

    cleanupSparkWarehouse()

    spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.hadoop." + METASTOREURIS.varname, uris)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .enableHiveSupport()
      .getOrCreate()

    createDatabase()
  }

  override def afterAll(): Unit = {
    if (metastore != null) {
      metastore.stop()
      metastore = null
    }

    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private def createDatabase(): Unit = {
    // Drop the database if it already exists
    spark.sql("DROP DATABASE IF EXISTS junit_db CASCADE")
    // Create a new database
    spark.sql("CREATE DATABASE junit_db")
  }

  it should "perform a parquet table test" in {
    performTableTest("junit_parquet_table", "PARQUET")
  }

  it should "perform an iceberg table test" in {
    performTableTest("junit_iceberg_table", "ICEBERG")
  }

  private def performTableTest(tableName: String, tableType: String): Unit = {
    // Create a table in the database
    spark.sql(s"CREATE TABLE junit_db.$tableName (id BIGINT, data STRING) USING $tableType")

    // Insert data into the table
    spark.sql(s"INSERT INTO junit_db.$tableName VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    // Perform assertions to validate the table creation and data insertion
    assert(spark.catalog.databaseExists("junit_db"), s"Database junit_db should exist")
    assert(spark.catalog.tableExists(s"junit_db.$tableName"), s"Table $tableName should exist in database junit_db")

    // Show the data in the table
    spark.table(s"junit_db.$tableName").show(false)

    // Check the number of rows in the table
    val tableData = spark.table(s"junit_db.$tableName").collect()
    assert(tableData.length == 3, s"Expected 3 rows in the $tableName")
  }

  private def cleanupSparkWarehouse(): Unit = {
    val sparkWarehouseDir = new File(s"${sys.props.getOrElse("user.dir", "")}/spark-warehouse")
    if (sparkWarehouseDir.exists()) {
      try {
        FileUtils.deleteDirectory(sparkWarehouseDir)
        println("Spark warehouse directory deleted successfully.")
      } catch {
        case e: Throwable =>
          println(s"Failed to delete Spark warehouse directory: ${e.getMessage}")
      }
    }
  }
}
