// Databricks notebook source
// MAGIC %md
// MAGIC task 1

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Sample")

// COMMAND ----------

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{DoubleType, DateType, StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._

val transactionSchema = new StructType()
  .add(StructField("AccountId", IntegerType, true))
  .add(StructField("TranDate", StringType, true))
  .add(StructField("TranAmt", DoubleType, true))

// COMMAND ----------

import scala.collection.JavaConversions._

val rowData = Seq(Row( 1, "2011-01-01", 500.0),
        Row( 1, "2011-01-15", 50.0),
        Row( 1, "2011-01-22", 250.0),
        Row( 1, "2011-01-24", 75.0),
        Row( 1, "2011-01-26", 125.0),
        Row( 1, "2011-01-28", 175.0),
        Row( 2, "2011-01-01", 500.0),
        Row( 2, "2011-01-15", 50.0),
        Row( 2, "2011-01-22", 25.0),
        Row( 2, "2011-01-23", 125.0),
        Row( 2, "2011-01-26", 200.0),
        Row( 2, "2011-01-29", 250.0),
        Row( 3, "2011-01-01", 500.0),
        Row( 3, "2011-01-15", 50.0),
        Row( 3, "2011-01-22", 5000.0),
        Row( 3, "2011-01-25", 550.0),
        Row( 3, "2011-01-27", 95.0),
        Row( 3, "2011-01-30", 2500.0))

var transactionsDf = spark.createDataFrame(rowData,transactionSchema)

// COMMAND ----------

display(transactionsDf)

// COMMAND ----------

transactionsDf=transactionsDf.withColumn("TranDate", to_date($"TranDate"))

// COMMAND ----------

val logicalSchema = new StructType()
  .add(StructField("RowID", IntegerType, true))
  .add(StructField("FNmae", StringType, true))
  .add(StructField("Salary", IntegerType, true))

// COMMAND ----------

val rowData = Seq(Row(1,"George", 800),
        Row(2,"Sam", 950),
        Row(3,"Diane", 1100),
        Row(4,"Nicholas", 1250),
        Row(5,"Samuel", 1250),
        Row(6,"Patricia", 1300),
        Row(7,"Brian", 1500),
        Row(8,"Thomas", 1600),
        Row(9,"Fran", 2450),
        Row(10,"Debbie", 2850),
        Row(11,"Mark", 2975),
        Row(12,"James", 3000),
        Row(13,"Cynthia", 3000),
        Row(14,"Christopher", 5000))

var logicalDf = spark.createDataFrame(rowData,logicalSchema)

// COMMAND ----------

display(logicalDf)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

val windowSpec  = Window.partitionBy($"AccountId").orderBy($"TranDate")
transactionsDf.withColumn("RunTotalAmt",sum($"TranAmt") over windowSpec )
  .orderBy($"AccountId",$"TranDate")
  .show()

// COMMAND ----------

transactionsDf.withColumn("RunAvg",avg($"TranAmt") over windowSpec)
  .withColumn("RunTranQty",count("*") over windowSpec)
  .withColumn("RunSmallAmt", min($"TranAmt") over windowSpec)
  .withColumn("RunLargeAmt", max($"TranAmt") over windowSpec)
  .withColumn("RunTotalAmt", sum($"TranAmt") over windowSpec)
  .orderBy($"AccountId",$"TranDate")
  .show()

// COMMAND ----------

val windowSpec2  = Window.partitionBy($"AccountId").orderBy($"TranDate").rowsBetween(-2, 0)

transactionsDf.withColumn("SlideAvg",avg($"TranAmt") over windowSpec2 )
  .withColumn("SlideQty",count("*") over windowSpec2)
  .withColumn("SlideMin", min($"TranAmt") over windowSpec2)
  .withColumn("SlideMax", max($"TranAmt") over windowSpec2)
  .withColumn("SlideTotal", sum($"TranAmt") over windowSpec2)
  .withColumn("RN", row_number().over(windowSpec))
  .orderBy($"AccountId",$"TranDate")
  .show()

// COMMAND ----------

val windowSpec3 = Window.orderBy($"Salary").rowsBetween(Window.unboundedPreceding, 0)
val windowSpec4 = Window.orderBy($"Salary").rangeBetween(Window.unboundedPreceding, 0)

logicalDf.withColumn("SumByRows", sum($"Salary") over windowSpec3)
  .withColumn("SumByRange", sum($"Salary") over windowSpec4).orderBy($"RowID").show()

// COMMAND ----------

val windowSpec5 = Window.partitionBy($"TranAmt").orderBy($"TranDate")

transactionsDf.withColumn("RN", row_number().over(windowSpec5)).orderBy($"TranDate").limit(10).show()

// COMMAND ----------

// MAGIC %md
// MAGIC task 2

// COMMAND ----------

val windowSpec6  = Window.partitionBy($"AccountId").orderBy($"TranDate")

transactionsDf
  .withColumn("RunLead",lead($"TranAmt",1) over windowSpec6 )
  .withColumn("RunLag",lag($"TranAmt",1) over windowSpec6)
  .withColumn("RunFirstValue",first($"TranAmt") over windowSpec6.rowsBetween(-3, 0))
  .withColumn("RunLastValue",last($"TranAmt") over windowSpec6.rowsBetween(-3, 0))
  .withColumn("RunRowNumber",row_number() over windowSpec6 )
  .withColumn("RunDensRank",dense_rank() over windowSpec6)
  .orderBy($"AccountId",$"TranDate")
  .show()

// COMMAND ----------

transactionsDf
  .withColumn("RunFirstValue",first($"TranAmt") over windowSpec6.rangeBetween(Window.unboundedPreceding, 0))
  .withColumn("RunLastValue",last($"TranAmt") over windowSpec6.rangeBetween(Window.unboundedPreceding, 0))
  .orderBy($"AccountId",$"TranDate")
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC task 3

// COMMAND ----------

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela1 = spark.read
      .format("jdbc")
      .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
      .option("user","sqladmin")
      .option("password","$3bFHs56&o123$")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("query",s"SELECT * FROM SalesLT.ProductCategory")
      .load()

val tabela2 = spark.read
      .format("jdbc")
      .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
      .option("user","sqladmin")
      .option("password","$3bFHs56&o123$")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("query",s"SELECT * FROM SalesLT.vGetAllCategories")
      .load()


// COMMAND ----------

display(tabela1)

// COMMAND ----------

display(tabela2)

// COMMAND ----------

val LeftJoinSemi = tabela1.join(tabela2, tabela1("ProductCategoryID") === tabela2("ProductCategoryID"), "leftsemi")
LeftJoinSemi.explain()

// COMMAND ----------

val LeftJoinAnti = tabela1.join(tabela2, tabela1("ProductCategoryID") === tabela2("ProductCategoryID"), "leftanti")
LeftJoinAnti.explain()

// COMMAND ----------

display(LeftJoinSemi)

// COMMAND ----------

display(LeftJoinAnti)

// COMMAND ----------

// MAGIC %md
// MAGIC task 4

// COMMAND ----------

val dropJoin = tabela1.join(tabela2, tabela1("ProductCategoryID") === tabela2("ProductCategoryID")).drop(tabela1("ProductCategoryID"))
display(dropJoin)

// COMMAND ----------

val dropJoin2 = tabela1.join(tabela2,"ProductCategoryID")
display(dropJoin2)

// COMMAND ----------

// MAGIC %md
// MAGIC task 5

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val join3 = tabela1.join(broadcast(tabela2), tabela1("ProductCategoryID") === tabela2("ProductCategoryID"))

join3.explain()

//display(join3)
