// Databricks notebook source
// MAGIC %md 
// MAGIC Wykorzystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", "SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()


display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val names = SalesLT.select("TABLE_NAME").as[String].collect.toList
print(names)

for( i <- names ){
  val tab = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)  
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when, count}
import org.apache.spark.sql.Column

//Column type function
def countCols(columns: Array[String]): Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

// COMMAND ----------

val names = SalesLT.select("TABLE_NAME").as[String].collect.toList

// COMMAND ----------

// W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
//Ilosc nulli w kolumnach

for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  tab.select(countCols(tab.columns):_*).show()
}

// COMMAND ----------

//Ilosc nulli w wierszach

def countRows(){
//   https://stackoverflow.com/questions/49252670/iterate-rows-and-columns-in-spark-dataframe
}

// COMMAND ----------

// Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null

for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab_filled = tab.na.fill("999", tab.columns)
  display(tab_filled)
}

// COMMAND ----------

// Użyj funkcji drop żeby usunąć nulle,

for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab_na_dropped = tab.na.drop().show(false)
}

// COMMAND ----------

// wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
import org.apache.spark.sql.functions._

val sales_order_header = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/salesorderheader")

// display(sales_order_header.select("TaxAmt", "Freight"))

sales_order_header.select(stddev("TaxAmt").as("std_TaxAmt"), mean("TaxAmt").as("mean_TaxAmt"), sumDistinct("TaxAmt").as("sum_distinct_TaxAmt"), min("Freight").as("min_Freight"), max("Freight").as("max_Freight"), corr("TaxAmt", "Freight").as("corr_TaxAmt_Freight")).show()


// COMMAND ----------

// Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
val tab_product = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/product")
val groupped_tab_product = tab_product.groupBy("ProductModelId", "Color", "ProductCategoryId").count()

// tab_product.select(min("StandardCost"), max("StandardCost"), mean("StandardCost")).show()
groupped_tab_product.select(sumDistinct("ProductModelId"), count("Color"), last("ProductCategoryId")).show()
// display(groupped_tab_product)

// COMMAND ----------

display(sales_order_header)

// COMMAND ----------

//Zadanie 2 - 3 funkcje UDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

val udf_mean = udf((x: Double, y: Double) => (x+y)/2)
spark.udf.register("udf_mean", udf_mean)

val udf_account_number = udf((s: String) => s.replaceAll("-", "").mkString(""))
spark.udf.register("udf_account_number", udf_account_number)

val udf_dummy = udf((x: Int, y: Int) => x/y)
spark.udf.register("udf_dummy", udf_dummy)

val udf_test = sales_order_header.select(udf_mean($"TaxAmt", $"Freight") as("mean"), udf_account_number($"AccountNumber") as("AccountNumber"), udf_dummy($"SalesOrderID", $"RevisionNumber") as("DummyOrderId/2"))

display(udf_test)

// COMMAND ----------

//Zadanie 3 - brzydki JSON

