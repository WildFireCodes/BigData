// Databricks notebook source
// MAGIC %scala
// MAGIC val actorsUrl = "https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"

// COMMAND ----------

// MAGIC %scala
// MAGIC val actorsFile = "actors.csv"

// COMMAND ----------

// MAGIC %scala
// MAGIC //Zadanie 1 - tworzenie schematu
// MAGIC import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}
// MAGIC 
// MAGIC val schema = StructType(Array(
// MAGIC   StructField("imdb_title_id", StringType, true),
// MAGIC   StructField("ordering", IntegerType, true),
// MAGIC   StructField("imdb_name_id", StringType, true),
// MAGIC   StructField("category", StringType, true),
// MAGIC   StructField("job", StringType, true),
// MAGIC   StructField("characters", StringType, true)))
// MAGIC 
// MAGIC val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
// MAGIC 
// MAGIC val actorsDf = spark.read.format("csv")
// MAGIC               .option("header", "true")
// MAGIC               .option("inferSchema", "true")
// MAGIC               .load(filePath)
// MAGIC 
// MAGIC val actorsDfSchema = spark.read.format("csv")
// MAGIC             .option("header", "true")
// MAGIC             .schema(schema)
// MAGIC             .load(filePath)
// MAGIC 
// MAGIC // display(actorsDf)
// MAGIC // display(actorsDfSchema)
// MAGIC actorsDf.printSchema
// MAGIC actorsDfSchema.printSchema

// COMMAND ----------

// MAGIC %scala
// MAGIC // Zadanie 2 - tworzenie pliku .JSON ze schmatem z kilku rzedow danych
// MAGIC //kilka rzedow w JSONie -> DFs -> DFs + schema -> new.JSON
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC 
// MAGIC 
// MAGIC // val actorsCols = actorsDf.select("category", "characters")
// MAGIC 
// MAGIC // val schema = StructType(Array(
// MAGIC //   StructField("category", StringType, true),
// MAGIC //   StructField("characters", StringType, true)))
// MAGIC 
// MAGIC // val actorsJSON = actorsCols.write.option("header", "true").json("dbfs:/FileStore/tables/Files/file_2.json")
// MAGIC 
// MAGIC // val ff = spark.read.json("dbfs:/FileStore/tables/Files/file_2.json")
// MAGIC // display(ff)
// MAGIC 
// MAGIC val actorsRows = """
// MAGIC     [{
// MAGIC     "imdb_title_id": "tt0000009",
// MAGIC     "ordering": 1,
// MAGIC     "imdb_name_id": "nm0063086",
// MAGIC     "category": "actress",
// MAGIC     "job": "null",
// MAGIC     "characters": [
// MAGIC       "Miss Geraldine Holbrook (Miss Jerry)"
// MAGIC       ]
// MAGIC   },
// MAGIC   {
// MAGIC     "imdb_title_id": "tt0000009",
// MAGIC     "ordering": 2,
// MAGIC     "imdb_name_id": "nm0183823",
// MAGIC     "category": "actor",
// MAGIC     "job": "null",
// MAGIC     "characters": [
// MAGIC       "Mr. Hamilton"
// MAGIC       ]
// MAGIC   },
// MAGIC   {
// MAGIC     "imdb_title_id": "tt0002844",
// MAGIC     "ordering": 4,
// MAGIC     "imdb_name_id": "nm0137288",
// MAGIC     "category": "actress",
// MAGIC     "job": "null",
// MAGIC     "characters": [
// MAGIC       "Lady Beltham",
// MAGIC       "maîtresse de Fantômas"
// MAGIC       ]
// MAGIC   }]
// MAGIC   """
// MAGIC 
// MAGIC val schema = StructType(Array(
// MAGIC   StructField("imdb_title_id", StringType, true),
// MAGIC   StructField("ordering", IntegerType, true),
// MAGIC   StructField("imdb_name_id", StringType, true),
// MAGIC   StructField("category", StringType, true),
// MAGIC   StructField("job", StringType, true),
// MAGIC   StructField("characters", StringType, true)))
// MAGIC 
// MAGIC def convertJSONtoDataFrame(Rows: String, schema: StructType = null): DataFrame = {
// MAGIC   val reader = spark.read
// MAGIC   Option(schema).foreach(reader.schema) //zapytac, dlaczego Option jest bez . ?
// MAGIC   reader.json(sc.parallelize(Array(Rows))) //tworzy RDD - https://sparkbyexamples.com/apache-spark-rdd/how-to-create-an-rdd-using-parallelize/
// MAGIC }
// MAGIC 
// MAGIC val actorsRowsDf = convertJSONtoDataFrame(actorsRows, schema)
// MAGIC display(actorsRowsDf)
// MAGIC 
// MAGIC val actorsRowsDfJSON = actorsRowsDf.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/actorsRowsDfJSON.json")

// COMMAND ----------

// MAGIC %scala
// MAGIC //Zadanie 4
// MAGIC 
// MAGIC val brokendata = """
// MAGIC     [{
// MAGIC       'imdb_title_id': 'tt0110009', 
// MAGIC       'ordering': 2, 
// MAGIC       'imdb_name_id': 'nm003086', 
// MAGIC       'category': 1, 'job': 'null', 
// MAGIC       'characters': 'Miss Geraldine Holbrook (Miss Jerry)'
// MAGIC     },
// MAGIC     {
// MAGIC       'imdb_title_id': 123, 
// MAGIC       'ordering': 2, 
// MAGIC       'imdb_name_id': 'nm003086', 
// MAGIC       'category': 1, 
// MAGIC       'job': 'null', 
// MAGIC       'characters': 'Miss Geraldine Holbrook (Miss Jerry)'
// MAGIC     },
// MAGIC     {
// MAGIC       xxxxxxxxxxxxxxxx
// MAGIC     }]
// MAGIC """
// MAGIC 
// MAGIC val brokendataDf = convertJSONtoDataFrame(brokendata)
// MAGIC 
// MAGIC val brokendataDfJSON = brokendataDf.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/brokendataDfJSON.json")
// MAGIC 
// MAGIC val badRecords = spark.read.format("json")
// MAGIC .option("inferSchema","true")
// MAGIC .option("badRecordsPath", "dbfs:/FileStore/tables/Files/badrecords")
// MAGIC .load("dbfs:/FileStore/tables/Files/brokendataDfJSON.json")
// MAGIC 
// MAGIC val permissive = spark.read.format("json")
// MAGIC .option("inferSchema","true")
// MAGIC .option("mode", "PERMISSIVE")
// MAGIC .load("dbfs:/FileStore/tables/Files/brokendataDfJSON.json")
// MAGIC   
// MAGIC val dropmalformed = spark.read.format("json")
// MAGIC .option("inferSchema","true")
// MAGIC .option("mode", "DROPMALFORMED")
// MAGIC .load("dbfs:/FileStore/tables/Files/brokendataDfJSON.json")
// MAGIC 
// MAGIC val failfast = spark.read.format("json")
// MAGIC .option("inferSchema","true")
// MAGIC .option("mode", "FAILFAST")
// MAGIC .load("dbfs:/FileStore/tables/Files/brokendataDfJSON.json")

// COMMAND ----------

// MAGIC %scala
// MAGIC //zadanie 5
// MAGIC val file = actorsDf.write.mode("overwrite").option("header", "true").parquet("dbfs:/FileStore/tables/Files/file1.parquet")
// MAGIC 
// MAGIC val parquet_file = spark.read.format("parquet")
// MAGIC .option("inferSchema","true")
// MAGIC .load("dbfs:/FileStore/tables/Files/file1.parquet")
// MAGIC 
// MAGIC val file2 = actorsDf.write.mode("overwrite").option("header", "true").json("dbfs:/FileStore/tables/Files/file2.json")
// MAGIC 
// MAGIC val json_file = spark.read.format("json")
// MAGIC .option("inferSchema","true")
// MAGIC .load("dbfs:/FileStore/tables/Files/file1_2.json")
// MAGIC 
// MAGIC display(parquet_file)
