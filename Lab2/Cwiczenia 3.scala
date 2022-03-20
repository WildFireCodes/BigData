// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów TD
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val newDf = namesDf.withColumn("epoch", from_unixtime(unix_timestamp())) //zwraca wartosc systemowa
display(newDf)

// COMMAND ----------

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
val feet = newDf.withColumn("feet", $"height"/30.48)
display(feet)

// COMMAND ----------

//Dodaj kolumnę i policz wiek aktorów
val actors_age = feet.withColumn("simple age", floor(datediff(current_date(), $"date_of_birth")/365))

//val cur_date = current_date()

display(actors_age)

// COMMAND ----------

//Odpowiedz na pytanie jakie jest najpopularniesze imię?
val most_popular_name = feet.select(split($"name", " ").getItem(0).as("first name"))
val most_popular_name_new = most_popular_name.groupBy("first name").count().orderBy(desc("count")).show(1)

//najpopularniejsze imie to John - 873 wystapienia

// COMMAND ----------

//Usuń kolumny (bio, death_details)
val drop_cols = feet.drop("bio", "death_details")
drop_cols.printSchema()


// COMMAND ----------

//Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
val selectColumns = drop_cols.columns.toSeq

def remove_(s: String) = s.map(c => if(c == '_') ' ' else c)
val renamedCols = selectColumns.map(remove_)

def capitalize(s: String) = s.toLowerCase.split(' ').map(_.capitalize).mkString(" ")
val Capitalized = renamedCols.map(capitalize)

val newDf = drop_cols.toDF(Capitalized:_*)

display(newDf)

// COMMAND ----------

//Posortuj dataframe po imieniu rosnąco
val ascending = newDf.sort("name")

display(ascending)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

import org.apache.spark.sql.functions._
val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

val newDf = moviesDf.withColumn("epoch", from_unixtime(unix_timestamp())) //zwraca wartosc systemowa

display(newDf)

// COMMAND ----------

//ile lat uplynelo od publikacji filmu
val passed_time = newDf.withColumn("current_time", current_date())
                  .withColumn("date", to_date($"year", "yyyy"))
                  .withColumn("time_from_publication", floor(datediff($"date", $"current_time")/(-365)))

display(passed_time)

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
val film_budget = passed_time.withColumn("new_budget", regexp_replace(newDf("budget"), "\\D+", ""))

display(film_budget)

// COMMAND ----------

//Usuń wiersze z dataframe gdzie wartości są null
val no_null = film_budget.na.drop

display(no_null)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

import org.apache.spark.sql.functions._
val newDf = namesDf.withColumn("epoch", from_unixtime(unix_timestamp())) //zwraca wartosc systemowa

display(newDf)

// COMMAND ----------

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
val selected_cols_names = newDf.columns.toList.slice(5,15)

val mean_votes_cols = newDf.select(selected_cols_names.map(c => col(c)): _*)

val mean_votes = newDf.withColumn("mean_votes[1-10]", mean_votes_cols.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2) / lit(mean_votes_cols.columns.length))

display(mean_votes)

// COMMAND ----------

//Kto daje lepsze oceny? - dziewczyny
val ratingsAvg = mean_votes.select("females_allages_avg_vote","males_allages_avg_vote").agg(avg("females_allages_avg_vote") as "female_votes_avg", avg("males_allages_avg_vote") as "male_votes_avg")//.explain()

display(ratingsAvg)

// COMMAND ----------

//mediana ????
val median_votes = newDf.withColumn("median_votes", )


// COMMAND ----------

//Dla jednej z kolumn zmień typ danych do long
import org.apache.spark.sql.types.{LongType}

val cast_type = newDf.withColumn("weighted_avg_vote_long ", col("weighted_average_vote").cast(LongType))

cast_type.printSchema

// COMMAND ----------

//Zadanie 2
//SPARK UI:
// 1. Jobs - status zadan i czas trwania
// 2. Stages - stan wszystkich etapow zadan
// 3. Storage - listuje partycje i pokazuje ich wielkosc
// 4. Environment - pokazuje zmienne srodowiska
// 5. Executors - pokazuje liste egzekutorow zadan wraz z ich stanem


// COMMAND ----------

// Zadanie 3 
//Do jednej z Dataframe dołóż transformacje groupBy i porównaj jak wygląda plan wykonania 
//display(cast_type)

val select_explain = cast_type.select("imdb_title_id", "total_votes").explain()
val groupby_explain = cast_type.select("imdb_title_id", "total_votes").groupBy("imdb_title_id").count().explain()


// COMMAND ----------

// Zadanie 4
val df = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT * FROM information_schema.tables) as x")
      .option("user", "sqladmin")
      .option("password", "$3bFHs56&o123$")
      .load()

display(df)
