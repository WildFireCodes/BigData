// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

val path = "/FileStore/tables/Nested.json"
val json = spark.read.option("multiline","true").json(path)
display(json)

// COMMAND ----------

val json_drop = json.withColumn("newCol", $"pathLinkInfo".dropFields("alternateName","sourceFID"))

display(json_drop)
                            

// COMMAND ----------

// MAGIC %md
// MAGIC 2a

// COMMAND ----------

// First example
class Foo(val name: String, val age: Int, val sex: Symbol)

object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}

val fooList = Foo("Hugh Jass", 25, 'male) ::
              Foo("Biggus Dickus", 43, 'male) ::
              Foo("Incontinentia Buttocks", 37, 'female) ::
              Nil

val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
  val title = f.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  z :+ s"$title ${f.name}, ${f.age}"
}

// COMMAND ----------

stringList(0)

// COMMAND ----------

// Second example
val prices: Seq[Double] = Seq(1.5, 2.0, 2.5)
val sum = prices.foldLeft(0.0)(_ + _)

// COMMAND ----------

// MAGIC %md
// MAGIC 2b

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def deleteNestedFields(fields:Map):DataFrame = { 
    //json.foldLeft(json.withColumn("newCol", $.dropFields(fields:_*)) {
    //  (k, v) =>
    }
}

// COMMAND ----------

val excludedNestedFields = Map("pathLinkInfo" -> Array("alternateName","sourceFID"))

//deleteNestedFields(excludedNestedFields)
