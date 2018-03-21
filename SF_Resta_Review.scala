// Databricks notebook source

// MAGIC * Use CSV reader to read in each data file
// MAGIC * Convert RDD to DataFrame

// COMMAND ----------

// MAGIC %md ### Setting up input data sets

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import spark.implicits._
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
val baseDir = "/FileStore/tables/x01djp7u1502264915317"
val raw_inspections = sc.textFile(s"$baseDir/inspections_plus.tsv")
val violations = sc.textFile(s"$baseDir/violations_plus.tsv")
val business = sc.textFile(s"$baseDir/businesses_plus.tsv")

// COMMAND ----------

case class inspectData(i_bus_id: Int, score: Int, i_date: Int, itype: String)

val regex_insp = "(\\d{2,})\\t(\\d{1,})\\t(\\d{8})\\t(\\w.*)".r

def parseInspLine(line:String) : inspectData = {
  val groupList = regex_insp.findAllIn(line).matchData.toList
  if (groupList.length > 0) {
     val group = groupList(0)
      inspectData(group.group(1).toInt, group.group(2).toInt, group.group(3).toInt, group.group(4))
  } 
  else {
    inspectData(-1,-1,-1,"InvalidData")
 }
}
val FilteredInspData=raw_inspections.map(l => parseInspLine(l)).toDF()
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
val regex_viol = "(\\d{2,})\\t(\\d{8})\\t(\\d{6})\\t(\\w*\\s\\w*)\\t(\\w.*)".r

case class voilData(v_bus_id: Int, v_date: String, viol_type: Int, risk_ct: String, desc:String)

def parse1ViolLine(line:String) : voilData = {
  val groupList = regex_viol.findAllIn(line).matchData.toList
  if (groupList.length > 0) {
     val group = groupList(0)
      voilData(group.group(1).toInt, group.group(2), group.group(3).toInt, group.group(4), group.group(5))
  } 
  else {
    voilData(-1,"Invalid Data",-1,"InvalidData","InvalidData")
 }
}
val FilteredvoilData = violations.map(l => parse1ViolLine(l)).toDF()
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
val regex_busData ="(\\d{2,})\\t([^\\t]*)\\t([^\\t]*)\\t(sf|SF|S[^a-z]F.|San\\sFrancisco|S\\sF)\\t(\\d{5})\\t.*".r

case class busData(b_bus_id: Int,name:String,address:String,city:String,zip:Int)

def parseBusLine(line:String) : busData = {
  val groupList = regex_busData.findAllIn(line).matchData.toList
  if (groupList.length > 0) {
     val group = groupList(0)
      busData(group.group(1).toInt,group.group(2),group.group(3),group.group(4).toLowerCase(),group.group(5).toInt)
        
  } 
  else {
    busData(-1,"invalid data","invalid data","invalid data",-1)
 }
}
val FilteredBusiData = business.map(l => parseBusLine(l)).toDF()

// COMMAND ----------

// MAGIC %md #### 1) What is the inspection score distribution like? (inspections_plus.csv) 
// MAGIC 
// MAGIC Expected output - (***score, count***)

// COMMAND ----------

val insp_score_dist=FilteredInspData.groupBy("score").count()
insp_score_dist.where(insp_score_dist("score") >=0).orderBy(desc("count")).show()

// COMMAND ----------

// MAGIC %md #### 2) What is the risk category distribution like? (violations_plus.csv) 
// MAGIC Expected output - (***risk category, count***)

// COMMAND ----------

val rist_ct_distr=FilteredvoilData.groupBy("risk_ct").count().show()

// COMMAND ----------

// MAGIC %md #### 3) Which 20 businesses got lowest scores? (inspections_plus.csv, businesses_plus.csv)
// MAGIC (This should be more low score rather than lowest score)
// MAGIC 
// MAGIC Expected columns - (***business_id,name,address,city,postal_code,score***)

// COMMAND ----------

val result01= FilteredInspData.join(FilteredBusiData, FilteredInspData("i_bus_id") === FilteredBusiData("b_bus_id"),"inner")
val result02= result01.where(result01("score") >=0).distinct
val result03= result02.orderBy(asc("score")).select("i_bus_id","name","address","city","zip","score")
result03.show()

// COMMAND ----------

// MAGIC %md #### 4) Which 20 businesses got highest scores? (inspections_plus.csv, businesses_plus.csv)
// MAGIC Expected columns - (***business_id,name,address,city,postal_code,score***)

// COMMAND ----------

val result04= result02.orderBy(desc("score")).select("i_bus_id","name","address","city","zip","score")
result04.show(20)

// COMMAND ----------

// MAGIC %md #### 5) Among all the restaurants that got 100 score, what kind of violations did they get (if any)
// MAGIC (inspections_plus.csv, violations_plus.csv)
// MAGIC 
// MAGIC (Examine "High Risk" violation only)
// MAGIC 
// MAGIC Expected columns - ***(business_id, risk_category, date, description)***
// MAGIC 
// MAGIC Note - format the date in (***month/day/year***)

// COMMAND ----------

//val result04=result01.where(result01("score") ===100).distinct

val result05= FilteredInspData.join(FilteredvoilData, FilteredInspData("i_bus_id") === FilteredvoilData("v_bus_id"),"inner")
val result06 = result05.select($"score",$"i_bus_id",$"risk_ct",$"desc", unix_timestamp($"v_date", "yyyyMMdd").cast(TimestampType).as("date")).where(result05("score") ===100).distinct
val result07= result06.where(result06("risk_ct")==="High Risk").groupBy("desc").count().orderBy(desc("count"))
result06.show()


// COMMAND ----------

// MAGIC %md #### 6) Average inspection score by zip code
// MAGIC 
// MAGIC Expected columns - (***zip, average score with only two digits after decimal***)

// COMMAND ----------

val result08= result03.groupBy("zip").avg("score").withColumnRenamed("avg(score)", "avreage_score").orderBy(desc("avreage_score")).withColumn("avreage_score", round($"avreage_score", 2)).show()


// COMMAND ----------

// MAGIC %md #### 7) Compute the proportion of all businesses in each neighborhood that have incurred at least one of the violations
// MAGIC * "High risk vermin infestation"
// MAGIC * "Moderate risk vermin infestation"
// MAGIC * "Sewage or wastewater contamination”
// MAGIC * "Improper food labeling or menu misrepresentation"
// MAGIC * "Contaminated or adulterated food”
// MAGIC * "Reservice of previously served foods"
// MAGIC * "Expected output: zip code, percentage"
// MAGIC 
// MAGIC This question is asking for each neighborhood, what is the proportion of businesses that have incurred at least one of the above nasty violations
// MAGIC 
// MAGIC Note: use UDF to determine which violations match with one of the above extremely bad violations
// MAGIC 
// MAGIC Expected columns - (***zip code, total violation count, extreme violation count, proportion with only two digits after decimal***)

// COMMAND ----------


val deptUdf = udf[String,String] ( desc =>
  desc match {
case desc if desc.contains("High risk vermin") => "1"
case desc if desc.contains("Moderate risk vermin") => "1"
case desc if desc.contains("Sewage or wastewater") => "1"
case desc if desc.contains("Improper food labeling") => "1"
case desc if desc.contains("Contaminated or adulterated") => "1"
case desc if desc.contains("Reservice of previously")=> "1"
case desc if desc.contains("Expected output: zip code")  => "1"
case _ => "0"
}
)

val res11 = FilteredvoilData.where(FilteredvoilData("risk_ct").contains("High Risk")).distinct.select($"v_bus_id",deptUdf($"desc") as "descScore")
val res13 = FilteredBusiData.join(res11, FilteredBusiData("b_bus_id") === res11("v_bus_id"),"inner").distinct().select("zip","descScore")

val res14 =res13.withColumn("newdescScore", res13("descScore").cast(IntegerType)).drop("descScore").groupBy("zip").avg("newdescScore").orderBy(desc("avg(newdescScore)"))

val res15  = res14 .withColumnRenamed("avg(newdescScore)", "proportion").withColumn("proportion", round($"proportion", 2)).show()



// COMMAND ----------

// MAGIC %md ### 8) Are SF restaurants clean? Justify your answer
// MAGIC 
// MAGIC (***Make to sure backup your answer with data - don't just state your opinion***)

// COMMAND ----------

val viol = FilteredBusiData.join(FilteredvoilData, FilteredBusiData("b_bus_id") === FilteredvoilData("v_bus_id"),"inner")
val viol_filt= viol.select("b_bus_id","risk_ct").distinct.groupBy("risk_ct").count().show()


display(viol.select("b_bus_id","desc").where(!(viol("desc").contains("InvalidData"))).distinct.groupBy("desc").count().orderBy(desc("count")).limit(15))

//Given the high distribution of High and Medium risk violations, SF restaurants are not clean
