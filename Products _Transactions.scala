// Databricks notebook source
// MAGIC ##### Transaction Data Format: 
// MAGIC   * 0 -> transaction date
// MAGIC   * 1 -> time,
// MAGIC   * 2 -> customer ID, 
// MAGIC   * 3 -> product ID, 
// MAGIC   * 4 -> quantity, 
// MAGIC   * 5 -> transaction total
// MAGIC   
// MAGIC ##### Product Data Format: 
// MAGIC   * 0 -> product Id
// MAGIC   * 1 -> name,
// MAGIC   * 2 -> price,
// MAGIC   * 3 -> quantity

// COMMAND ----------

// MAGIC %md #### Setup Steps

// COMMAND ----------

val folder = "/FileStore/tables/82du0w2u1501571038151/"
val transactions = s"$folder/transactions.txt"
val products = s"$folder/data_products.txt"
val txData = spark.sparkContext.textFile(transactions).map(line => line.split("#"))
val prodData = spark.sparkContext.textFile(products).map(line => line.split("#"))

val txTuples = txData.map(tx => (tx(2).toInt, tx(3).toInt,tx(4).toInt,tx(5).toDouble))
val P_Ids = prodData.map(pd => (pd(0).toInt, pd(1).toString.toLowerCase(),pd(2).toDouble,pd(3).toInt))

// COMMAND ----------

// MAGIC %md #### 1) Top 5 customers made the most # of transactions
// MAGIC 
// MAGIC ##### Expected output: (memberId, # of transactions)

// COMMAND ----------

val txCids = txTuples.map {case(cid,pid,qty,trx) => (cid, 1) }
val cusTxs = txCids.reduceByKey(_ + _).sortBy(_._2, false).take(5)
txTuples.take(5)

// COMMAND ----------

// MAGIC %md #### 2) Give 5% discount for customers who bought 2 or more Barbie Shopping Mall Playsets
// MAGIC 
// MAGIC (User mapValues API to update the values without changing the key)
// MAGIC 
// MAGIC ##### Expected output: (custId, (prodId, quantity, oldTotal, newTotal))

// COMMAND ----------

val BarbiePid = P_Ids.filter(_._2.contains("barbie shopping mall playset")).map(x=> (x._1,(x._3)))//pid, price
val txCidsnew= txTuples.map(x=> (x._2,(x._1,x._3)))//pid, cid, qty 
val Consolida= BarbiePid.join(txCidsnew).map{case(x1,(x2,(x3,x4))) => (x1,x2,x3,x4)}.filter(x=>x._4>=2)//pid,cid,price,qty
val discount= Consolida.map{case((pid,price,cid,qty)) => (cid,pid,qty,price*qty,price*qty*.95)}.collect//cid,pid,qty,ototal,nTot


// COMMAND ----------

// MAGIC %md #### 3) List of products that didn't sell yesterday
// MAGIC (use either substractKey or performing a left or right outer join between products and transactions)
// MAGIC 
// MAGIC Expected output: (productId, name, price, quantity)

// COMMAND ----------

// there are two ways of doing this - 1) using substractKey 2) using leftoutejoin
val txPIds = txTuples.map(x=> (x._2,(1)))// pid
val nPIds = P_Ids.map(x => (x._1,(x._2,x._3,x._4))) // pid, name, price, qty
val notsold = nPIds.subtractByKey(txPIds).take(100)//pids, name, price, qty

// COMMAND ----------

// there are two ways of doing this - 1) using substractKey 2) using leftoutejoin
val txPIdsA=txTuples.map(a=>(a._2,(1))).reduceByKey(_+_).sortByKey()
val nPIdsA=P_Ids.map(a=>(a._1,(a._2))).reduceByKey(_+_).sortByKey()
val newnotsold = nPIdsA.leftOuterJoin(txPIdsA).filter(x=>x._2._2==None).collect

// COMMAND ----------

// MAGIC %md #### 4) Generate a report of products with totals sold. Sort output by product name alphabetically
// MAGIC 
// MAGIC Expected output: (productId, product name, product price, total sold)

// COMMAND ----------

val txPIds = txTuples.map(x=> (x._2,(x._3))).reduceByKey((acc, value) => acc + value)
val nPIds = P_Ids.map(x => (x._1,(x._2,x._3,x._4)))
val alltrx = txPIds.join(nPIds).map{case (pid,(t_qty,(pname,price,p_qty))) => (pid,pname,price,t_qty) }.sortBy(_._2).take(100)

// COMMAND ----------

// MAGIC %md #### 5) (Extra Credit) Generate statistics per customer's transactions.
// MAGIC 
// MAGIC ##### (calculate the average, minimum, maximum and total price of products bought per customer)
// MAGIC 
// MAGIC Expected output:(customerId, min, max, average, total)

// COMMAND ----------

val txTuplesPids = txTuples.map {case(cid,pid,qty,trx) => (pid,(cid,qty))}
val txCidprice = txTuplesPids.join(nPIds).map{case (pid,((cid,txqty),(pname, pprice,pqty))) => (cid,(pprice))}
val txCidMinprice = txCidprice.reduceByKey(math.min(_, _))
val txCidMaxpqty = txCidprice.reduceByKey(math.max(_, _))
val txCidTotal = txCidprice.reduceByKey(_+_)
val txCidAvg = txCidprice.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2)
val finaRDD = txCidMinprice.join(txCidMaxpqty).join(txCidMaxpqty).join(txCidTotal).join(txCidAvg).map{ case (n,((((a,b),c),d),e)) => (a,b,c,d,e) }
