import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.fs._
import org.apache.spark.storage.StorageLevel

import java.io.FileNotFoundException

object SparkBigData {

  var ss : SparkSession = null
  var spConf : SparkConf = null

  private var trace_log : Logger = LogManager.getLogger("Logger_Console")

  val schema_order = StructType(
    Array(
      StructField("orderid", IntegerType, false),
      StructField("customerid", IntegerType, false),
      StructField("campaignid", IntegerType, true),
      StructField("orderdate", TimestampType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipcode", StringType, true),
      StructField("paymenttype", StringType, true),
      StructField("totalprice", DoubleType, true),
      StructField("numorderlines", IntegerType, true),
      StructField("numunit", IntegerType, true)
    )
  )

  def main(args: Array[String]): Unit = {

    val session_s = Session_Spark(true)

    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header","true")
      .csv("file:///Users/Raphael/Projets/Scala-Formation/DataFrame/2010-12-06.csv")

    val df_gp = session_s.read
      .format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("DataFrame/")

    val df_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("DataFrame/2010-12-06.csv", "DataFrame/2011-12-08.csv", "DataFrame/2011-01-20.csv")

    // df_gp2.show(7)

    //df_gp.show(7)
    //println("df_test count : " + df_test.count() + " df_group count : " + df_gp.count())

    // df_test.printSchema()

    val df_2 = df_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("ID du client"),
      col("StockCode").cast(IntegerType).alias("Code de la marchandise"),
      col("Invoice".concat("_c0")).alias("ID de la commande"),
    )

    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("valeur_constante", lit(50))
      .withColumnRenamed("_c0", "ID_client")
      .withColumn("ID_commande", concat_ws("|",col("InvoiceNo"), col("ID_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("Created_dt", current_timestamp())
      .withColumn("reduction_test", when(col("total_amount") > 15, lit(3)).otherwise(lit(0)))
      .withColumn("reduction", when(col("total_amount") < 15, lit(0))
        .otherwise(when(col("total_amount").between(15, 20), lit(3))
          .otherwise(when(col("total_amount") > 15, lit(4)))))
      .withColumn("net_income", col("total_amount") - col("reduction"))

    val df_notreduced = df_3.filter(col("reduction") === lit(0) && col("Country").isin("United Kingdom", "France", "USA"))

    // df_notreduced.show(5)

    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("DataFrame/orders.txt")

    val df_ordersGood = df_orders.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")

    val df_product = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("DataFrame\\product.txt")

    val df_orderlines = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("DataFrame\\orderline.txt")

    // df_orders.show(10)
    // df_orders.printSchema()

    val df_joinorders = df_orderlines.join(df_ordersGood, df_ordersGood.col("orderid") === df_orderlines.col("orderid"), Inner.sql)
      .join(df_product, df_product.col("productid") === df_orderlines.col("productid"), Inner.sql)

    // df_joinorders.printSchema()

    val df_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("DataFrame/2010-12-06.csv")

    val df_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("DataFrame/2011-01-20.csv")

    val df_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("DataFrame/2011-12-08.csv")

    val df_unitedfiles = df_fichier1.union(df_fichier2.union(df_fichier3))
    // println(df_fichier3.count() + "" + df_unitedfiles.count())

    df_joinorders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("state","city")
      .sum("total_amount").as("Commande totales")

    val wn_specs = org.apache.spark.sql.expressions.Window.partitionBy(col("state"))
    val df_windows = df_joinorders.withColumn("ventes_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(wn_specs))
      .select(
        col("orderlineid"),
        col("zipcode"),
        col("PRODUCTGROUPNAME"),
        col("state"),
        col("ventes_dep").alias("Ventes_par_departement")
      )

    df_windows.write
      .mode(SaveMode.Overwrite)
      .csv("DataFrame//Ecriture")

    // Manipulation des dates et du temps en Spark
    df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MM/yyyy")) // date_format() permet de changer la disposition des dates
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_secondes", window(col("orderdate"), "5 secondes"))
      .select(
        col("orderdate"),
        col("periode_secondes"),
        col("periode_secondes.start"),
        col("periode_secondes.end")
      )

    //df_ordersGood.show(10)


    df_unitedfiles.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
      .withColumn("Invoice_add_2months", add_months(col("InvoiceTimestamp"), 2))
      .withColumn("Invoice_add_date", date_add(col("InvoiceDate"), 30))
      .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 25))
      .withColumn("Invoice_date_diff", datediff(current_date(), col("InvoiceDate")))
      .withColumn("InvoiceDateQuarter", quarter(col("InvoiceDate")))
      .withColumn("InvoiceDate_id", unix_timestamp(col("InvoiceDate")))
      .withColumn("InvoiceDate_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd-MM-yyyy"))
      // .show(10)

    df_product
      .withColumn("productGP", substring(col("PRODUCTGROUPNAME"),2,2))
      .withColumn("productln", length(col("PRODUCTGROUPNAME")))
      .withColumn("concat_product", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
      .show(10)

    // Permet de donner les dates tous les 5 jours. Peut être fait en secondes, minutes, jour...
    df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MM/yyyy")) // date_format() permet de changer la disposition des dates
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_jour", window(col("orderdate"), "5 days"))
      .select(
        col("periode_jour"),
        col("periode_jour.start"),
        col("periode_jour.end")
      )
      // .show(10)

    /*
    df_windows
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("DataFrame\\Ecriture")

    df_2.write
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.ending.direct", "name")
      .orc("users_with_options.orc")
    */



  }


  def spark_hdfs () : Unit = {

    val config_fs = Session_Spark(true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_path = new Path("DataFrame/marketing/")
    val dest_path = new Path("DataFrame/indexes")
    val ren_src = new Path("DataFrame/marketing/fichier_reporting.parquet")
    val dest_src = new Path("DataFrame/marketing/reporting.parquet")
    val local_path = new Path("DataFrame/Ecriture/") // ajouter le nom du fichier
    val path_local = new Path("DataFrame")

    // Lecture des fichiers d'un dossier
    val files_list = fs.listStatus(src_path)
    files_list.foreach(f => println(f.getPath))

    val files_list1 = fs.listStatus(src_path).map(x => x.getPath)
    for( i <- 1 to files_list1.length) {
      println(files_list1(i))
    }

    // Renommer des fichiers
    fs.rename(ren_src, dest_src)

    // Supprimer des fichiers dans un dossier
    fs.delete(dest_src, true)

    // Copie des fichiers
    fs.copyFromLocalFile(local_path, dest_path)
    fs.copyToLocalFile(dest_path, path_local)
  }



  def manip_rdd() : Unit = {

    val session_s = Session_Spark(true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")

    val rdd_test : RDD[String] = sc.parallelize(List("Alain", "Juvenal", "Julien", "Anna"))
    rdd_test.foreach{ l => println(l)}
    println()

    val rdd2 : RDD[String] = sc.parallelize(Array("Lucie", "Fabien", "Jules"))
    rdd2.foreach{l => println(l)}
    println()

    val rdd3 = sc.parallelize(Seq(("Julien", "Math", 16), ("Aline", "Math", 13), ("Raphaël", "Math", 15)))
    println("Premier élément de mon RDD 3")
    rdd3.take(1).foreach{l => println(l)}
    println()

    if(rdd3.isEmpty()) {
      println("Le RDD est vide")
    } else {
      println("Voici le contenu du RDD3")
      rdd3.foreach{l => println(l)}
    }

    // Sauvegarder les données dans un dossier, #PATH permet de choisir le chemin de l'installation sur la machine
    rdd3.saveAsTextFile("#PATH")

    // Sauvegarder une repartition ( en l'occurence la n°1 ici ) dans un dossier. Avec le path
    rdd3.repartition(1).saveAsTextFile("#PATH")

    // création d'un RDD à partir d"une source de données - Permet de lire un fichier .txt dans un dossier
    // val rdd4 = sc.textFile("#PATH\\fichier.txt")
    // println("lecture du contenu du RDD4")
    // rdd4.foreach{l => println(l)}

    // Permet de lire tous les fichiers .txt du dossier
    val rdd5 = sc.textFile("#PATH\\*")
    println("lecture du contenu du RDD5")
    rdd5.foreach { l => println(l) }

    val rdd_trans : RDD[String] = sc.parallelize(List("Alain mange une banane", "La banane est un bon aliment pour la santé", "Acheter une bonne banane"))
    // rdd_trans.foreach(l => println("Ligne de mon RDD : " + l))

    val rdd_map = rdd_trans.map(x => x.split(" "))
    // println("Nbr d'éléments de mon RDD map : " + rdd_map.count())

    val rdd6 = rdd_trans.map(w => (w, w.length, w.contains("banane")))
    // rdd6.foreach(l => println(l))

    val rdd7 = rdd6.map(x => (x._1.toUpperCase(), x._2, x._3))
    // rdd7.foreach(l => println(l))

    val rdd8 = rdd6.map(x => (x._1.split(" "), 1))
    // rdd8.foreach(l => println(l._1(0), l._2))

    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(w => (w, 1))
    //rdd_fm.foreach(l => println(l))

    // val rdd_compte = rdd5.flatMap(x => x.split(",")).map(m => (m, 1)).reduceByKey((x, y) => x + y)
    // rdd_compte.repartition(1).saveAsTextFile("#PATH\\Comptage.txt")
    // rdd_compte.foreach(l => println(l))

    val rdd_filtered = rdd_fm.filter(x => x._1.contains("banane"))
    // rdd_filtered.foreach(l => println(l))

    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y)
    // rdd_reduced.foreach(l => println(l))

    rdd_fm.cache()
    // rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    // rdd_fm.unpersist()

    import session_s.implicits._
    val df : DataFrame = rdd_fm.toDF("texte", "valeur")
    // df.show(50)


  }

  /**
   * Fonction qui initialise et instancie une session spark
   * @param Env : c'est une variable qui indique l'environnement sur lequel notre application est déployée
   *            Si Env = true, alors l'appli est déployée en local, sinon elle est déployée sur un cluster
   */
  def Session_Spark(Env : Boolean = true) : SparkSession = {
    try {
      if(Env == true) {

        System.setProperty("hadoop.home.dir" , "file:///Users/Raphael/hadoop")
        ss = SparkSession.builder()
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", true)
          .getOrCreate()

      } else {

        ss = SparkSession.builder()
          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()

      }
    } catch {
      case ex : FileNotFoundException => trace_log.error("Nouc n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex : Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
    }
    ss
  }

}

