package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source



object SampleProgram {

  def majuscule(s: String, filtre: String): String = {
    if(s.contains(filtre)) s
    else s.toUpperCase
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    /*exo 1*/
    println("Exo 1")
    /*Question 1*/
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")
    /*Question 2*/
    println("Question 2")
    val di_caprio= rdd.filter(elem => elem.contains("Di Caprio"))
    val count_di_caprio = di_caprio.count()
    println(count_di_caprio)
    /*Question 3*/
    println("Question 3")
    val notes = di_caprio.map(item => (item.split(";")(2).toDouble))
    val moyenne = notes.sum() / notes.count()
    println(moyenne)
    /*Question 4*/
    println("Question 4")
    val vue = di_caprio.map(item => (item.split(";")(1).toDouble))
    val total_vue = rdd.map(item => (item.split(";")(1).toDouble))
    val pourcentage_dicaprio = (vue.sum()/ total_vue.sum())*100
    println(pourcentage_dicaprio)
    /*Question 5*/
    println("Question 5")


    /*exo 2*/
    println("Exo 2")
    /*Question 1*/
    val df: DataFrame = sparkSession.read.option("delimiter", ";").option("inferSchema", true).option("header", false).csv("data/donnees.csv")
    import org.apache.spark.sql.functions._
    /*Question 2*/
    println("Question 2")
    val df_rename = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")
    df_rename.show
    /*Question 3*/
    println("Question 3")
    //2
    val count_di_caprio_df = df_rename.filter(col("acteur_principal") === "Di Caprio").count()
    println(count_di_caprio_df)
    //3
    val mean_di_caprio = df_rename.groupBy("acteur_principal")
      .mean("note_film")
      .filter(col("acteur_principal") === "Di Caprio")
      .select("avg(note_film)")
      .first.get(0).toString.toDouble
    println(mean_di_caprio)
    //4
    val vue_dicaprio = df_rename.groupBy("acteur_principal").sum("nombre_vues")
      .filter(col("acteur_principal") === "Di Caprio")
      .select("sum(nombre_vues)").first.get(0).toString.toDouble
    val vue_total = df_rename.agg(sum("nombre_vues").alias("total")).first.get(0).toString.toDouble
    val pourcentage_vue_dicaprio = (vue_dicaprio / vue_total)* 100
    println(pourcentage_vue_dicaprio)
    //5

    /*Question 4*/
    println("Question 4")
    val df_new = df_rename.withColumn("pourcentage_vue", (col("nombre_vues")/vue_total)*100)
    df_new.show


  }
}