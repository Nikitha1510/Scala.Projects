package edu.uta.cse6331

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Multiply {
  
  def inputmmatrix(line : String) = {
    val record = line.split(",");
    val i = record(0);
    val j = record(1);
    val v = record(2);
    (i,j,v)
  }
  
  def inputnmatrix(line : String) = {
    val record1 = line.split(",");
    val j = record1(0);
    val k = record1(1);
    val w = record1(2);
    (j,k,w)
  }
  
  
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Multiply");
    conf.setMaster("local[*]");
    val sc = new SparkContext(conf);
    
    val tempmmatrix = sc.textFile(args(0));
    val mmatrix = tempmmatrix.map(inputmmatrix);
    
    val tempnmatrix = sc.textFile(args(1));
    val nmatrix = tempnmatrix.map(inputnmatrix);
    
    val result1 = mmatrix.map(m => (m._2,m)).join(nmatrix.map(n => (n._1,n))).map {case (k,(m,n)) => ((m._1,n._2),(m._3.toDouble * n._3.toDouble))}   
    val result2 = result1.reduceByKey((val1,val2) => val1+val2).sortByKey(true,0);
    
    result2.foreach(println);
    
    
  }
  
  
}
