package edu.uta.cse6331

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Employee {
  
   def readEmpl(line : String) = {
     val input = line.split(",")
     val id = input(0)
     val name = input(1)
     val age = input(2)
     val gender = input(3)
     val dept = input(4)
     
     (id, name, age,if(gender=="M") { 
       "Male"
     }else {
       "Female"
     },dept)
     
     
   }
    def readDept(line : String) = {
      val input2 = line.split(",")
      val did = input2(0)
      val dname = input2(1)
      
      (did,dname)
      
    }
  
   def main(args : Array[String]) {
     
      val conf = new SparkConf().setAppName("Source");
      conf.setMaster("local[*]");
      val sc = new SparkContext(conf);
      val inputValues = sc.textFile(args(0))
      val inputDepartment = sc.textFile(args(1))
      val data = inputValues.map(readEmpl)
      val deptdata = inputDepartment.map(readDept)
      
      val output = data.map(tuple2 => {
        (tuple2._5,tuple2)
      }).join(deptdata.map(tuple3 =>{
        (tuple3._1,tuple3)
      })).map {
        case (key,(tuple2,tuple3)) =>
          (tuple2._1,tuple2._2,tuple2._3,tuple3._1,tuple3._2)
        }
        
      output.collect().sortBy(result => {
        result._5
      }).foreach(println)
     
   }
  
}