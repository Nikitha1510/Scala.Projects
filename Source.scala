package edu.uta.cse6331

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Source {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Source");
    conf.setMaster("local[*]");
    val sc = new SparkContext(conf);

    val ingraph = sc.textFile(args(0))
        .map(line => {
            val inputgr = line.split(",")
            (inputgr(0).toInt, inputgr(1).toInt, inputgr(2).toInt)
        })
        
    var grp = ingraph.groupBy(_._3)
    
    var dist = ingraph.map(ingraph => (ingraph._1, if(ingraph._1==0L){
      0L 
    }
    else{
      Long.MaxValue
    }, ingraph._2, ingraph._3
    ))
   
    var result = grp.map(tuple => {
      (tuple._1,
      if(tuple._1 == 0L){
        0L
      }
      else{
        Long.MaxValue
      })
    })
    
      for(i <- 1 to 4){
        
        dist = dist.map(tuple1 => {
          (tuple1._4,tuple1)
        }).join(result.map(tuple2 => {
          (tuple2._1,tuple2)
        })).map {
          case (key,(tuple1,tuple2)) => (tuple1._1,tuple2._2,tuple1._3,tuple1._4)
        }
        
        result = dist.map(tuple1 => {
          (tuple1._1,tuple1)
        }).join(result.map(tuple2 => {
          (tuple2._1,tuple2)
        })).map( {
        case (jkey,(tuple1, tuple2)) =>if((tuple2._2 != Long.MaxValue)&&(tuple1._2 > (tuple2._2 + tuple1._3)) )
                                                (tuple1._4, tuple2._2 + tuple1._3)
                                             else
                                                 (tuple1._4, tuple1._2)

      }).reduceByKey(_ min _)
        
       
    
  }
  
    result.filter(tuple => {
      tuple._2 != Long.MaxValue     
    }).collect().toSeq.sortBy(tuple => {tuple._1}).foreach(println);
    
}
}