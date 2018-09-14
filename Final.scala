package finalproject
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import org.apache.spark.sql.SQLContext
import org.dmg.pmml.True


object Final {
  
  def inputtable(line : String) = {
      
    val datatable = line.split("	");
    val case_status = datatable(1);
    val employer_name = datatable(2);
    val occupational_name = datatable(3);
    val job_title = datatable(4);
    val fulltime_pos = datatable(5);
    val wage = datatable(6);
    val year = datatable(7);
    val worksite = datatable(8);
    val latitude = datatable(9);
    val longitude = datatable(10);
    (case_status, employer_name, occupational_name, job_title, fulltime_pos, wage, year, worksite, latitude, longitude)
  }
  
  def main(args : Array[String]){
    
   val conf = new SparkConf().setAppName("H1-B Petetions Analysis")
   conf.setMaster("local[*]")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)
    
    val e_temp = sc.textFile(args(0));
        
   //Query 1
    val inputdata = e_temp.map(inputtable)
    val result = inputdata.groupBy(_._7)
    val employer = inputdata.map(inputdata => ((inputdata._7, inputdata._2), (1)))
                  .reduceByKey((x,y) => (x + y))
                  
    val sortedResult = employer.sortByKey(true, 0)
    //sortedResult.foreach(println)
    sortedResult.saveAsTextFile(args(1))
  
   
    //Query 2
    val jobs = inputdata.map(locationtuple => (locationtuple._8,locationtuple._4))
                        .filter(tuple => tuple._2=="PRODUCT MANAGER") 
                        .groupByKey()
    val locationcount = jobs.map(tuple =>{
                          val (location , positionList) = tuple
                          (location , positionList.size)
                        })
    locationcount.collect().sortWith((X,Y) => X._2 > Y._2)
    locationcount.saveAsTextFile(args(2))
                    
  }
 
}