
class Student(val name: String, val ID: Int, val age: Double) {
   var n: String = name
   var num: Int = ID
   var a: Double = age
   
}

object Example {
   def main(args: Array[String]) {
      val student = new Student("Nikitha", 10056, 23.0)
      printStudent

      def printStudent{
         println ("Name of the student : " + student.n);
         println ("ID Number of the student: " + student.num);
         println ("Age of the student: " + student.a);
      }
   }
}