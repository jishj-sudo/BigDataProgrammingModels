case class Neumaier(sum: Double, c: Double)

object HW {

   def q1_countsorted(x: Int, y: Int, z:Int): Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val xlty: Int = {if (x<y) {1} else {0}}
      val yltz: Int = {if (y<z) {1} else {0}}
      val xltz: Int = {if (x<z) {1} else {0}}
      xlty+yltz+xltz
      
      
   }

   def q2_interpolation(name: String, age: Int): String = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
     //""
      val informal: String = "howdy"
      val formal: String = "hello"
      val lowerName: String = name.toLowerCase
      val greeting: String = if (age <21) {"${informal}, ${lowerName}"} else {"${formal}, ${lowerName}"}
      greeting
   }

   def q3_polynomial(arr: Seq[Double]): Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      arr.foldLeft((0.toDouble,1.toDouble))({(x,y) => (x._1+x._2*y,x._2+1)})._1.toInt
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int):Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      1
   }
   def q5_stringy(start: Int, n: Int): Vector[String] = {
     Vector("1","2","3")
   }  
   def q6_modab(a: Int, b: Int, c: Vector[Int]): Vector[Int] = {
     Vector(1,2,3)
   }
   def q7_count(arr: Vector[Int])(f: (Int) => (Boolean)): Int = {
     1
  }
  /* @annotation.tailrec */
   def q8_count_tail(arr: Vector[Int])(f: (Int) => (Boolean)): Int = {
     2
  }
  def q9_neumaier(input: Seq[Double]): Int = {
    3
  }
   // create the rest of the functions yourself
   // in order for the code to compile, you need to (at the very least) create
   // blank versions of the remaining functions and have them return a value of 
   // the expected type, like the blank functions above.
   // remember, to compile, you don't specify any file names, you just use sbt compile
}
