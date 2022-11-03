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
      val lowerName: String = name.toLowerCase
      val greeting: String = if (age <21) {s"howdy, $lowerName"} else {s"hello, $lowerName"}
      greeting
   }

   def q3_polynomial(arr: Seq[Double]): Double = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      arr.foldLeft((0.toDouble,1.toDouble))({(x,y) => (x._1 +x._2 * y, x._2 + 1)})._1
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int):Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      f(f(x,y),z)
   }
   def q5_stringy(start: Int, n: Int): Vector[String] = {
     val vec = Vector.range(start,start+n)
     vec.map(_.toString)
   }  
   def q6_modab(a: Int, b: Int, c: Vector[Int]): Vector[Int] = {
     c.filter(x => x >= a && x % b != 0)
   }
   def q7_count(arr: Vector[Int])(f: (Int) => (Boolean)): Int = {
    if (arr.isEmpty) {0} 
    
    else {if (f(arr.head)) {1+q7_count(arr.tail)(f)} else {q7_count(arr.tail)(f)}} 
   }
   @annotation.tailrec 
   def q8_count_tail(arr: Vector[Int],count: Int = 0)(f: (Int) => (Boolean)): Int = {
    
    if (arr.isEmpty) {count} 
    
    else {if (f(arr.head)) {q8_count_tail(arr.tail,1+count)(f)} else {q8_count_tail(arr.tail,count)(f)}} 
  }
  def q9_neumaier(input: Seq[Double]): Double = {
    val initialState = Neumaier(0,0)
    val finalState =input.foldLeft(initialState)((s,i) => if (s.sum.abs >= i.abs) 
                                        {Neumaier(s.sum + i, s.c + ((s.sum - (s.sum + i)) + i))}
                                   else {Neumaier(s.sum + i, s.c + ((i     - (s.sum + i)) + s.sum))})
    finalState.sum + finalState.c
  }
   // create the rest of the functions yourself
   // in order for the code to compile, you need to (at the very least) create
   // blank versions of the remaining functions and have them return a value of 
   // the expected type, like the blank functions above.
   // remember, to compile, you don't specify any file names, you just use sbt compile
}
