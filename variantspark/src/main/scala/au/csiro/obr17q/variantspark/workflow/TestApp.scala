package au.csiro.obr17q.variantspark.workflow

object TestApp {
  def main(args:Array[String])  {
    println(Range(0,2).size)

    val s = Range(0,300).toList.toSet
    println( s.getClass)
    
    for (i <- 0 until 100000) {
      s.contains(i)
    }
    
    val result = List(1,2,3,4).toSeq.foldLeft(List[Int](0))((l,v) => (v + l.head) :: l ).reverse
    println(result)
  }
}