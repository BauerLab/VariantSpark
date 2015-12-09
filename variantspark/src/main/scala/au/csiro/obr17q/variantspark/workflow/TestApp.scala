package au.csiro.obr17q.variantspark.workflow

object TestApp {
  def main(args:Array[String])  {
    println(Range(0,2).size)

    val result = List(1,2,3,4).toSeq.foldLeft(List[Int](0))((l,v) => (v + l.head) :: l ).reverse
    println(result)
  }
}