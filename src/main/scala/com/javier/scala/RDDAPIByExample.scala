package com.javier.scala
import org.apache.spark._
import org.apache.spark.rdd.RDD


object RDDAPIByExample {
    def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "aggregate", System.getenv("SPARK_HOME"))
    val z = sc.parallelize(List(1,2,3,4,5,6), 2)
    
    // lets first print out the contents of the RDD with partition labels
    def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
    
    val e = z.mapPartitionsWithIndex(myfunc).collect()
    e.foreach { x => print(x) }
    
}
}