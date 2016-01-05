package com.javier.scala
import org.apache.spark._
import org.apache.spark.rdd.RDD


object RDDapiByExample2 {
    def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val a = new Aggregate(master).aggregate()
    val b = new Aggregate2(master).aggregate()
}
}

//myfunc debe ser una funcion/variable para poder ser serializable.
//Si myfunc se encuentra en el mismo objeto main da igual que este definido como metodo
//se va a comportar como una funcion y por tanto no hay problema como es el caso de 
//RDDAPIByExample.scala. Para el caso de RDDapiByExample2.scala class Aggregate necesitaremos poner
//myfunc como variable inmutable. No es más que especificar con un val y quitar el tipo
//de datos a retornar.

//Otra opción es especificar que el objeto entero es serializable con extends java.io.Serializable
// con esto conseguimos que cuando los worker nodes requieran usar el metodo myfunc puedan hacerlo gracias
// a que de manera serializada pueden acceder al metodo de la clase que los contiene. Esto de la serializacion es
// necesario puesto que es en el main donde se crea el objeto (val b = new Aggregate2(master).aggregate())
// el propietario del SparkContext va a ser el driver y por tanto el main ( ya que es quien lo crea ...)
// Es por ello que de alguna manera hay que serializar esto para que no solo sea el maestro el que tenga acceso
// a este metodo. 

class Aggregate(execution:String) {
  def aggregate(){
        val sc = new SparkContext(execution, "aggregate", System.getenv("SPARK_HOME"))
        val z = sc.parallelize(List(1,2,3,4),2)
        val e = z.mapPartitionsWithIndex(myfunc).collect()
        e.foreach { x => print(x) }
        sc.stop() 
        }
   val myfunc= (index: Int, iter: Iterator[(Int)])  => {
         iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
   
  }

class Aggregate2(execution:String) extends java.io.Serializable{
  def aggregate(){
        val sc = new SparkContext(execution, "aggregate", System.getenv("SPARK_HOME"))
        val z = sc.parallelize(List(1,2,3,4),2)
        val e = z.mapPartitionsWithIndex(myfunc).collect()
        e.foreach { x => print(x) }
        }
    def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
         iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
   
  }