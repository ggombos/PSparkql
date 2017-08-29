import SparqlSpark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.io.Source 
import bleibinhaus.hdfsscalaexample._

object NTripleGraphLoader {
	class MyHashMap[A, B](initSize : Int) extends mutable.HashMap[A, B] {
	  override def initialSize: Int = initSize // 16 - by default
	}
	
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Spar(k)ql GraphLoader")
		val sc = new SparkContext(conf)

		var edgeArray = Array("")
		var vertexIds = new MyHashMap[String, RDFVertex](400000)
		var id = args(3).toLong  //1L

		var rownum = 0;
		val path = new java.io.File(args(0)).getCanonicalPath
		val vertexPath = new java.io.File(args(1)).getCanonicalPath
		val edgePath = new java.io.File(args(2)).getCanonicalPath

        for(line <- Source.fromInputStream(HDFSFileService.getFile(args(0))).getLines()) {
			rownum = rownum+1
			val triple = line.split("\\s+")
			val sVertex = vertexIds.getOrElseUpdate(triple(0), new SparqlSpark.RDFVertex(id,triple(0)))
			//ha id == sVertex.id id++
			if (sVertex.id == id) id=id+1
		
			//data property
			if (triple(2)(0) == '"') {
				vertexIds(triple(0)).addProps(triple(1),triple(2));
			//type property
			} else if (triple(1) == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
				vertexIds(triple(0)).addProps(triple(1),triple(2));
			//object property
			} else {
				val oVertex = vertexIds.getOrElseUpdate(triple(2), new SparqlSpark.RDFVertex(id,triple(2)))
				if (oVertex.id == id) id=id+1
				edgeArray = edgeArray :+ sVertex.id+" "+oVertex.id+" "+triple(1)
			}
		}

		//print vertex
		vertexIds.toArray.map(s => {
			var str = s._2.id+" "+s._2.uri
			s._2.props.foreach(prop => str += " "+prop.prop+"##PropObj##"+prop.obj)
			str
		})
		
		val vertexRDD: RDD[RDFVertex] = sc.parallelize(vertexIds.toArray.map(s => s._2))

		val edgeRDD: RDD[String] = sc.parallelize(edgeArray.slice(1,edgeArray.length))
		
		vertexRDD.coalesce(1,true).saveAsTextFile(vertexPath)
		edgeRDD.coalesce(1,true).saveAsTextFile(edgePath)
		
		println("MINIDFORNEXTLUBM "+id)
	}
}