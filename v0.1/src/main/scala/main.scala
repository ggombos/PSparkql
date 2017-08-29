import SparqlSpark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.io.Source 
import java.util.Calendar

object Main {
	class MyHashMap[A, B](initSize : Int) extends mutable.HashMap[A, B] {
	  override def initialSize: Int = initSize // 16 - by default
	}
	
	def show(x: Option[Long]) = x match {
		case Some(s) => s
		case None => 0L
	}
	
	def main(args: Array[String]) {
		println("TIME START "+Calendar.getInstance().getTime())
		var conf = new SparkConf().setAppName("PSpar(k)ql")
		val sc = new SparkContext(conf)

		var edgeArray = Array(Edge(0L,0L,"http://dummy/URI"))

		println("TIME READ VERTEX START "+Calendar.getInstance().getTime())

//--------------read version N3
//create vertex
		val tripleRDD = sc.textFile(args(0)).map(line => {
			val l = line.split("\\s+")
			(l(0), l(1), l(2))
		})
		
		//create vertexes
		val nodes = sc.union(tripleRDD.flatMap(x => List(x._1, x._3))).distinct		
		val vertexIds = nodes.zipWithUniqueId

		var NodesIdMap = vertexIds.collectAsMap()
		
		val vertexes = vertexIds.map(iri_id => {
			(iri_id._1, new SparqlSpark.RDFVertex(iri_id._2, iri_id._1))
		})
		
		
		//Subject vertexes with properties
		val vertexSRDD: RDD[(Long,RDFVertex)] = 
			tripleRDD.map(triple => (triple._1, (triple._2, triple._3)))
			.join(vertexes)
			.map(t => {				//(iri, ( (p, o), rdfvertex  ))
				val p_o = t._2._1
				val vertex = t._2._2
				if (p_o._2(0) == '"') {	//"
					//ide kell hogy add property
					vertex.addProps(p_o._1,p_o._2);
//					dataProps.add(p_o._1)
				//type property
				} else if (p_o._1 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
					vertex.addProps(p_o._1,p_o._2);
				//object property
				} else {

				}
				(vertex.id, vertex)
			}).distinct
			
		var dataProps = vertexSRDD.flatMap(x => {
			x._2.props.map(p => (p.prop))
		}).collect().toSet
		println("DATAPROPS")
		println(dataProps)
		println("---------")
		var objProps = tripleRDD.filter(triple => (!dataProps.contains(triple._2))).map(x => x._2).collect.toSet
			
			
			//object vertexes
		val vertexORDD: RDD[(Long,RDFVertex)] = 
			tripleRDD.map(triple => (triple._3, ()))
			.join(vertexes)
			.map(t => {				//(iri, ( (), rdfvertex  ))
				val vertex = t._2._2
				(vertex.id, vertex)
			}).distinct
		
		val vertexRDD: RDD[(Long,RDFVertex)] = vertexORDD.union(vertexSRDD).reduceByKey((a,b) => {
		  if (a.props.length > b.props.length) {
		    a
		  } else {
		    b
		  }
		})

		vertexRDD.persist(StorageLevel.MEMORY_AND_DISK)

		//ez itt kicommentelheto
		val edgeLoopRDD: RDD[Edge[String]] = vertexRDD.map(vertex => {
			Edge(vertex._1,vertex._1,"LOOP")
		})
		
		var edgeRDD: RDD[Edge[String]] = 
			tripleRDD.map(s_p_o => {
				if (s_p_o._3(0) != '"' && s_p_o._2 != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
					Edge(show(NodesIdMap.get(s_p_o._1)), show(NodesIdMap.get(s_p_o._3)), s_p_o._2)
				} else {
					Edge(0,0,"")
				}
			}).distinct.filter(e => e.srcId !=0)
		 edgeRDD.persist(StorageLevel.MEMORY_AND_DISK)
		
		//NodesIdMap = NodesIdMap.empty
		vertexes.unpersist()
		vertexIds.unpersist()
		nodes.unpersist()

//--------------read version N3

		val query = args(2)
		
		println("QUERY: "+query)
		println("TIME CREATE PLAN START "+Calendar.getInstance().getTime())
		val planRes = PSparqlPlan.createPlan(query,dataProps,objProps)
		val plan = planRes.plan
		val rootNode = planRes.rootNode
		val varNum:Int = planRes.numVar
		var maxIter = 0
		println(1)
		plan.foreach(f => {if (f.level>maxIter) maxIter = f.level})
		if (plan.size>0) {
			println(2)
			val allP = plan.map(triple => {triple.tp.p})		
			edgeRDD = edgeRDD.filter( edge => allP.contains(edge.attr))
			println(3)
		} else {
			println(4)
			edgeRDD = edgeRDD.filter(_ => false)		//[Edge[String]]
		}
		println("TIME EDGE UNION START "+Calendar.getInstance().getTime())
		edgeRDD = edgeRDD.union(edgeLoopRDD)
	
		println("TIME CREATE GRAPH START "+Calendar.getInstance().getTime())
		
		val graph = Graph(vertexRDD,edgeRDD)
		println("TIME CREATE GRAPHOPS START "+Calendar.getInstance().getTime())

		val head = mutable.LinkedHashMap[String,Int]()
		var rows = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
//TODO: KELL-E A LISTA, ELEG CSAK EGYEDI KULCS HOZZA, AMIT A MSG COMBINE MEGOLD!!!
		val initMsg:mutable.LinkedHashMap[String,RDFTable] = mutable.LinkedHashMap.empty[String,RDFTable]
		val emptyTable = new RDFTable(head,rows)

		def msgCombiner(
				a: mutable.LinkedHashMap[String,RDFTable], 
				b: mutable.LinkedHashMap[String,RDFTable]
				):mutable.LinkedHashMap[String,RDFTable] = {
			var c:mutable.LinkedHashMap[String,RDFTable] = mutable.LinkedHashMap.empty[String,RDFTable]
			b.foreach(i => {
				var newkey = i._1
				if (!i._1.contains("####")) 
					newkey = i._1 + "####" + ListMap(i._2.head.toSeq.sortBy(_._1):_*).keySet.mkString("")
				if (c.contains(newkey)){
					c(newkey) = c.getOrElseUpdate(newkey,emptyTable).merge(i._2) //.clone()
				} else {
					c(newkey) = i._2 //.clone()
				}
/*					if (c.contains(i._1)){
						c(i._1) = c.getOrElseUpdate(i._1,emptyTable).merge(i._2) //.clone()
					} else {
						c(i._1) = i._2 //.clone()
					}*/
			})
			a.foreach(i => {
				var newkey = i._1
				if (!i._1.contains("####")) 
					newkey = i._1 + "####" + ListMap(i._2.head.toSeq.sortBy(_._1):_*).keySet.mkString("")
				if (c.contains(newkey)){
					c(newkey) = c.getOrElseUpdate(newkey,emptyTable).merge(i._2) //.clone()
				} else {
					c(newkey) = i._2 //.clone()
				}
			})
			return c
		}

		//EdgeTriplet[x,y]   -- x nodetype, y edgetype
		def sendMsg(edge: EdgeTriplet[RDFVertex,String]): Iterator[(VertexId, mutable.LinkedHashMap[String,RDFTable])] = {
			var iteration = edge.dstAttr.getIter()
			if (edge.srcAttr.iter>edge.dstAttr.iter) { 
				iteration = edge.srcAttr.getIter()
			}
			var i:Iterator[(VertexId, mutable.LinkedHashMap[String,RDFTable])] = Iterator.empty
			
/*			if (edge.srcAttr.uri == "<http://www.Department0.University1000.edu/GraduateStudent103>" && edge.attr == "<http://spark.elte.hu#advisor>" ) println("SOURCE "+iteration)
			if (edge.dstAttr.uri == "<http://www.Department0.University1000.edu/GraduateStudent103>" && edge.attr == "<http://spark.elte.hu#advisor>" ) println("DEST")*/
			
			if (iteration <= maxIter) {
//				plan.filter(f=>f.level == iteration).foreach(x => println("ITERATION: "+iteration+"prop: "+x.tp))
				plan.filter(f=>f.level == iteration).foreach(triple => {
					var triplePattern = triple.tp
					var tablePattern = triple.headPattern
					if (edge.attr == triplePattern.p) {
						//SEND forward
						if (triple.src == triplePattern.s) {
							if (tablePattern.forall(a => edge.srcAttr.tableMap.contains(triplePattern.s) && edge.srcAttr.tableMap(triplePattern.s).head.contains(a)) && 
							edge.srcAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.s,new mutable.MutableList[VertexProp]())) &&
							edge.srcAttr.checkObjectProperty(triplePattern.s) && edge.dstAttr.checkObjectProperty(triplePattern.o)) {
								var m = edge.srcAttr.mergeEdgeToTable(
										triplePattern.o,triplePattern.s,
										triplePattern.s,triplePattern.o,edge.srcAttr.uri,edge.dstAttr.uri,iteration)
								i = i ++ Iterator((edge.dstAttr.id, m))
								//its ugly, but necessary to increment the iter number
								i = i ++ Iterator((edge.srcAttr.id, initMsg))
							}
						//SEND backward
						} else {
							if (tablePattern.forall(a => edge.dstAttr.tableMap.contains(triplePattern.o) && edge.dstAttr.tableMap(triplePattern.o).head.contains(a)) && 
							edge.dstAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.o,new mutable.MutableList[VertexProp]())) &&
							edge.dstAttr.checkObjectProperty(triplePattern.o) && edge.srcAttr.checkObjectProperty(triplePattern.s)) {
								var m = edge.dstAttr.mergeEdgeToTable(
										triplePattern.s,triplePattern.o,
										triplePattern.s,triplePattern.o,edge.srcAttr.uri,edge.dstAttr.uri,iteration)
								i = i ++ Iterator((edge.srcAttr.id, m))
								//its ugly, but necessary to increment the iter number
								i = i ++ Iterator((edge.dstAttr.id, initMsg))
							}
//							i = i ++ Iterator((edge.dstAttr.id, initMsg))
						}
					} else {
						//Iterator.empty
					}
				})
			}
			return i
		}

        def vertexProgram(id: VertexId, attr: RDFVertex, msgSum: mutable.LinkedHashMap[String,RDFTable]):RDFVertex = {
/*			if (attr.uri == "<http://www.Department0.University1000.edu/FullProfessor5>") {
				println("-->"+attr.iter)
				println("--------------")
				msgSum.foreach(x => x.foreach(m => {
					println("key: "+m._1)
					m._2.rows.distinct.map(row => {
							var t = "###"
								m._2.head.foreach(varName => {
									t+= row(m._2.head(varName._1))+" "
								})
							println(t)
						})
				}))
				println("============")
				attr.tableMap.foreach(x => {
					println("key: "+x._1)
					(x._2).rows.distinct.map(row => {
							var t = "###"
								x._2.head.foreach(varName => {
									t+= row(x._2.head(varName._1))+" "
								})
							println(t)
						})
				})
				println("attr.msgNum: "+attr.msgNum+" msgNum: "+msgSum.size )
			}*/
			attr.mergeMsgToTable(msgSum)
			attr.iter = attr.iter + 1
			attr.msgNum = attr.msgNum + msgSum.keySet.size
			return attr.clone()
		}

		var startTime = System.currentTimeMillis()
		println("TIME PREGEL START "+Calendar.getInstance().getTime())
		//TODO: MAXITER-t megadni!!!!
		//TODO: keresni olyan mutable eseteket, ahol nincs lementve a visszateresi ertek mert akkor sok felesleges objektumot hoz letre
		//pl.: head = mutable.ListBuffer[String]()
		// head = head += "valami"
		var result = Pregel(graph,initMsg,Int.MaxValue,EdgeDirection.Either)(vertexProgram,sendMsg,msgCombiner)
		var withResult = true;
		if (!withResult) {
			println("WITHOUT RESULT "+Calendar.getInstance().getTime());
			System.exit(1);
		}
		println("RES "+Calendar.getInstance().getTime());
		val rootNodeDataProp = planRes.dataProperties.getOrElse(rootNode,new mutable.MutableList[VertexProp]())

		if (plan.size == 0) {
			var res = result.vertices.filter(v => v._2.checkDataProperty(rootNodeDataProp))
			println("RESULT1: "+res.count());
		} else {
			println("All Nodes:"+result.vertices.count())
			println("plan size: "+varNum)
			println("root node: "+rootNode)
			var rootMsgNum = plan.filter(f => (f.tp.s == rootNode || f.tp.o == rootNode) && f.tp.p != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" && !planRes.dataProperties.contains(f.tp.p)).size
			println("limit:"+rootMsgNum)
			var res2 = result.vertices.filter(v => (v._2.tableMap.contains(rootNode) 
				&& (v._2.tableMap(rootNode).head.size == varNum)
				&& (v._2.msgNum == rootMsgNum)
				&& (v._2.tableMap(rootNode).rows.size > 0)
					))
//			res2.collect().foreach(v => println(v._2.msgNum+" <-> "+plan.size))
//			var tmpres = res2.filter(v => (v._2.uri == "<http://www.Department0.University1000.edu/GraduateStudent103>"))
//			tmpres.collect().foreach(v => println(v._2.tableMap(rootNode).head))
			println("Nodes with rootNode: "+res2.count())
			
			res2 = res2.filter(v => (
					v._2.checkDataProperty(rootNodeDataProp)
					))
			println("Nodes after rootNode Dataprop check: "+res2.count())
			if (res2.count() > 0) {
				var rowNum = 0;
				res2.collect().foreach(v => {
					//TODO: ez a distinct nem itt kellene!!!!!
					rowNum = rowNum + v._2.tableMap(rootNode).rows.distinct.size
				})
				println("Final results row: "+rowNum)
				println("RESULT: "+rowNum)
				
				//printResult
				if (false) {
					var helpHead = res2.first()._2.tableMap(rootNode).head.clone()

					helpHead.foreach(h => print(h._1+" "))
					println("")
					println("=============")
					res2.collect().foreach(v => {
						v._2.tableMap(rootNode).rows.distinct.map(row => {
							var t = "###"
								helpHead.foreach(varName => {
									t+= row(v._2.tableMap(rootNode).head(varName._1))+" "
								})
							println(t)
						})
					})

					println("-------------")
				}
			} else {
				println("RESULT3: 0")
			}
		}
		var stopTime = System.currentTimeMillis();
		println("TIME STOP "+Calendar.getInstance().getTime())
		println("Elapsed Time: "+(stopTime-startTime))

	}
}
