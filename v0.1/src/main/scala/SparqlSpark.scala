import _root_.org.apache.spark.graphx._
import scala.collection.mutable

object SparqlSpark {
	class Msg(val vari:String, val tab:RDFTable) extends java.io.Serializable {
		var table: RDFTable = tab
		var variable: String = vari
	}

	class VertexProp(val p: String, val o: String) extends java.io.Serializable {
		val prop: String = p
		val obj: String = o
	}
	class RDFTable(val _head:mutable.LinkedHashMap[String, Int],val _rows:mutable.ArrayBuffer[mutable.ListBuffer[String]]) extends java.io.Serializable {
		var head:mutable.LinkedHashMap[String, Int] = _head
		var iteration:mutable.Set[Int] = mutable.Set.empty[Int]
		var rows:mutable.ArrayBuffer[mutable.ListBuffer[String]] = _rows
		//TODO: EZ ITT VAN HASZNALVA???????
//+		var iter:Int = -1
		
		override def clone():RDFTable= {
			return this
/*			var newhead:mutable.LinkedHashMap[String,Int] = this.head.clone()
			var newrow:mutable.ArrayBuffer[mutable.ListBuffer[String]] = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
			this.rows.foreach(row => newrow += row.clone())
			var x:RDFTable = new RDFTable(newhead,newrow)
			x.iter = this.iter
			x.iteration = this.iteration.clone()
			return x*/
		}
		def checkUnion(table2:RDFTable):Boolean= {
			var s = ((head.keySet -- table2.head.keySet) ++ (table2.head.keySet -- head.keySet)).size
			return (s==0)
		}

		def merge(table2:RDFTable):RDFTable= {
			//var tmp = "merge: oldrow: "+rows+"  newrow: "+table2.rows
			var union = checkUnion(table2)
			if (head.size == 0) {
				head = table2.head
				rows = table2.rows
			}

			val table1:RDFTable = this
			var newhead:mutable.LinkedHashMap[String,Int] = table1.head.clone()		//ez a clone kell!
			var newrows:mutable.ArrayBuffer[mutable.ListBuffer[String]] = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
				
			if (union) {
				newrows = table1.rows		//.clone()
				table2.rows.map(r2 => {
					var newrow:mutable.ListBuffer[String] = mutable.ListBuffer[String]()
					table1.head.foreach( h => {
                                		newrow.append(r2(table2.head(h._1)))
                                	})
					newrows.append(newrow)
				})
			} else {
				newrows = table1.rows.flatMap(r => table2.rows.map(r2 => {
					var l = true
					table2.head.foreach( h2 => {
							if (table1.head.contains(h2._1) && (r(table1.head.getOrElseUpdate(h2._1,-1)) != r2(h2._2))) {
								l = false
							}
						}
					)
					if (l) {
						var newrow:mutable.ListBuffer[String] = r.clone()		//ez a clone kell
						table2.head.foreach( h2 => {
							if (!table1.head.contains(h2._1)) {
								newrow.append(r2(h2._2))
								if(!newhead.contains(h2._1)){
                                    newhead(h2._1) = newhead.size
                                }
							}
						})
						newrow
					} else {
						mutable.ListBuffer[String]()
					}
				}
				))
			}
			newrows = newrows.filter(r => (r.length>0))
			table1.rows = newrows
			table1.head = newhead
			
			//println(tmp)
			
			/*var m = "table1 iter: ("
			table1.iteration.foreach(x => m = m+x)
			m = m + ") table2 iter: ("
			table2.iteration.foreach(x => m = m+x)
			println(m+") ")
			*/
			table1.iteration = table1.iteration.union(table2.iteration)
			return table1	//.clone()
		}
	}
	class RDFVertex(val Vid: org.apache.spark.graphx.VertexId, val u: String) extends java.io.Serializable {
		val id: org.apache.spark.graphx.VertexId = Vid
		var uri: String = u.trim
		var iter:Int = -1
		var msgNum:Int = 0
		var props:Array[VertexProp] = Array[VertexProp]()
		//var table:RDFTable = new RDFTable(mutable.LinkedHashMap.empty[String,Int], mutable.ArrayBuffer[mutable.ListBuffer[String]]())
		var tableMap:mutable.LinkedHashMap[String,RDFTable] = mutable.LinkedHashMap.empty[String,RDFTable]
		//var query:RDFTable = new RDFTable(mutable.LinkedHashMap.empty[String,Int], mutable.ArrayBuffer[mutable.ListBuffer[String]]())
		//var finish:Boolean = false
		
		override def toString():String = {
			var str:String = id+" "+uri
			props.foreach(prop => str = str+" "+prop.prop+"##PropObj##"+prop.obj)
			return str
		}

		override def clone():RDFVertex = {
//			return this;
			var x = new RDFVertex(id,uri)
			x.iter = this.iter
			x.msgNum = this.msgNum
			tableMap.foreach(v => x.tableMap(v._1)=v._2.clone())
			x.props = props.clone()
			return x
		}
		
		def checkDataProperty(queryProp: mutable.MutableList[VertexProp]):Boolean= {
			//var s:String = uri+" dataprop: "+queryProp.clone().size+" "+props.size
			var b:Boolean = queryProp.forall(qp => {
				if (qp.obj(0) == '?') { //"	
					props.exists( p => {
						(qp.prop == p.prop )
					})
				} else {
					props.exists( p => {
						(qp.prop == p.prop && qp.obj == p.obj)
					})
				}
			})
//			println(s)
			return b
		}

		def checkObjectProperty(_uriVar: String):Boolean= {
			if (_uriVar(0) != '?' && _uriVar != uri) {
				return false
			}
			return true
		}
		
		def addProps(p: String,o: String) {
			if (!props.exists( pro => {pro.prop == p && pro.obj == o})) {
				props = props :+ new VertexProp(p,o)
			}
		}
		def haveMsg():Boolean ={
			false
		}
		def mergeMsgToTable(msg:mutable.LinkedHashMap[String,RDFTable]) {
			//TODO: ez a foreach csak egyszer fut le, mert csak egy változót értékelünk ki egyszerre
			msg.foreach(m => {
				var key = m._1
				if (m._1.contains("####")) key = m._1.split("####")(0)
				if (tableMap.contains(key)) {
					//DEBUG
/*					if (uri == "<http://www.Department0.University1000.edu/GraduateStudent103>") {
						println("Before MERGE: "+m._1)
						println("-----------")
						tableMap(m._1).rows.distinct.map(row => {
							var t = "###"
								tableMap(m._1).head.foreach(varName => {
									t+= row(tableMap(m._1).head(varName._1))+" "
								})
							println(t)
						})
						println("-----------")
						(m._2).rows.distinct.map(row => {
							var t = "###"
								(m._2).head.foreach(varName => {
									t+= row((m._2).head(varName._1))+" "
								})
							println(t)
						})
						println("-----------")
					}*/
					
					tableMap(key) = tableMap(key).merge(m._2)
				} else {
					tableMap(key) = m._2  //.clone()
				}
//				println("MERGE MSG TABLENODE: "+m._1)
			})
			if (!msg.isEmpty) tableMap.filter(m => {
				var key = m._1
				if (m._1.contains("####")) key = m._1.split("####")(0)
				msg.contains(key)
			})
			
/*					if (uri == "<http://www.Department0.University1000.edu/GraduateStudent103>") {
						println("After MERGE: ")
						tableMap.foreach(m=> {
							println("key: "+m._1)
							(m._2).rows.distinct.map(row => {
							var t = "###"
								(m._2).head.foreach(varName => {
									t+= row((m._2).head(varName._1))+" "
								})
							println(t)
						})})
					}*/
		}
	
		def getIter():Int= {return iter}
	
		def mergeEdgeToTable(vertexVar:String,mergeVar:String,var1:String,var2:String,s:String,o:String, iteration:Int):mutable.LinkedHashMap[String,RDFTable]= {
			//println("edgeMergeTo: "+uri+" "+s+" "+o)
			var t = new RDFTable(
				mutable.LinkedHashMap[String,Int](var1->0,var2->1),
				mutable.ArrayBuffer[mutable.ListBuffer[String]](
					mutable.ListBuffer[String](s,o)
				)
			)
			//TODO: ITT TISZTAZNI A SOK ITER VS ITERATION-T
			//iter = melyik iteracioban van, iteration = mely iteraciokban vett reszt az eredmeny, az osszes iteracioval rendelkezo jo nekunk
//+			t.iter = iter
//			if (uri == "<http://www.Department0.University1000.edu/GraduateStudent103>") 
/*			if (tableMap.contains(mergeVar)) {
				println(uri+" "+mergeVar+" Before:" + tableMap(mergeVar).rows.size)
				println("tsize: "+t.rows.size)
				tableMap(mergeVar).rows.distinct.map(row => {
							var t = "###"
								tableMap(mergeVar).head.foreach(varName => {
									t+= row(tableMap(mergeVar).head(varName._1))+" "
								})
							println(t)
						})
			}*/

			t.iteration.add(iteration)
			if (tableMap.contains(mergeVar)) {
				if (tableMap(mergeVar).head.size>0) 
					t = t.merge(tableMap(mergeVar))
			} else {
				//nop
			}
//			if (uri == "<http://www.Department0.University1000.edu/GraduateStudent103>") 
/*			if (tableMap.contains(mergeVar)) {
				println(uri+" "+mergeVar+" After:" + tableMap(mergeVar).rows.size)
				println("tsize: "+t.rows.size)
								tableMap(mergeVar).rows.distinct.map(row => {
							var t = "###"
								tableMap(mergeVar).head.foreach(varName => {
									t+= row(tableMap(mergeVar).head(varName._1))+" "
								})
							println(t)
						})
			}*/
			return mutable.LinkedHashMap[String,RDFTable](vertexVar->t)
		}
		def mergeTable(vertexVar:String,t:RDFTable) {
			//println("RDFVertex merge:"+uri+"  "+t.rows)
//+			if (t.iter>iter) iter = t.iter
			//println("MERGE TABLENODE: "+vertexVar)
			tableMap(vertexVar) = tableMap(vertexVar).merge(t)  //.clone()
		}
	}
}
