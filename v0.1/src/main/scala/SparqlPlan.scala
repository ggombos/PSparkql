import scala.collection.mutable._
import scala.collection.immutable
import SparqlSpark._
import scala.collection.concurrent._

object PSparqlPlan {
    class Triple(val tp:String) extends java.io.Serializable {
        var spo = tp.split(" ")
        val s:String = spo(0)
        val p:String = spo(1)
        val o:String = spo(2)
        val str:String = tp
        var finish:Boolean = false
		
		override def toString():String ={
            return str
        }
    }
	
	class PlanResult(_plan:ListBuffer[PlanItem], _rootNode:String, _varNum:Int, _dataProperties:LinkedHashMap[String,MutableList[VertexProp]]) extends java.io.Serializable {
		val plan:ListBuffer[PlanItem] = _plan.clone()
		val rootNode:String = _rootNode
		val numVar:Int = _varNum
		val dataProperties = _dataProperties.clone()
	}
	
	class PlanItem(_tp:Triple,_src:String,_headPattern:Set[String]) extends java.io.Serializable {
		val tp:Triple = _tp
		val src:String = _src
		var headPattern:Set[String] = _headPattern
        var level:Int = 0
	}
	
	def orderByMaxProp(TPs:Array[Triple], objProps:immutable.Set[String]):Array[Triple] = {
		if (TPs.size > 0) {
			val queryObjProp = TPs.map(t => t.p)
			val maxObjProperty = objProps.filter(a => queryObjProp.contains(a))
					.groupBy((p:String) => p)
					.mapValues(_.size).toSeq
					.sortWith(_._2 > _._2).take(1)(0)._1
			println("maxObjProp: "+maxObjProperty)


			var i = 0;
			while ( TPs(i).p != maxObjProperty && i < TPs.size ) {
				i+=1
			}
			if (i < TPs.size) {
				var tmp = TPs(0)
				TPs(0) = TPs(i)
				TPs(i) = tmp
			}
		}
		return TPs
	}

	def splitQuery(SparqlQuery:String, dataProps:immutable.Set[String]):(Array[Triple],LinkedHashMap[String,MutableList[VertexProp]])= {
	    var query = SparqlQuery.split(" . ")
        var TPs:Array[Triple] = query.map(tp => new Triple(tp))
	    var queryArray:Array[String] = Array[String]()
	    var dataProperties:LinkedHashMap[String,MutableList[VertexProp]] = LinkedHashMap[String,MutableList[VertexProp]]()
	    TPs = TPs.flatMap(tp => {
	        if ((tp.p == "rdf:type") || (tp.p == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")|| tp.o(0) == '"'	//"
				|| dataProps.contains(tp.p)) {
	            var propList = dataProperties.getOrElseUpdate(tp.s, MutableList[VertexProp]())
				propList += new VertexProp(tp.p,tp.o)
	            dataProperties(tp.s) = propList
	            Array[Triple]()
	        } else {
                Array[Triple](tp)
	        }
        })
        return (TPs,dataProperties)
    }
    
    def createPlan(SparqlQuery: String, dataProps:immutable.Set[String], objProps:immutable.Set[String]):PlanResult = {
        var (objPropPatterns,dataPropPatterns):(Array[Triple],LinkedHashMap[String,MutableList[VertexProp]]) = splitQuery(SparqlQuery,dataProps)
		
		objPropPatterns = orderByMaxProp(objPropPatterns,objProps)

        var plan = ListBuffer[PlanItem]()
        var rootNode = ""
        var vars:Set[String] = HashSet()
        var q = new Queue[String]()
        

        if (objPropPatterns.size > 0) {
            rootNode = objPropPatterns(0).s
            q.enqueue(rootNode)
        } else {
            rootNode = dataPropPatterns.keySet.head
        }
        
        while (!q.isEmpty) {
            var actVar = q.dequeue
            objPropPatterns.map(pattern => {
                if (!pattern.finish && (pattern.s == actVar || pattern.o == actVar)) {
                    vars += pattern.s
                    vars += pattern.o
                    if ( pattern.s == actVar) {
                        if (!q.contains(pattern.o)) q.enqueue(pattern.o)
                        plan += new PlanItem(new Triple(pattern.str),pattern.o,Set[String]())
                    } else {
                        if (!q.contains(pattern.s)) q.enqueue(pattern.s)
                        plan += new PlanItem(new Triple(pattern.str),pattern.s,Set[String]())
                    }
                    pattern.finish = true
                }                
            })
        }
        
        plan = addAcceptHeaders(plan)
        printPlan(plan,dataPropPatterns)
        return new PlanResult(plan,rootNode,vars.size,dataPropPatterns)
    }
    
    
    def addAcceptHeaders(p:ListBuffer[PlanItem]):ListBuffer[PlanItem] = {
        var headers:LinkedHashMap[String,Set[String]] = LinkedHashMap[String,Set[String]]()
        var levels:LinkedHashMap[String,Int] = LinkedHashMap[String,Int]()
        
        var plan = p.reverse
        
        plan = plan.map(planitem => {
            var src = planitem.src
            var des = planitem.tp.o
            if (src == planitem.tp.o) {
                des = planitem.tp.s
            }
            if (!headers.contains(des)) headers(des) = HashSet()
            if (!headers.contains(src)) headers(src) = HashSet()

            if (!levels.contains(des)) levels(des) = 0
            if (!levels.contains(src)) levels(src) = 0
            
            headers(des) = headers(des).union(headers(src))
            headers(des) += src
            
            levels(des) = levels(src)+1
            
            planitem.headPattern = headers(src)
            planitem.level = levels(src)
            planitem
       })
       return plan
    }

    def printPlan(plan:ListBuffer[PlanItem],dataProperties:LinkedHashMap[String,MutableList[VertexProp]]) {  
        plan.map(tp => { 
            println("PLAN-- [")
            //list.map(tp => 
            println("PLAN-- ("+tp.tp.toString()+", "+tp.src+", "+tp.headPattern+", "+tp.level+")")//)
            println("PLAN-- ],")
        })
        println("PLAN-- DATAPROPERTIES")
        dataProperties.map(v => {
            println("PLAN-- "+v._1)
            v._2.map(p => {
                println("PLAN-- "+p.prop+" "+p.obj)
            })
        })
    }

}