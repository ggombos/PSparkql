import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable._
import scala.io.Source 

object LUBMInfer {	
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Spar(k)ql LUBMInfer")
		val sc = new SparkContext(conf)
		val ontoPrefix = "http://spark.elte.hu"+"#"
		//var result:ListBuffer[String] = new ListBuffer[String]()
		val aType:String = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		
		val doctoralDegreeFrom = "<"+ontoPrefix+"doctoralDegreeFrom>"
		val mastersDegreeFrom = "<"+ontoPrefix+"mastersDegreeFrom>"
		val undergraduateDegreeFrom = "<"+ontoPrefix+"undergraduateDegreeFrom>"
		val GraduateCourse = "<"+ontoPrefix+"GraduateCourse>"
		val TechnicalReport = "<"+ontoPrefix+"TechnicalReport>"
		val JournalArticle = "<"+ontoPrefix+"JournalArticle>"
		val ConferencePaper = "<"+ontoPrefix+"ConferencePaper>"
		val	UndergraduateStudent = "<"+ontoPrefix+"UndergraduateStudent>"
		val	GraduateStudent = "<"+ontoPrefix+"GraduateStudent>"
		val ClericalStaff = "<"+ontoPrefix+"ClericalStaff>"
		val SystemsStaff = "<"+ontoPrefix+"SystemsStaff>"
		val Lecturer = "<"+ontoPrefix+"Lecturer>"
		val PostDoc = "<"+ontoPrefix+"PostDoc>"
		val AssistantProfessor = "<"+ontoPrefix+"AssistantProfessor>"
		val AssociateProfessor = "<"+ontoPrefix+"AssociateProfessor>"
		val Chair = "<"+ontoPrefix+"Chair>"
		val Dean = "<"+ontoPrefix+"Dean>"
		val FullProfessor = "<"+ontoPrefix+"FullProfessor>"
		val VisitingProfessor = "<"+ontoPrefix+"VisitingProfessor>"
		val headOf = "<"+ontoPrefix+"headOf>"
		val linesRDD:RDD[String] = sc.textFile(args(0)).flatMap(line => {
			var result:ListBuffer[String] = new ListBuffer[String]()
			val l = line.split("\\s+")
			val s:String = l(0)
			val p:String = l(1) 
			val o:String = l(2)
			if (p == aType) {
				o match {
					case a @ (`doctoralDegreeFrom` | `mastersDegreeFrom` | `undergraduateDegreeFrom`) => {
						result += s+" "+aType+" <"+ontoPrefix+"degreeFrom> ."
					}
					case  a @ (`GraduateCourse`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Work> ."
					}
					case  a @ (`TechnicalReport` | `JournalArticle` | `ConferencePaper`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Publication> ."
					}
					case a @ (`UndergraduateStudent`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Person> ."
						result += s+" "+aType+" <"+ontoPrefix+"Student> ."
					}
					case a @ (`GraduateStudent`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Student> ."
					}
					case a @ (`ClericalStaff` | `SystemsStaff` | `Lecturer` | `PostDoc`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Employee> ."
						result += s+" "+aType+" <"+ontoPrefix+"Person> ."
					}
					case a @ (`AssistantProfessor` | `AssociateProfessor` | `FullProfessor` | `VisitingProfessor`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Employee> ."
						result += s+" "+aType+" <"+ontoPrefix+"Person> ."
						result += s+" "+aType+" <"+ontoPrefix+"Faculty> ."
						result += s+" "+aType+" <"+ontoPrefix+"Professor> ."
					}
					case a @ (`Chair` | `Dean`) => {
						result += s+" "+aType+" <"+ontoPrefix+"Employee> ."
						result += s+" "+aType+" <"+ontoPrefix+"Person> ."
						result += s+" "+aType+" <"+ontoPrefix+"Faculty> ."
					}
					case _ => {}
				}
			}
			p match {
				case `headOf` => {
					result += s+" <"+ontoPrefix+"worksFor> "+o+" ."
				}
				case _ => {}
			}
			
			result += s+" "+p+" "+o+" ."
			result
		})
				
		linesRDD.coalesce(1,true).saveAsTextFile("/user/ggombos/sparkGraphInput/LUBMInfer")
	}
}