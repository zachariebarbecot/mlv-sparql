package mlv.sparql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra

object App {

  val FILE_UNIV = "data/univ1.nt"
  val FILE_PROPS = "data/univProps.txt"
  val FILE_CONCEPTS = "data/univConcepts.txt"
  val FILE_DICT = "data/univ1_dict.txt"
  val FILE_ENCODED = "data/univ1_encoded.txt"

  case class Univ(s1: String, s2: String, s3: String)
  case class Props(l1: Long, s1: String)
  case class Concepts(l1: Long, s1: String)
  case class Dict(s1: String, l1: Long)
  case class Encoded(l1: Long, l2: Long, l3: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Sparql project")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val univ = sc.textFile(FILE_UNIV).map(_.split(" ")).map(u => Univ(u(0), u(1), u(2))).toDF
    univ.registerTempTable("univ")
    val props = sc.textFile(FILE_PROPS).map(_.split(" ")).map(p => Props(p(0).trim.toLong, p(1))).toDF
    props.registerTempTable("props")
    val concepts = sc.textFile(FILE_CONCEPTS).map(_.split(" ")).map(c => Concepts(c(0).trim.toLong, c(1))).toDF
    concepts.registerTempTable("concepts")
    val dict = sc.textFile(FILE_DICT).map(_.split(" ")).map(d => Dict(d(0), d(1).trim.toLong)).toDF
    dict.registerTempTable("dict")
    val encoded = sc.textFile(FILE_ENCODED).map(_.split(" ")).map(e => Encoded(e(0).trim.toLong, e(1).trim.toLong, e(2).trim.toLong)).toDF
    encoded.registerTempTable("encoded")

    val qstr = """PREFIX  lubm: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
        PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT  ?x ?y
        WHERE
          {  ?x rdf:type lubm:Chair .
             ?y rdf:type lubm:Department .
             ?x lubm:worksFor ?y .
             ?y lubm:subOrganizationOf <http://www.University0.edu>
          }"""
    val query = QueryFactory.create(qstr)
    val op = Algebra.compile(query)
    println(op)

    sc.stop
  }
}