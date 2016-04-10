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
  val FILE_ALGEBRA = "data/algebra.txt"
  val FILE_RESULT = "data/result.txt"

  case class Univ(s1: String, s2: String, s3: String)
  case class Props(l1: Long, s1: String)
  case class Concepts(l1: Long, s1: String)
  case class Dict(s1: String, l1: Long)
  case class Encoded(l1: Long, l2: Long, l3: Long)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\zacha\\Documents\\workspace\\sparql\\winutil\\")

    val conf = new SparkConf()
      .setAppName("Sparql project")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val univ = sc.textFile(FILE_UNIV).map(_.split(" ")).map(u => Univ(u(0), u(1), u(2))).toDF
    val props = sc.textFile(FILE_PROPS).map(_.split(" ")).map(p => Props(p(0).trim.toLong, p(1))).toDF
    val concepts = sc.textFile(FILE_CONCEPTS).map(_.split(" ")).map(c => Concepts(c(0).trim.toLong, c(1))).toDF
    val dict = sc.textFile(FILE_DICT).map(_.split(" ")).map(d => Dict(d(0), d(1).trim.toLong)).toDF
    val encoded = sc.textFile(FILE_ENCODED).map(_.split(" ")).map(e => Encoded(e(0).trim.toLong, e(1).trim.toLong, e(2).trim.toLong)).toDF

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
    scala.tools.nsc.io.File(FILE_ALGEBRA).writeAll(op.toString())
    val algebra = sc.textFile(FILE_ALGEBRA).map(_.split("\n"))

    //Utilisation des dsl dataframes
    /*
    val u1 = univ.select("*").withColumnRenamed("s1", "s").withColumnRenamed("s2", "p").withColumnRenamed("s3", "o")
    val s1 = dict.select("*").withColumnRenamed("s1", "s")
    val p1 = props.select("*").withColumnRenamed("s1", "p").withColumnRenamed("l1", "l2")
    val c1 = concepts.select("*").withColumnRenamed("s1", "o").withColumnRenamed("l1", "l3")

    val res = u1.join(s1, Seq("s")).select("l1", "p", "o")
    val res1 = res.join(p1, Seq("p")).select("l1", "l2", "o")
    val res2 = res1.join(c1, Seq("o")).select("l1", "l2", "l3")
    val res3 = res2.unionAll(encoded)
    res3.rdd.coalesce(1, true).saveAsTextFile(FILE_RESULT)
		*/

    sc.stop
  }
}