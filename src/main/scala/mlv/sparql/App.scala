package mlv.sparql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.graph.Node
import org.apache.jena.sparql.syntax.ElementGroup
import org.apache.jena.sparql.syntax.Element
import org.apache.jena.sparql.syntax.ElementPathBlock
import org.apache.jena.sparql.core.TriplePath
import scala.collection.JavaConversions._

object App {

  val FILE_UNIV = "data/univ1.nt"
  val FILE_PROPS = "data/univProps.txt"
  val FILE_CONCEPTS = "data/univConcepts.txt"
  val FILE_DICT = "data/univ1_dict.txt"
  val FILE_ENCODED = "data/univ1_encoded.txt"
  val FILE_ALGEBRA = "data/algebra.txt"
  val FILE_RESULT_ENCODED = "data/result_encoded"
  val FILE_RESULT_DECODED = "data/result_decoded"

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


    // Utilisation DSL pour encoder

    val u1 = univ.select("*").withColumnRenamed("s1", "s").withColumnRenamed("s2", "p").withColumnRenamed("s3", "o")
    val s1 = dict.select("*").withColumnRenamed("s1", "s")
    val p1 = props.select("*").withColumnRenamed("s1", "p").withColumnRenamed("l1", "l2")
    val c1 = concepts.select("*").withColumnRenamed("s1", "o").withColumnRenamed("l1", "l3")

    val res = u1.join(s1, Seq("s")).select("l1", "p", "o")
    val res1 = res.join(p1, Seq("p")).select("l1", "l2", "o")
    val res2 = res1.join(c1, Seq("o")).select("l1", "l2", "l3")
    res2.map(x => x(0) + " " + x(1) +  " " +  x(2)).coalesce(1,true).saveAsTextFile(FILE_RESULT_ENCODED)
  
    // Utilisation DSL pour decoder
    val e11 = encoded.select("*").withColumnRenamed("l1", "s").withColumnRenamed("l2", "p").withColumnRenamed("l3", "o")
    val s11 = dict.select("*").withColumnRenamed("l1", "s")
    val p11 = props.select("*").withColumnRenamed("l1", "p").withColumnRenamed("s1", "s2")
    val c11 = concepts.select("*").withColumnRenamed("l1", "o").withColumnRenamed("s1", "s3")
    
    val resb = e11.join(s11, Seq("s")).select("s1", "p", "o")
    val res1b = resb.join(p11, Seq("p")).select("s1", "s2", "o")
    val res2b = res1b.join(c11, Seq("o")).select("s1", "s2", "s3")
    res2b.map(x => x(0) + " " + x(1) +  " " +  x(2)).coalesce(1,true).saveAsTextFile(FILE_RESULT_DECODED)
    
    
    sc.stop
  }
}