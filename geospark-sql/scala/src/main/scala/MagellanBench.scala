
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.magellan.dsl.expressions._
import magellan._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._


object MagellanBench extends App{
  // SPARK CONFIGURATION
  val conf = new SparkConf()
    .setAppName("MagellanUnitTest")
    .set("spark.sql.crossJoin.enabled", "true")

  val spark = SparkSession.builder()
    .config(conf)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
  import spark.implicits._ // << add this
  var sqlContext = spark.sqlContext
  var sc = spark.sparkContext

  /**
  val conf = new SparkConf()
    .setAppName("MagellanBenchMark")
    .set("spark.sql.crossJoin.enabled", "true")

  val sparkSession = SparkSession.builder()
    .config(conf)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  var sqlContext = sparkSession.sqlContext
  var sc = sparkSession.sparkContext

    */


  // LOAD DATA
  val dataPath = "Point data path"
  //val latlngDF = spark.read.json(dataPath))

  //Load data and create indexes
  var pointlmpath = "/root/bigdata/pointlm"
  var pointlm_merge = sqlContext.read.format("magellan").
    option("magellan.index", "true").
    load(pointlmpath).
    select($"*").
    cache()

  var edgespath = "/root/bigdata/edges"
  var edges_merge = sqlContext.read.format("magellan").
    option("magellan.index", "true").
    load(edgespath).
    select($"*").
    cache()

  var arealmpath = "/root/bigdata/arealm"
  var arealm_merge = sqlContext.read.format("magellan").
    option("magellan.index", "true").
    load(arealmpath).
    select($"*").
    cache()


  var beginTime = System.currentTimeMillis()
  var runtime = System.currentTimeMillis() - beginTime

  var j1 = arealm_merge
    .select($"polygon".as("s1"))


  var j2 = arealm_merge
    .select($"polygon".as("s2"))


  var em1 = edges_merge
    .select($"polyline".as("e1"))

  var em2 = edges_merge
    .select($"polyline".as("e2"))

  var p1 = pointlm_merge
    .select($"point".as("p1"))

  var p2 = pointlm_merge
    .select($"point".as("p2"))

  ///////////////////////////////////////POLYGON AND POLYGON//////////////////////////////////////////////
  beginTime = System.currentTimeMillis()
  getSelectAreaWithinArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Longest Line Intersects Area took : " + runtime + " (ms)")


  beginTime = System.currentTimeMillis()
  getSelectAreaOverlapsArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Area Overlaps Area took : " + runtime + " (ms)")

  beginTime = System.currentTimeMillis()
  getSelectAreaContainsArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Area Contains Area took : " + runtime + " (ms)")

  beginTime = System.currentTimeMillis()
  getSelectAreaDisjointArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Area Disjoint Area took : " + runtime + " (ms)")

  /////////////////////////////////////////LINE AND POLYGON //////////////////////////////////////////////
  beginTime = System.currentTimeMillis()
  getSelectLineIntersectsArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Line Intersects Area took : " + runtime + " (ms)")


  beginTime = System.currentTimeMillis()
  getSelectLineWithinArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Area Within Area took : " + runtime + " (ms)")


  beginTime = System.currentTimeMillis()
  getSelectLineOverlapsArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Line Overlaps Area took : " + runtime + " (ms)")


  ///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////
  beginTime = System.currentTimeMillis()
  getSelectLineIntersectsLine()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Line Intersects Line took : " + runtime + " (ms)")

  ///////////////////////////////////////POINT AND POINT //////////////////////////////////////////////
  beginTime = System.currentTimeMillis()
  getSelectPointWithinArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Point Within Area took : " + runtime + " (ms)")


  beginTime = System.currentTimeMillis()
  getSelectPointIntersectsArea()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Point Intersects Area took : " + runtime + " (ms)")

  beginTime = System.currentTimeMillis()
  getSelectPointIntersectsLine()
  runtime = System.currentTimeMillis() - beginTime
  println("Select Point Intersects Line took : " + runtime + " (ms)")

  //beginTime = System.currentTimeMillis()
  //getSelectPointEqualsPoint()
  //runtime = System.currentTimeMillis() - beginTime
  //println("Select Point Equals Point took : " + runtime + " (ms)")

  sc.stop()
  System.out.println("All GeoSpark Benchmarks completed and passed!")


  /////////////////////////////////////////POLYGON AND POLYGON //////////////////////////////////////////////


  def getSelectAreaWithinArea() {

    val joined2 = j1.join(j2).where($"s1" within $"s2")

  }

  def getSelectAreaOverlapsArea() {
    //select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_Intersects(a1.shape, a2.shape) AND assert_true(ST_contains(a1.shape, a2.shape))AND assert_true(ST_contains(a2.shape, a1.shape))
    val joined1 = j1.join(j2).where($"s1" intersects $"s2" and !($"s1" >? $"s2") and !($"s2" >? $"s1"))
    //joined1.show()
  }


  def getSelectAreaContainsArea() {
    //select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_contains(a1.shape, a2.shape)
    val joined2 = j1.join(j2).where($"s1" >? $"s2")
    //joined2.count()
  }


  def getSelectAreaDisjointArea() {
    //sb.append("select count(*) from  arealm_merge a1, arealm_merge a2 where assert_true(ST_Intersects(a1.shape, a2.shape))");

    val joined4 = j1.join(j2).where(!($"s1" intersects $"s2"))
    //joined4.count()
  }

  /////////////////////////////////////////LINE AND POLYGON //////////////////////////////////////////////


  def getSelectLineIntersectsArea() {
    //sb.append("select count(*) from  arealm_merge a, edges_merge e where Intersects(a.shape, e.shape)");
    val line1 = j1.join(em1).where(!($"e1" intersects $"s1"))
    //line1.count();
  }

  def getSelectLineWithinArea() {

    //sb.append("select count(*) from  arealm_merge a, edges_merge e where within(a.shape, e.shape)");
    val line2 = j1.join(em1).where(!($"e1" within $"s1"))
    //line2.count();
  }

  def getSelectLineOverlapsArea() {
    //sb.append("select  count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.shape, a.shape) AND assert_true(ST_contains(e.shape, a.shape))AND assert_true(ST_contains(a.shape, e.shape))");
    val line3 = j1.join(em1).where(!($"e1" intersects $"s1") and !($"e1" >? $"s1") and !($"s1" >? $"e1"))
    //line3.count();
  }

  ///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////

  def getSelectLineIntersectsLine() {
    //sb.append("select first 5  e1.se_row_id from  edges_merge e1 , edges_merge e2 where ST_Intersects(e1.shape, e2.shape) AND assert_true(ST_contains(e1.shape, e2.shape))AND assert_true(ST_contains(e2.shape, e1.shape))  ");


    val line4 = em1.join(em2).where(($"e1" intersects $"e2"))
    //line4.show(5);
  }


  ///////////////////////////////////////POINT AND //////////////////////////////////////////////

  def getSelectPointWithinArea() {
    //sb.append("select count(*)  from  arealm_merge a, pointlm_merge p where ST_Within(p.shape, a.shape)");

    val point1 = p1.join(j1).where($"p1" within $"s1")
    //point1.count()
  }

  def getSelectPointIntersectsArea() {
    //sb.append("select count(*)  from  arealm_merge a, pointlm_merge p where ST_Intersects(p.shape, a.shape)");
    val point2 = p1.join(j1).where($"p1" intersects $"s1")
    //point2.count()
  }

  def getSelectPointIntersectsLine() {
    //sb.append("select count(*)  from  edges_merge e, pointlm_merge p where ST_Intersects(p.shape, e.shape)");
    val point3 = p1.join(em1).where($"p1" intersects $"e1")
    //point3.count()
  }

  def getSelectPointEqualsPoint(){

    //sb.append("select count(*) from  pointlm_merge p1, pointlm_merge p2 where ST_within(p1.shape, p2.shape) AND ST_within(p2.shape, p1.shape)");
    val point4 = p1.join(p2).where(($"p1" >? $"p2") and ($"p2" >? $"p1"))
    //point4.count()
  }



}




