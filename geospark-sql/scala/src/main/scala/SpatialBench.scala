import spatialspark._
import org.apache.spark.{SparkConf, SparkContext}
import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkConf, SparkContext}
import spatialspark.join.{BroadcastSpatialJoin, PartitionedSpatialJoin}
import spatialspark.operator.SpatialOperator
import spatialspark.query.RangeQuery

object SpatialBench extends App {
  val showProgress = "true" // show Spark's progress bar (makes log file look ugly)

  val numRuns = 5

  private val cellSize = 2
  private val cellSizePoly = 2

  private val maxCostPoints = 100 * 1000
  private val maxCostPolies = 10 * 1000

  private val ppD = 90
  private val partitions = ppD * ppD

  private val treeOrderPoints = 10
  private val treeOrderPolies = 10

  val initialPartitions = 32
  val bsp_threshold = 32

  val ratio = 0.0001
  val levels = 10
  val parallel = true

  //  val persistPointsPath = "/data/edbt2017/stark_persist_points"
  //  val persistPoliesPath = "/data/edbt2017/stark_persist_polies"

  val doFilters = true
  val doJoins = false

  val nonPersistent = true
  val persistent = false

  val reorderLocation = "/data/taxi/SpatialSparkReordered.out"

  val pointlm_mergePath  = "/root/bigdata/pointlm_merge.csv"
  val	edgespath="/root/bigdata/edges_merge.csv"
  val arealmpath="/root/bigdata/arealm_merge.csv"
  val areawaterpath="/root/bigdata/areawater_merge.csv"

  val conf = new SparkConf().setAppName("Spatial Join App")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
  val sc = new SparkContext(conf)


  def pRDD(sc: SparkContext , path:String) = sc.textFile(path)
    .map(_.split("\n"))
    .map { arr => (new WKTReader().read(arr(0))) }
    .zipWithIndex().map(_.swap)
  val pointlm_merge = pRDD(sc,pointlm_mergePath)
  val arealm_merge = pRDD(sc,arealmpath)
  val edges_merge = pRDD(sc,edgespath)
  val areawater_merge = pRDD(sc,areawaterpath)

  var beginTime = System.currentTimeMillis()
  var runtime = System.currentTimeMillis() - beginTime

    try {

      ///////////////////////////////////////POLYGON AND POLYGON//////////////////////////////////////////////


      beginTime = System.currentTimeMillis()
      getSelectAreaOverlapsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Area Overlaps Area took : " + runtime +" (ms)")

      beginTime = System.currentTimeMillis()
      getSelectAreaContainsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Area Overlaps Area took : " + runtime +" (ms)")

      beginTime = System.currentTimeMillis()
      getSelectAreaWithinArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Area Equals Area took : " + runtime +" (ms)")


      /////////////////////////////////////////LINE AND POLYGON //////////////////////////////////////////////
      beginTime = System.currentTimeMillis()
      getSelectLineIntersectsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Line IntersectsArea took : " + runtime +" (ms)")


      beginTime = System.currentTimeMillis()
      getSelectLineWithinArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Area Within Area took : " + runtime +" (ms)")


      beginTime = System.currentTimeMillis()
      getSelectLineOverlapsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Line Overlaps Area took : " + runtime +" (ms)")


      ///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////
      beginTime = System.currentTimeMillis()
      getSelectLineIntersectsLine()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Line Intersects Line took : " + runtime +" (ms)")

      ///////////////////////////////////////POINT AND POINT //////////////////////////////////////////////
      beginTime = System.currentTimeMillis()
      getSelectPointWithinArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Point Within Area took : " + runtime +" (ms)")


      beginTime = System.currentTimeMillis()
      getSelectPointIntersectsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Point Intersects Area took : " + runtime +" (ms)")

      beginTime = System.currentTimeMillis()
      getSelectPointIntersectsLine()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Point Intersects Line took : " + runtime +" (ms)")


      System.out.println("All SpatialSpark Benchmarks completed and passed!")



      sc.stop()
    } catch {
      case e: Throwable =>
        // println(spatialsparkStats.evaluate)
        println(e.getMessage)
        e.printStackTrace(System.err)
    }




  def getSelectAreaOverlapsArea() {

    BroadcastSpatialJoin(sc, arealm_merge, arealm_merge, SpatialOperator.Overlaps)

  }

  def getSelectAreaContainsArea() {


    BroadcastSpatialJoin(sc, arealm_merge, arealm_merge, SpatialOperator.Contains)
  }

  def getSelectAreaWithinArea() {


    BroadcastSpatialJoin(sc, arealm_merge, arealm_merge, SpatialOperator.Within)


  }


  def getSelectLineIntersectsArea() {

    BroadcastSpatialJoin(sc, arealm_merge, edges_merge, SpatialOperator.Intersects)


  }

  def getSelectLineWithinArea() {

    BroadcastSpatialJoin(sc, arealm_merge, edges_merge, SpatialOperator.Within)


  }

  def getSelectLineOverlapsArea() {

    BroadcastSpatialJoin(sc, arealm_merge, edges_merge, SpatialOperator.Overlaps)

  }

  def getSelectLineIntersectsLine() {
    BroadcastSpatialJoin(sc, edges_merge, edges_merge, SpatialOperator.Intersects)

  }

  def getSelectPointWithinArea() {

    BroadcastSpatialJoin(sc, pointlm_merge, arealm_merge, SpatialOperator.Within)

  }

  def getSelectPointIntersectsArea() {

    BroadcastSpatialJoin(sc, pointlm_merge, arealm_merge, SpatialOperator.Intersects)

  }

  def getSelectPointIntersectsLine() {

    BroadcastSpatialJoin(sc, pointlm_merge, edges_merge, SpatialOperator.Intersects)


  }


}
