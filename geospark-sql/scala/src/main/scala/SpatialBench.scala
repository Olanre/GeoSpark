import spatialspark._
import org.apache.spark.{SparkConf, SparkContext}
import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkConf, SparkContext}
import spatialspark.join.{BroadcastSpatialJoin, PartitionedSpatialJoin}
import spatialspark.operator.SpatialOperator
import spatialspark.partition._
import spatialspark.partition.bsp.BinarySplitPartitionConf
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.partition.stp.SortTilePartitionConf
import spatialspark.query.RangeQuery
import spatialspark.util.MBR
import org.apache.spark.rdd.RDD


import scala.util.Try

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
  val	edges_mergePath="/root/bigdata/edges_merge.csv"
  val arealm_mergePath="/root/bigdata/arealm_merge.csv"

  val conf = new SparkConf().setAppName("Spatial Join App")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
  conf.set("spark.kryoserializer.buffer.max.mb", "512")

  val sc = new SparkContext(conf)


  def pRDD(sc: SparkContext , path:String) = sc.textFile(path)
    .map(_.split("\n"))
    .map { arr => (new WKTReader().read(arr(0))) }
    .zipWithIndex().map(_.swap)

  //load left dataset
  val pointlm_merge = sc.textFile(pointlm_mergePath).map(x => x.split("COMMA")).zipWithIndex()
  val pointlm_mergeById = pointlm_merge.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0)))))
    .filter(_._2.isSuccess).map(x => (x._1, x._2.get))


  val arealm_merge = sc.textFile(arealm_mergePath).map(x => x.split("COMMA")).zipWithIndex()
  val arealm_mergeById = arealm_merge.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0)))))
    .filter(_._2.isSuccess).map(x => (x._1, x._2.get))



  val edges_merge = sc.textFile(edges_mergePath).map(x => x.split("COMMA")).zipWithIndex()
  val edges_mergeById = edges_merge.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0)))))
    .filter(_._2.isSuccess).map(x => (x._1, x._2.get))

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
      println("Select Area Contains Area took : " + runtime +" (ms)")

      beginTime = System.currentTimeMillis()
      getSelectAreaWithinArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Area Within Area took : " + runtime +" (ms)")


      /////////////////////////////////////////LINE AND POLYGON //////////////////////////////////////////////
      beginTime = System.currentTimeMillis()
      getSelectLineIntersectsArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Line Intersects Area took : " + runtime +" (ms)")


      beginTime = System.currentTimeMillis()
      getSelectLineWithinArea()
      runtime = System.currentTimeMillis() - beginTime
      println("Select Line Within Area took : " + runtime +" (ms)")


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

  def getPartConf(leftGeometryById: RDD[(Long, Geometry)],
                  rightGeometryById: RDD[(Long, Geometry)]): PartitionConf ={

    var paralllelPartition = true
    var numPartitions = 512
    var method = "stp"
    var methodConf = "32:32:0.1"
    var extentString = ""
    var extent = extentString match {
      case "" =>
        val temp = leftGeometryById.map(x => x._2.getEnvelopeInternal)
          .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
          .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
        val temp2 = rightGeometryById.map(x => x._2.getEnvelopeInternal)
          .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
          .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
        (temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
      case _ => (extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
        extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)
    }

    var partConf = method match {
      case "stp" =>
        val dimX = methodConf.split(":").apply(0).toInt
        val dimY = methodConf.split(":").apply(1).toInt
        val ratio = methodConf.split(":").apply(2).toDouble
        new SortTilePartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4), ratio, paralllelPartition)
      case "bsp" =>
        val level = methodConf.split(":").apply(0).toLong
        val ratio = methodConf.split(":").apply(1).toDouble
        new BinarySplitPartitionConf(ratio, new MBR(extent._1, extent._2, extent._3, extent._4), level, paralllelPartition)
      case _ =>
        val dimX = methodConf.split(":").apply(0).toInt
        val dimY = methodConf.split(":").apply(1).toInt
        new FixedGridPartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4))
    }
    return partConf
  }


  def getSelectAreaOverlapsArea() {

    BroadcastSpatialJoin(sc, arealm_mergeById, arealm_mergeById, SpatialOperator.Overlaps)

  }

  def getSelectAreaContainsArea() {


    BroadcastSpatialJoin(sc, arealm_mergeById, arealm_mergeById, SpatialOperator.Contains)
  }

  def getSelectAreaWithinArea() {


    BroadcastSpatialJoin(sc, arealm_mergeById, arealm_mergeById, SpatialOperator.Within)

  }


  def getSelectLineIntersectsArea() {

    var partConf = getPartConf( edges_mergeById, arealm_mergeById)
    BroadcastSpatialJoin(sc, edges_mergeById, arealm_mergeById, SpatialOperator.Intersects)
    //PartitionedSpatialJoin(sc, edges_mergeById, arealm_mergeById, SpatialOperator.Intersects, 0.0, partConf)

  }

  def getSelectLineWithinArea() {
    var partConf = getPartConf( edges_mergeById, arealm_mergeById)
     BroadcastSpatialJoin(sc, edges_mergeById, arealm_mergeById, SpatialOperator.Within )
    //PartitionedSpatialJoin(sc, edges_mergeById, arealm_mergeById, SpatialOperator.Within , 0.0, partConf)

  }

  def getSelectLineOverlapsArea() {
    var partConf = getPartConf( edges_mergeById ,arealm_mergeById)
    BroadcastSpatialJoin(sc,  edges_mergeById, arealm_mergeById, SpatialOperator.Overlaps )
   // PartitionedSpatialJoin(sc,  edges_mergeById, arealm_mergeById, SpatialOperator.Overlaps , 0.0, partConf)

  }

  def getSelectLineIntersectsLine() {
    var partConf = getPartConf(edges_mergeById, edges_mergeById)
    //BroadcastSpatialJoin(sc, edges_mergeById, edges_mergeById, SpatialOperator.Intersects)
    PartitionedSpatialJoin(sc, edges_mergeById, edges_mergeById, SpatialOperator.Intersects, 0.0, partConf)

  }

  def getSelectPointWithinArea() {

    BroadcastSpatialJoin(sc, pointlm_mergeById, arealm_mergeById, SpatialOperator.Within)

  }

  def getSelectPointIntersectsArea() {

    BroadcastSpatialJoin(sc, pointlm_mergeById, arealm_mergeById, SpatialOperator.Intersects)

  }

  def getSelectPointIntersectsLine() {
    var partConf = getPartConf(pointlm_mergeById, edges_mergeById)
    BroadcastSpatialJoin(sc, pointlm_mergeById, edges_mergeById, SpatialOperator.Intersects)
    //PartitionedSpatialJoin(sc, pointlm_mergeById, edges_mergeById, SpatialOperator.Intersects , 0.0, partConf)


  }


}
