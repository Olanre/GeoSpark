import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType, JoinBuildSide}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.monitoring.GeoSparkListener
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

object Geospark extends App{
    
    var sparkSession = SparkSession.builder()
    .appName("readGeoSparkScala") // Change this to a proper name
    // Enable GeoSpark custom Kryo serializer
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
    
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    GeoSparkSQLRegistrator.registerAll(sparkSession)


    //loading data
    val resourceFolder = System.getProperty("user.dir")+"/bigdata/"
    val arealm = resourceFolder + "arealm_merge.dbf"
      val pointlm= resourceFolder + "pointlm_merge.dbf"
      val edges = resourceFolder + "edges_merge.dbf"
      val areawater = resourceFolder + "areawater_merge.dbf"
    
    //loading arealm
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, arealm)
        var rawSpatialDf = Adapter.toDf(spatialRDD,sparkSession)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        var arealm_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape), _c1, _c2
                                       | FROM rawSpatialDf
                                     """.stripMargin)
        arealm_merge.show()
        arealm_merge.printSchema()

    //loading points
    var spatialRDD1 = new SpatialRDD[Geometry]
    spatialRDD1.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, pointlm)
        var rawSpatialDf1 = Adapter.toDf(spatialRDD1,sparkSession)
        rawSpatialDf1.createOrReplaceTempView("rawSpatialDf1")
        var pointlm_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape), _c1, _c2
                                       | FROM rawSpatialDf1
                                     """.stripMargin)
        pointlm_merge.show()
        pointlm_merge.printSchema()

    //loading edges
    var spatialRDD2 = new SpatialRDD[Geometry]
    spatialRDD2.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, edges)
        var rawSpatialDf2 = Adapter.toDf(spatialRDD2,sparkSession)
        rawSpatialDf2.createOrReplaceTempView("rawSpatialDf2")
        var edges_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape), _c1, _c2
                                       | FROM rawSpatialDf2
                                     """.stripMargin)
        edges_merge.show()
        edges_merge.printSchema()
        
    //loading areawater
    var spatialRDD3 = new SpatialRDD[Geometry]
    spatialRDD2.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, areawater)
        var rawSpatialDf3 = Adapter.toDf(spatialRDD3,sparkSession)
        rawSpatialDf3.createOrReplaceTempView("rawSpatialDf3")
        var edges_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape), _c1, _c2
                                       | FROM rawSpatialDf2
                                     """.stripMargin)
        arewater_merge.show()
        arewater_merge.printSchema()

    /////////////////////////////////////// SELECT POINT //////////////////////////////////////////////

    val beginTime = System.currentTimeMillis()
    getSelectAllFeaturesWithinADistanceFromPoint()
    val runtime = System.currentTimeMillis() - beginTime
    println("Select All Features Within A Distance From a Point took : " + runtime +" (ms)")
    
    ///////////////////////////////////////POLYGON AND POLYGON//////////////////////////////////////////////
    beginTime = System.currentTimeMillis()
    getSelectLongestLineIntersectsArea()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Longest Line Intersects Area took : " + runtime +" (ms)")


    beginTime = System.currentTimeMillis()
    getSelectAreaOverlapsArea()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Area Overlaps Area took : " + runtime +" (ms)")
    
    beginTime = System.currentTimeMillis()
    getSelectAreaContainsArea()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Area Overlaps Area took : " + runtime +" (ms)")

    beginTime = System.currentTimeMillis()
    getSelectAreaEqualsArea()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Area Equals Area took : " + runtime +" (ms)")
    
    beginTime = System.currentTimeMillis()
    getSelectAreaDisjointArea()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Area Disjoint Area took : " + runtime +" (ms)")
    
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
    getSelectLineOverlapsLine()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Line Overlaps Line took : " + runtime +" (ms)")

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

    beginTime = System.currentTimeMillis()
    getSelectPointEqualsPoint()
    runtime = System.currentTimeMillis() - beginTime
    println("Select Point Equals Point took : " + runtime +" (ms)")

    sparkSession.stop()
    System.out.println("All GeoSpark Benchmarks completed and passed!")

    /**
        * Test spatial join query.
        *
        * @throws Exception the exception
        */
    def getSelectAllFeaturesWithinADistanceFromPoint() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM arealm_merge  
             |WHERE ST_Distance(shape, ST_PointFromText('POINT(-97.7 30.30)', 0)) < 1000
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectLongestLineIntersectsArea(){
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM areawater_merge a, edges_merge e 
             |WHERE ST_Intersects(e.shape, a.shape) and e.se_row_id = 
             |(SELECT first 1 se_row_id from edges_merge order by ST_Length(shape) desc) 
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()
    
    }


    def getSelectAreaOverlapsArea() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE ST_overlaps(a1.shape, a2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectAreaContainsArea() {
        spatialDf = sparkSession.sql(
            """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE ST_contains(a1.shape, a2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectAreaEqualsArea() {
        spatialDf = sparkSession.sql(
            """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE ST_within(a1.shape, a2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectAreaDisjointArea() {
        spatialDf = sparkSession.sql(
            """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE ST_Disjoint(a1.shape, a2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectLineIntersectsArea() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_Intersects(e.shape, a.shape)"
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectLineWithinArea() {
        spatialDf = sparkSession.sql(
          """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_within(e.shape, a.shape)"
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectLineOverlapsArea() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_Overlaps(e.shape, a.shape)"
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectLineOverlapsLine() {
        spatialDf = sparkSession.sql(
           """
             |SELECT first 5  e1.se_row_id 
             |FROM  edges_merge e1 , edges_merge e2 
             |WHERE ST_overlaps(e1.shape, e2.shape) 
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectPointWithinArea() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM arealm_merge a, pointlm_merge p   
             |WHERE ST_Within(p.shape, a.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectPointIntersectsArea() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM arealm_merge a, pointlm_merge p   
             |WHERE ST_Intersects(p.shape, a.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectPointIntersectsLine() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM edges_merge e, pointlm_merge p 
             |WHERE ST_Intersects(p.shape, e.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
    
    def getSelectPointEqualsPoint() {
        spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM pointlm_merge p1, pointlm_merge p2
             |WHERE ST_Equals(p1.shape, p2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         spatialDf.show()

    }
}
