import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType, JoinBuildSide}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator



object GeoSparkBench extends App{
    
    var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
        config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
        config("geospark.global.index","true").
        config("spark.sql.crossJoin.enabled", "true").
        appName("GeoSparkSQL-demo").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)


    //loading data
    val resourceFolder = System.getProperty("user.dir")+"/bigdata/"
    val arealm = resourceFolder + "arealm_merge.shp"
      val pointlm= resourceFolder + "pointlm_merge.shp"
      val edges = resourceFolder + "edges_merge.shp"

    //loading arealm
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, arealm)
        var rawSpatialDf = Adapter.toDf(spatialRDD,sparkSession)
        rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
        var arealm_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape) as shape
                                       | FROM rawSpatialDf
                                     """.stripMargin)
        arealm_merge.createOrReplaceTempView("arealm_merge")
        arealm_merge.show()
        arealm_merge.printSchema()

    //loading points
    var spatialRDD1 = new SpatialRDD[Geometry]
    spatialRDD1.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, pointlm)
        var rawSpatialDf1 = Adapter.toDf(spatialRDD1,sparkSession)
        rawSpatialDf1.createOrReplaceTempView("rawSpatialDf1")
        var pointlm_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape) as shape
                                       | FROM rawSpatialDf1
                                     """.stripMargin)
        pointlm_merge.createOrReplaceTempView("pointlm_merge")
        pointlm_merge.show()
        pointlm_merge.printSchema()

    //loading edges
    var spatialRDD2 = new SpatialRDD[Geometry]
    spatialRDD2.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, edges)
        var rawSpatialDf2 = Adapter.toDf(spatialRDD2,sparkSession)
        rawSpatialDf2.createOrReplaceTempView("rawSpatialDf2")
        var edges_merge = sparkSession.sql("""
                                       | SELECT ST_GeomFromWKT(rddshape) as shape
                                       | FROM rawSpatialDf2
                                     """.stripMargin)
        edges_merge.createOrReplaceTempView("edges_merge")
        edges_merge.show()
        edges_merge.printSchema()


    var beginTime = System.currentTimeMillis()
    var runtime = System.currentTimeMillis() - beginTime

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



    sparkSession.stop()
    System.out.println("All GeoSpark Benchmarks completed and passed!")

    /**
        * Test spatial join query.
        *
        * @throws Exception the exception
        */

  ////////////////////////////////////////SPATIAL JOIN//////////////////////////////////////////////////



  ////////////////////////////////////////ALLPAIR SPATIAL JOIN//////////////////////////////////////////////////

  ///////////////////////////////////////POLYGON AND POLYGON//////////////////////////////////////////////

    def getSelectAreaOverlapsArea() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*)
             |FROM  arealm_merge a1 , arealm_merge a2
             |WHERE ST_Intersects(a1.shape, a2.shape) AND !(ST_Contains(a1.shape, a2.shape)) AND !(ST_Contains(a2.shape, a1.shape))
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }
    
    def getSelectAreaContainsArea() {
        var spatialDf = sparkSession.sql(
            """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE ST_Contains(a1.shape, a2.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }

    def getSelectAreaWithinArea() {
      var spatialDf = sparkSession.sql(
        """
          |SELECT count(*)
          |FROM  arealm_merge a1 , arealm_merge a2
          |WHERE ST_Within(a1.shape, a2.shape)
        """.stripMargin)
      spatialDf.createOrReplaceTempView("spatialdf")
      //spatialDf.show()

    }

    
    def getSelectAreaDisjointArea() {
        var spatialDf = sparkSession.sql(
            """
             |SELECT count(*) 
             |FROM  arealm_merge a1 , arealm_merge a2 
             |WHERE !(ST_Intersects(a1.shape, a2.shape))
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }

  ///////////////////////////////////////LINE AND POLYGON//////////////////////////////////////////////


  def getSelectLineIntersectsArea() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_Intersects(e.shape, a.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }
    
    def getSelectLineWithinArea() {
        var spatialDf = sparkSession.sql(
          """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_Within(e.shape, a.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }
    
    def getSelectLineOverlapsArea() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*)
             |FROM  arealm_merge a, edges_merge e 
             |WHERE ST_Intersects(e.shape, a.shape) AND !(ST_Contains(e.shape, a.shape)) AND !(ST_Contains(a.shape, e.shape))
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }

  ///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////


  def getSelectLineIntersectsLine() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT *
             |FROM  edges_merge e1 , edges_merge e2 
             |WHERE ST_Intersects(e1.shape, e2.shape)
             |LIMIT 5
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }


  ///////////////////////////////////////POINT AND //////////////////////////////////////////////

  def getSelectPointWithinArea() {
    var spatialDf = sparkSession.sql(
      """
        |SELECT count(*)
        |FROM arealm_merge a, pointlm_merge p
        |WHERE ST_Within(p.shape, a.shape)
      """.stripMargin)
    spatialDf.createOrReplaceTempView("spatialdf")
    //spatialDf.show()

  }

  def getSelectPointIntersectsArea() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM arealm_merge a, pointlm_merge p   
             |WHERE ST_Intersects(p.shape, a.shape)
           """.stripMargin)
      spatialDf.createOrReplaceTempView("spatialdf")
      //spatialDf.show()

    }
    
    def getSelectPointIntersectsLine() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM edges_merge e, pointlm_merge p 
             |WHERE ST_Intersects(p.shape, e.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }


    def getSelectPointEqualsPoint() {
        var spatialDf = sparkSession.sql(
           """
             |SELECT count(*) 
             |FROM pointlm_merge p1, pointlm_merge p2
             |WHERE ST_Within(p1.shape, p2.shape) AND ST_Within(p2.shape, p1.shape)
           """.stripMargin)
         spatialDf.createOrReplaceTempView("spatialdf")
         //spatialDf.show()

    }
}
