package example;


import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

// TODO: Auto-generated Javadoc
/**
 * The Class Example.
 */
public class Example implements Serializable{

	/** The sc. */
	public static JavaSparkContext sc;

	/** The geometry factory. */
	static GeometryFactory geometryFactory;

	/** The Point RDD input location. */
	static String PointRDDInputLocation;

	/** The Point RDD offset. */
	static Integer PointRDDOffset;

	/** The Point RDD num partitions. */
	static Integer PointRDDNumPartitions;

	/** The Point RDD splitter. */
	static FileDataSplitter PointRDDSplitter;

	/** The Point RDD index type. */
	static IndexType PointRDDIndexType;

	/** The object RDD. */
	static PointRDD objectRDD;

	/** The Polygon RDD input location. */
	static String PolygonRDDInputLocation;

	/** The Polygon RDD start offset. */
	static Integer PolygonRDDStartOffset;

	/** The Polygon RDD end offset. */
	static Integer PolygonRDDEndOffset;

	/** The Polygon RDD num partitions. */
	static Integer PolygonRDDNumPartitions;

	/** The Polygon RDD splitter. */
	static FileDataSplitter PolygonRDDSplitter;

	/** The query window RDD. */
	static PolygonRDD queryWindowRDD;

	/** The join query partitioning type. */
	static GridType joinQueryPartitioningType;

	/** The each query loop times. */
	static int eachQueryLoopTimes;

	/** The k NN query point. */
	static Point kNNQueryPoint;

	/** The range query window. */
	static Envelope rangeQueryWindow;

	static String ShapeFileInputLocation;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
		sc = new JavaSparkContext(conf);
		var test;
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

		String resourceFolder = System.getProperty("user.dir")+"/src/test/resources/";

		PointRDDInputLocation = resourceFolder+"arealm-small.csv";
		PointRDDSplitter = FileDataSplitter.CSV;
		PointRDDIndexType = IndexType.RTREE;
		PointRDDNumPartitions = 5;
		PointRDDOffset = 0;

		PolygonRDDInputLocation = resourceFolder + "county_small.tsv";
		PolygonRDDSplitter = FileDataSplitter.WKT;
		PolygonRDDNumPartitions = 5;
		PolygonRDDStartOffset = 0;
		PolygonRDDEndOffset = -1;

		geometryFactory=new GeometryFactory();
		kNNQueryPoint=geometryFactory.createPoint(new Coordinate(-84.01, 34.01));
		rangeQueryWindow=new Envelope (-90.01,-80.01,30.01,40.01);
		joinQueryPartitioningType = GridType.QUADTREE;
		eachQueryLoopTimes=5;

		ShapeFileInputLocation = resourceFolder+"shapefiles/polygon";

		try {
			testSpatialRangeQuery();
			testSpatialRangeQueryUsingIndex();
			testSpatialKnnQuery();
			testSpatialKnnQueryUsingIndex();
			testSpatialJoinQuery();
			testSpatialJoinQueryUsingIndex();
			testDistanceJoinQuery();
			testDistanceJoinQueryUsingIndex();
			testCRSTransformationSpatialRangeQuery();
			testCRSTransformationSpatialRangeQueryUsingIndex();
			testCRSTransformation();
			testLoadShapefileIntoPolygonRDD();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("GeoSpark DEMOs failed!");
			return;
		}
		sc.stop();
		System.out.println("All GeoSpark DEMOs passed!");

	}

	/**
	 * Test spatial range query.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialRangeQuery() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count();
			assert resultSize>-1;
		}
	}



	/**
	 * Test spatial range query using index.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialRangeQueryUsingIndex() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		objectRDD.buildIndex(PointRDDIndexType,false);
		objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count();
			assert resultSize>-1;
		}

	}

	/**
	 * Test spatial knn query.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialKnnQuery() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false);
			assert result.size()>-1;
		}
	}

	/**
	 * Test spatial knn query using index.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialKnnQueryUsingIndex() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		objectRDD.buildIndex(PointRDDIndexType,false);
		objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true);
			assert result.size()>-1;
		}
	}

	/**
	 * Test spatial join query.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialJoinQuery() throws Exception {
		queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

		objectRDD.spatialPartitioning(joinQueryPartitioningType);
		queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

		objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
		queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count();
			assert resultSize>0;
		}
	}

	/**
	 * Test spatial join query using index.
	 *
	 * @throws Exception the exception
	 */
	public static void testSpatialJoinQueryUsingIndex() throws Exception {
		queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

		objectRDD.spatialPartitioning(joinQueryPartitioningType);
		queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

		objectRDD.buildIndex(PointRDDIndexType,true);

		objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
		queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count();
			assert resultSize>0;
		}
	}

	/**
	 * Test spatial join query.
	 *
	 * @throws Exception the exception
	 */
	public static void testDistanceJoinQuery() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		CircleRDD queryWindowRDD = new CircleRDD(objectRDD,0.1);

		objectRDD.spatialPartitioning(GridType.QUADTREE);
		queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

		objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
		queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

		for(int i=0;i<eachQueryLoopTimes;i++)
		{

			long resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count();
			assert resultSize>0;
		}
	}

	/**
	 * Test spatial join query using index.
	 *
	 * @throws Exception the exception
	 */
	public static void testDistanceJoinQueryUsingIndex() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		CircleRDD queryWindowRDD = new CircleRDD(objectRDD,0.1);

		objectRDD.spatialPartitioning(GridType.QUADTREE);
		queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

		objectRDD.buildIndex(IndexType.RTREE,true);

		objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
		queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count();
			assert resultSize>0;
		}
	}


	/**
	 * Test CRS transformation spatial range query.
	 *
	 * @throws Exception the exception
	 */
	public static void testCRSTransformationSpatialRangeQuery() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY(),"epsg:4326","epsg:3005");
		objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count();
			assert resultSize>-1;
		}
	}



	/**
	 * Test CRS transformation spatial range query using index.
	 *
	 * @throws Exception the exception
	 */
	public static void testCRSTransformationSpatialRangeQueryUsingIndex() throws Exception {
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326","epsg:3005");
		objectRDD.buildIndex(PointRDDIndexType,false);
		objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
		for(int i=0;i<eachQueryLoopTimes;i++)
		{
			long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count();
			assert resultSize>-1;
		}

	}

	public static void testCRSTransformation()
	{
		objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
		objectRDD.CRSTransform("epsg:4326","epsg:3005");
		objectRDD.rawSpatialRDD.count();
	}

	public static void testLoadShapefileIntoPolygonRDD() throws Exception {
		PolygonRDD spatialRDD = ShapefileReader.readToPolygonRDD(sc, ShapeFileInputLocation);
		try {
			RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180,180,-90,90), false, false).count();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}