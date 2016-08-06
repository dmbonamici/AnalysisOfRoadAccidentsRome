import com.mongodb.hadoop.MongoInputFormat;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class AccidentCause implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	public void run() {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("AccidentRome"));
		// Set configuration options for the MongoDB Hadoop Connector.
		Configuration mongodbConfig = new Configuration();
		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		mongodbConfig.set("mongo.input.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti");

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
				mongodbConfig,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);

		JavaPairRDD<String, Integer> municipality = documents.flatMapToPair(
				t-> {
					String group = ((String)t._2.get("Gruppo")).trim();
					String incident = ((String)t._2.get("NaturaIncidente")).trim();
					String key = group+"_"+incident;
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && incident != null && group.length() > 0 && incident.length() > 0) {
						temp.add(new Tuple2<String,Integer>(key,1));
					}

					return temp;
				}
				);

		JavaPairRDD<String, Integer> counts = municipality.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> intermediate = counts.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizerFile = new StringTokenizer(t._1,"_");
					String key = tokenizerFile.nextToken();
					bo.put("TipoIncidente",tokenizerFile.nextToken());
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> intermediate2 = intermediate.groupByKey();
		
		JavaPairRDD<String, Iterable<BSONObject>> intermediate3 = intermediate2.flatMapToPair(
				t -> {	
					List<Tuple2<String,Iterable<BSONObject>>> temp = new ArrayList<Tuple2<String,Iterable<BSONObject>>>();
					Iterator<BSONObject> boNew = t._2.iterator();
					List<BSONObject> lbo = new ArrayList<BSONObject>();
					Iterable<BSONObject> newIterable;
					while (boNew.hasNext()){
						lbo.add(boNew.next());
					}
					lbo.sort(new Comparator<BSONObject>() {
						@Override
						public int compare(BSONObject bo1, BSONObject bo2) {
							return (int)bo2.get("Count") - (int)bo1.get("Count");
						}
					});	
					newIterable = lbo;
					temp.add(new Tuple2<String,Iterable<BSONObject>>(t._1,newIterable));
					return temp;
				});

		JavaPairRDD<Object, BSONObject> fin = intermediate3.flatMapToPair(
				t -> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					bo.put("Gruppo", t._1);
					bo.put("Info", t._2);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti_cause");

		// Save this RDD as a Hadoop "file".
		// The path argument is unused; all documents will go to 'mongo.output.uri'.
		fin.saveAsNewAPIHadoopFile(
				"file:///this-is-completely-unused",
				Object.class,
				BSONObject.class,
				MongoOutputFormat.class,
				outputConfig
				);
	}

	public static void main(final String[] args) {
		new AccidentCause().run();
	}
}
