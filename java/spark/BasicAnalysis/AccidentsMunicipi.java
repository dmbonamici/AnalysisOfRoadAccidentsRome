package it.uniroma3.bigdata;

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
import java.util.List;

public class IncidentiMunicipi {

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
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && group.length() > 0) {
						temp.add(new Tuple2<String,Integer>(group,1));
					}

					return temp;
				}
				);

		JavaPairRDD<String, Integer> counts = municipality.reduceByKey(
				(a, b) -> a + b
				);

		JavaPairRDD<Object, BSONObject> fin = counts.flatMapToPair(
				t -> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					bo.put("Municipio", t._1);
					bo.put("NumIncidenti", t._2);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

				// Create a separate Configuration for saving data back to MongoDB.
				Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti_municipi");

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
	new IncidentiMunicipi().run();
}
}
