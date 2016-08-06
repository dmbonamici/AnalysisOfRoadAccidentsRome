package it.uniroma3.bigdata;

import com.mongodb.hadoop.MongoInputFormat;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class IncidentiCond implements java.io.Serializable{

	private static final String[] ASFALTATA = {"Asfaltata", "In conglomerato cementizio","Bitumata"}, DISSESTATA = {"Con buche", "Strada pavimentata dissestata"}, LASTRICATA = {"Lastricata", "In cubetti di porfido", "Acciotolata"}, STERRATA = {"Sterrata", "Inghiaiata", "Fondo naturale"};
	
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

		JavaPairRDD<String, Integer> munPav = documents.flatMapToPair(
				t-> {
					String group = ((String)t._2.get("Gruppo")).trim();
					String street = "";
					if (Arrays.asList(ASFALTATA).contains(((String)t._2.get("Pavimentazione")))){
						street = "Asfaltata";
					}
					else  if (Arrays.asList(DISSESTATA).contains(((String)t._2.get("Pavimentazione")))){
						street = "Dissestata";
					}
					else  if (Arrays.asList(LASTRICATA).contains(((String)t._2.get("Pavimentazione")))){
						street = "Lastricata";
					}
					else  if (Arrays.asList(STERRATA).contains(((String)t._2.get("Pavimentazione")))){
						street = "Sterrata";
					}
					String key = group+"_"+street;
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && street != null && group.length() > 0 && street.length() > 0) {
						temp.add(new Tuple2<String,Integer>(key,1));
					}

					return temp;
				}
				);

		JavaPairRDD<String, Integer> munPavRBK = munPav.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> munPav2 = munPavRBK.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizerFile = new StringTokenizer(t._1,"_");
					String key = tokenizerFile.nextToken();
					bo.put("Tipo",tokenizerFile.nextToken());
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> munPav2GBK = munPav2.groupByKey();
		
		JavaPairRDD<String, Integer> munCond = documents.flatMapToPair(
				t-> {
					String group = ((String)t._2.get("Gruppo")).trim();
					String segnaletica = ((String)t._2.get("Segnaletica")).trim();
					String key = group+"_"+segnaletica;
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && segnaletica != null && group.length() > 0 && segnaletica.length() > 0) {
						temp.add(new Tuple2<String,Integer>(key,1));
					}

					return temp;
				}
				);

		JavaPairRDD<String, Integer> munCondRBK = munCond.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> munCond2 = munCondRBK.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizerFile = new StringTokenizer(t._1,"_");
					String key = tokenizerFile.nextToken();
					bo.put("Tipo",tokenizerFile.nextToken());
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> munCond2GBK = munCond2.groupByKey();
		
		JavaPairRDD<String, Integer> munTraf = documents.flatMapToPair(
				t-> {
					String group = ((String)t._2.get("Gruppo")).trim();
					String traffico = ((String)t._2.get("Traffico")).trim();
					String key = group+"_"+traffico;
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && traffico != null && group.length() > 0 && traffico.length() > 0) {
						temp.add(new Tuple2<String,Integer>(key,1));
					}

					return temp;
				}
				);

		JavaPairRDD<String, Integer> munTrafRBK = munTraf.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> munTraf2 = munTrafRBK.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizerFile = new StringTokenizer(t._1,"_");
					String key = tokenizerFile.nextToken();
					bo.put("Intensita",tokenizerFile.nextToken());
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> munTraf2GBK = munTraf2.groupByKey();
		
		JavaPairRDD<String, Tuple2<Tuple2<Iterable<BSONObject>, Iterable<BSONObject>>, Iterable<BSONObject>>> munJoin = munPav2GBK.join(munTraf2GBK).join(munCond2GBK);
		
		JavaPairRDD<Object, BSONObject> munInfo = munJoin.flatMapToPair(
				t -> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					bo.put("Gruppo", t._1);
					bo.put("Pavimentazione", t._2._1._1);
					bo.put("Traffico", t._2._1._2);
					bo.put("Segnaletica", t._2._2);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti_condizioni");

		// Save this RDD as a Hadoop "file".
		// The path argument is unused; all documents will go to 'mongo.output.uri'.
		munInfo.saveAsNewAPIHadoopFile(
				"file:///this-is-completely-unused",
				Object.class,
				BSONObject.class,
				MongoOutputFormat.class,
				outputConfig
				);
	}

	public static void main(final String[] args) {
		new IncidentiCond().run();
	}
}
