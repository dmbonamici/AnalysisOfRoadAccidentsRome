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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class IncidentiFasce implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	private static final String FASCIA1 = "18-25", FASCIA2 = "26-39", FASCIA3 = "40-69", FASCIA4 = "71-99", FASCIA5 ="non-identificato";
	private static final int ANNO_CORRENTE = 2016;

	public void run() {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("AccidentRome"));

		// Set configuration options for the MongoDB Hadoop Connector.
		Configuration mongodbConfig = new Configuration();
		Configuration mongodbConfig2 = new Configuration();

		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		mongodbConfig2.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		mongodbConfig.set("mongo.input.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti");

		mongodbConfig2.set("mongo.input.uri",
				"mongodb://localhost:27017/incidenti_new.persone");

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> incidenti = sc.newAPIHadoopRDD(
				mongodbConfig,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> persone = sc.newAPIHadoopRDD(
				mongodbConfig2,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);

		JavaPairRDD<String, BSONObject> incidenti2 = incidenti.flatMapToPair(
				t-> {
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					temp.add(new Tuple2<String,BSONObject>(t._2.get("ID").toString(),t._2));
					return temp;
				});

		JavaPairRDD<String, BSONObject> persone2 = persone.flatMapToPair(
				t-> {
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					temp.add(new Tuple2<String,BSONObject>(t._2.get("IDProtocollo").toString(),t._2));
					return temp;
				});

		JavaPairRDD<String, Tuple2<BSONObject, BSONObject>> incidenti2persone = incidenti2.join(persone2);



		JavaPairRDD<String, Integer> tripla = incidenti2persone.flatMapToPair(
				t-> {
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					String gruppo = t._2._1.get("Gruppo").toString();
					String fasciaEta = FASCIA5;
					int annoNascita = 0;
					String tipoPersona = t._2._2.get("TipoPersona").toString();
					String sesso = t._2._2.get("Sesso").toString();
					if (t._2._2.get("AnnoNascita") != null && !t._2._2.get("AnnoNascita").toString().equals("")) {
						annoNascita = Integer.valueOf((String) t._2._2.get("AnnoNascita"));
					}
					if (tipoPersona.equals("Conducente")) {
						if (ANNO_CORRENTE - annoNascita < 26){
							fasciaEta = FASCIA1;
						}
						else if (ANNO_CORRENTE - annoNascita < 40) {
							fasciaEta = FASCIA2;
						}
						else if (ANNO_CORRENTE - annoNascita < 70) {
							fasciaEta = FASCIA3;
						}
						else if (ANNO_CORRENTE - annoNascita < 100){
							fasciaEta = FASCIA4;
						}
						String key = gruppo+"_"+fasciaEta+"_"+sesso;
						temp.add(new Tuple2<String,Integer>(key,1));
					}
					return temp;
				});

		JavaPairRDD<String, Integer> triplaReduce = tripla.reduceByKey(
				(a, b) -> a + b);
		
		JavaPairRDD<String, BSONObject> doppia = triplaReduce.flatMapToPair(
				t->{
					String gruppo = "", fasciaEta = "", sesso = "";
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					if (tokenizer.hasMoreTokens()) {
						gruppo = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						fasciaEta = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						sesso = tokenizer.nextToken();
					}
					String key = gruppo+"_"+fasciaEta;
					bo.put("Sesso", sesso);
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>>doppiaGroupBy = doppia.groupByKey();
		
		JavaPairRDD<String, Iterable<BSONObject>> doppiaOrdered = doppiaGroupBy.flatMapToPair(
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

		
		JavaPairRDD<String, BSONObject> single = doppiaOrdered.flatMapToPair(
				t->{
					String gruppo = "", fasciaEta = "";
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					if (tokenizer.hasMoreTokens()) {
						gruppo = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						fasciaEta = tokenizer.nextToken();
					}
					String key = gruppo;
					bo.put("FasciaEta", fasciaEta);
					bo.put("Info", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>>singleGroupBy = single.groupByKey();

		JavaPairRDD<Object, BSONObject> fin = singleGroupBy.flatMapToPair(
				t -> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					bo.put("Municipio", t._1);
					bo.put("info", t._2);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/incidenti_new.incidenti_fasce_sesso");

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
		new IncidentiFasce().run();
	}
}
