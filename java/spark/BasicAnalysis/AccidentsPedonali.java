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
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class IncidentiPedonali implements java.io.Serializable{

	private static final long serialVersionUID = 1L;

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
				"mongodb://localhost:27017/Incidenti_new.incidenti");

		mongodbConfig2.set("mongo.input.uri",
				"mongodb://localhost:27017/Incidenti_new.pedoni");

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> incidenti = sc.newAPIHadoopRDD(
				mongodbConfig,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> pedoni = sc.newAPIHadoopRDD(
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

		JavaPairRDD<String, BSONObject> pedoni2 = pedoni.flatMapToPair(
				t-> {
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					temp.add(new Tuple2<String,BSONObject>(t._2.get("IDProtocollo").toString(),t._2));
					return temp;
				});

		JavaPairRDD<String, Tuple2<BSONObject, BSONObject>> incidenti2pedoni = incidenti2.join(pedoni2);

		JavaPairRDD<String, Integer> municipio_segnaletica = incidenti2pedoni.flatMapToPair(
				t-> {
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					String key = t._2._1.get("Gruppo").toString()+"_"+t._2._1.get("Segnaletica").toString();
					temp.add(new Tuple2<String,Integer>(key,1));
					return temp;
				});

		JavaPairRDD<String, Integer> municipio_visibilita = incidenti2pedoni.flatMapToPair(
				t-> {
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					String key = t._2._1.get("Gruppo").toString()+"_"+t._2._1.get("Visibilita").toString();
					temp.add(new Tuple2<String,Integer>(key,1));
					return temp;
				});

		JavaPairRDD<String, Integer> municipio_illuminazione = incidenti2pedoni.flatMapToPair(
				t-> {
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					String key = t._2._1.get("Gruppo").toString()+"_"+t._2._1.get("Illuminazione").toString();
					temp.add(new Tuple2<String,Integer>(key,1));
					return temp;
				});


		JavaPairRDD<String, Integer> municipioSegnaletica_counts = municipio_segnaletica.reduceByKey(
				(a, b) -> a + b
				);

		JavaPairRDD<String, Integer> municipioIlluminazione_counts = municipio_illuminazione.reduceByKey(
				(a, b) -> a + b
				);

		JavaPairRDD<String, Integer> municipioVisibilita_counts = municipio_visibilita.reduceByKey(
				(a, b) -> a + b
				);



		JavaPairRDD<String, String> municipio_countSegnaletica = municipioSegnaletica_counts.flatMapToPair(
				t -> {
					List<Tuple2<String, String>> temp = new ArrayList<Tuple2<String, String>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String municipio = "";
					String segnaletica = "";
					if (tokenizer.hasMoreTokens()) {
						municipio = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						segnaletica = tokenizer.nextToken();
					}
					int count = t._2;
					temp.add(new Tuple2<String,String>(municipio,segnaletica+"_"+count));
					return temp;
				});

		JavaPairRDD<String, Iterable<String>> municipio_segnaleticaIterable = municipio_countSegnaletica.groupByKey();

		JavaPairRDD<String, String> municipio_countVisibilita = municipioVisibilita_counts.flatMapToPair(
				t -> {
					List<Tuple2<String, String>> temp = new ArrayList<Tuple2<String, String>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String municipio = "";
					String visibilita = "";
					if (tokenizer.hasMoreTokens()) {
						municipio = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						visibilita = tokenizer.nextToken();
					}
					int count = t._2;
					temp.add(new Tuple2<String,String>(municipio,visibilita+"_"+count));
					return temp;
				});

		JavaPairRDD<String, Iterable<String>> municipio_visibilitaIterable = municipio_countVisibilita.groupByKey();

		JavaPairRDD<String, String> municipio_countIlluminazione = municipioIlluminazione_counts.flatMapToPair(
				t -> {
					List<Tuple2<String, String>> temp = new ArrayList<Tuple2<String, String>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String municipio = "";
					String illuminazione = "";
					if (tokenizer.hasMoreTokens()) {
						municipio = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						illuminazione = tokenizer.nextToken();
					}
					int count = t._2;
					temp.add(new Tuple2<String,String>(municipio,illuminazione+"_"+count));
					return temp;
				});

		JavaPairRDD<String, Iterable<String>> municipio_illuminazioneIterable = municipio_countIlluminazione.groupByKey();

		JavaPairRDD<String, Tuple2<Tuple2<Iterable<String>, Iterable<String>>, Iterable<String>>> municipioJoin = municipio_illuminazioneIterable.join(municipio_visibilitaIterable).join(municipio_segnaleticaIterable);

		JavaPairRDD<Object, BSONObject> fin = municipioJoin.flatMapToPair(
				t -> {
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					BSONObject bo = new BasicBSONObject();
					BSONObject info = new BasicBSONObject();

					String municipio = t._1;
					Iterator<String> illuminazioneIterable = t._2._1._1.iterator();
					Iterator<String> visibilitaIterable = t._2._1._2.iterator();
					Iterator<String> segnaleticaIterable = t._2._2.iterator();

					BSONObject segnaleticaJson = new BasicBSONObject();
					BSONObject visibilitaJson = new BasicBSONObject();
					BSONObject illuminazioneJson = new BasicBSONObject();

					String key = "";
					String value = "";
					while (segnaleticaIterable.hasNext()){
						StringTokenizer tokenizer = new StringTokenizer(segnaleticaIterable.next(),"_");
						if (tokenizer.hasMoreTokens()) {
							key = tokenizer.nextToken();
							if (tokenizer.hasMoreTokens()) {
								value = tokenizer.nextToken();
								segnaleticaJson.put(key, Integer.parseInt(value));
							}
						}
					}

					while (visibilitaIterable.hasNext()){
						StringTokenizer tokenizer = new StringTokenizer(visibilitaIterable.next(),"_");

						if (tokenizer.hasMoreTokens()) {
							key = tokenizer.nextToken();
							if (tokenizer.hasMoreTokens()) {
								value = tokenizer.nextToken();
								visibilitaJson.put(key, Integer.parseInt(value));
							}
						}
					}

					while (illuminazioneIterable.hasNext()){
						StringTokenizer tokenizer = new StringTokenizer(illuminazioneIterable.next(),"_");

						if (tokenizer.hasMoreTokens()) {
							key = tokenizer.nextToken();
							if (tokenizer.hasMoreTokens()) {
								value = tokenizer.nextToken();
								illuminazioneJson.put(key, Integer.parseInt(value));
							}
						}
					}

					info.put("Segnaletica", segnaleticaJson);
					info.put("Visibilita", visibilitaJson);
					info.put("Illuminazione", illuminazioneJson);
					bo.put("Municipio", municipio);
					bo.put("info", info);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/Incidenti_new.incidenti_pedonali");

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
		new IncidentiPedonali().run();
	}
}
