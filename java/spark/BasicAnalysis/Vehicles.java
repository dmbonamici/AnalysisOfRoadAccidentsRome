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
import java.util.StringTokenizer;

public class Vehicles implements java.io.Serializable{

	private static final long serialVersionUID = 1L;

	public void run() {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("AccidentRome"));
		// Set configuration options for the MongoDB Hadoop Connector.
		Configuration mongodbConfig = new Configuration();
		Configuration mongodbConfig2 = new Configuration();
		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		mongodbConfig.set("mongo.input.uri",
				"mongodb://localhost:27017/incidenti.incidenti");

		mongodbConfig2.set("mongo.input.uri",
				"mongodb://localhost:27017/incidenti.veicoli");

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> incidenti = sc.newAPIHadoopRDD(
				mongodbConfig,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> veicoli = sc.newAPIHadoopRDD(
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

		JavaPairRDD<String, BSONObject> veicoli2 = veicoli.flatMapToPair(
				t-> {
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					temp.add(new Tuple2<String,BSONObject>(t._2.get("IDProtocollo").toString(),t._2));
					return temp;
				});

		JavaPairRDD<String, Tuple2<BSONObject, BSONObject>> incidenti2veicoli = incidenti2.join(veicoli2);

		JavaPairRDD<String, Integer> tripla = incidenti2veicoli.flatMapToPair(
				t-> {
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					String key = t._2._2.get("Marca").toString()+"_"+t._2._1.get("NaturaIncidente").toString()+"_"+t._2._1.get("FondoStradale").toString();
					temp.add(new Tuple2<String,Integer>(key,1));
					return temp;
				});

		JavaPairRDD<String, Integer> triplaReduce = tripla.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> pair = triplaReduce.flatMapToPair(
				t->{
					String tipoMacchina = "", tipoIncidente = "", fondoStradale = ""; 
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					if (tokenizer.hasMoreTokens()) {
						tipoMacchina = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						tipoIncidente = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						fondoStradale = tokenizer.nextToken();
					}
					String key = tipoMacchina+"_"+tipoIncidente;
					bo.put("FondoStradale",fondoStradale);
					bo.put("NumIncidenti", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>>pairGroupBy = pair.groupByKey();

		JavaPairRDD<String, BSONObject> single = pairGroupBy.flatMapToPair(
				t->{
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String tipoIncidente = "", tipoMacchina = "";
					if (tokenizer.hasMoreTokens()) {
						tipoMacchina = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						tipoIncidente = tokenizer.nextToken();
					}
					bo.put("TipoIncidente", tipoIncidente);
					bo.put("Condizioni", t._2);
					temp.add(new Tuple2<String,BSONObject>(tipoMacchina,bo));
					return temp;
				});
		
		JavaPairRDD<String, Iterable<BSONObject>>singleGroupBy = single.groupByKey();
		
		JavaPairRDD<Object, BSONObject> finale = singleGroupBy.flatMapToPair(
				t -> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<Object, BSONObject>> temp = new ArrayList<Tuple2<Object, BSONObject>>();
					bo.put("MarcaMacchina", t._1);
					bo.put("Incidenti", t._2);
					temp.add(new Tuple2<Object,BSONObject>(null,bo));
					return temp;
				});

		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				"mongodb://localhost:27017/incidenti.output3");

		// Save this RDD as a Hadoop "file".
		// The path argument is unused; all documents will go to 'mongo.output.uri'.
		finale.saveAsNewAPIHadoopFile(
				"file:///this-is-completely-unused",
				Object.class,
				BSONObject.class,
				MongoOutputFormat.class,
				outputConfig
				);
	}

	public static void main(final String[] args) {
		new Vehicles().run();
	}
}
