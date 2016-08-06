import com.mongodb.hadoop.MongoInputFormat;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import java.text.SimpleDateFormat;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class IncidentiGiorni implements java.io.Serializable {

	private String[] weekDay = {"non-definito","Domenica","Lunedi", "Martedi", "Mercoledi", "Giovedi", "Venerdi", "Sabato"};
	private String[] fasceOrarie = {"0-6", "7-9", "10-13", "14-17", "18-20", "21-24", "non-definita"};
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

		JavaPairRDD<String, Integer> tripla = documents.flatMapToPair(
				t-> {
					String group = ((String)t._2.get("Gruppo")).trim();
					String dataOra = ((String)t._2.get("DataOraIncidente")).trim();
					Calendar c = GregorianCalendar.getInstance();
					int dayOfWeek = 0;
					int hour = 25;
					String fascia = fasceOrarie[6];
					try {
						c.setTime(new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(dataOra));
						dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
						hour = c.get(Calendar.HOUR_OF_DAY);
					}
					catch (java.text.ParseException e1){
						try {
							c.setTime(new SimpleDateFormat("dd/MM/yyyy").parse(dataOra));
							dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
						}
						catch(java.text.ParseException e2){
						}
					}
					if (hour < 6){
						fascia = fasceOrarie[0];
					}
					else if (hour < 9){
						fascia = fasceOrarie[1];
					}
					else if (hour < 13){
						fascia = fasceOrarie[2];
					}
					else if (hour < 17){
						fascia = fasceOrarie[3];
					}
					else if (hour < 20){
						fascia = fasceOrarie[4];
					}
					else if (hour < 24){
						fascia = fasceOrarie[5];
					}
					String key = group+"_"+weekDay[dayOfWeek]+"_"+fascia;
					List<Tuple2<String,Integer>> temp = new ArrayList<Tuple2<String,Integer>>();
					if (group != null && group.length() > 0) {
						temp.add(new Tuple2<String,Integer>(key,1));
					}
					return temp;
				}
				);

		JavaPairRDD<String, Integer> triplaReduce = tripla.reduceByKey(
				(a, b) -> a + b);

		JavaPairRDD<String, BSONObject> doppia = triplaReduce.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String group = "", dayOfWeek = "", hour = "";
					if (tokenizer.hasMoreTokens()) {
						group = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						dayOfWeek = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						hour = tokenizer.nextToken();
					}
					String key = group+"_"+dayOfWeek;
					bo.put("FasciaOraria",hour);
					bo.put("Count", t._2);
					temp.add(new Tuple2<String,BSONObject>(key,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> doppiaGroupBy = doppia.groupByKey();

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


		JavaPairRDD<String, BSONObject> singola = doppiaOrdered.flatMapToPair(
				t-> {
					BSONObject bo = new BasicBSONObject();
					List<Tuple2<String,BSONObject>> temp = new ArrayList<Tuple2<String,BSONObject>>();
					StringTokenizer tokenizer = new StringTokenizer(t._1,"_");
					String group = "", dayOfWeek = "";
					if (tokenizer.hasMoreTokens()) {
						group = tokenizer.nextToken();
					}
					if (tokenizer.hasMoreTokens()) {
						dayOfWeek = tokenizer.nextToken();
					}
					bo.put("Giorno", dayOfWeek);
					bo.put("Info", t._2);
					temp.add(new Tuple2<String,BSONObject>(group,bo));
					return temp;
				});

		JavaPairRDD<String, Iterable<BSONObject>> singolaGroupBy = singola.groupByKey();


		JavaPairRDD<Object, BSONObject> fin = singolaGroupBy.flatMapToPair(
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
				"mongodb://localhost:27017/incidenti_new.incidenti_giorno_ora");

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
		new IncidentiGiorni().run();
	}
}
