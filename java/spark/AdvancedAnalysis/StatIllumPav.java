
import com.mongodb.hadoop.MongoInputFormat;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.bson.BSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class StatIllumPav {

	private static final String FASCIA1 = "18-25", FASCIA2 = "26-39", FASCIA3 = "40-69", FASCIA4 = "71-99", FASCIA5 ="non-identificato";
	private static final int ANNO_CORRENTE = 2016;
	private static final String[] ASFALTATA = {"Asfaltata", "In conglomerato cementizio","Bitumata"}, DISSESTATA = {"Con buche", "Strada pavimentata dissestata"}, LASTRICATA = {"Lastricata", "In cubetti di porfido", "Acciotolata"}, STERRATA = {"Sterrata", "Inghiaiata", "Fondo naturale"};
	
	public void run() {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("AccidentRome"));

		Map<String, Double> incid2freq = new HashMap<String, Double>();
		try(BufferedReader br = new BufferedReader(new FileReader("/Users/Francesco/Desktop/ill_pav.txt"))){
			String currentLine="";
			while ((currentLine = br.readLine()) != null) {
				StringTokenizer tokenizerFile = new StringTokenizer(currentLine,",");
				String incid = tokenizerFile.nextToken();
				Double freq = Double.valueOf(tokenizerFile.nextToken());
				incid2freq.put(incid, freq);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 

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

		JavaRDD<List<String>> transactions = incidenti.map(
				t -> {
					List<String> parts = new ArrayList<String>();
					Set<String> set = new HashSet<String>();
					String street = ((String)t._2.get("Pavimentazione")).toUpperCase();
					if (Arrays.asList(ASFALTATA).contains((String)t._2.get("Pavimentazione"))){
						street = "Asfaltata";
					}
					else  if (Arrays.asList(DISSESTATA).contains((String)t._2.get("Pavimentazione"))){
						street = "Dissestata";
					}
					else  if (Arrays.asList(LASTRICATA).contains((String)t._2.get("Pavimentazione"))){
						street = "Lastricata";
					}
					else  if (Arrays.asList(STERRATA).contains((String)t._2.get("Pavimentazione"))){
						street = "Sterrata";
					}
					set.add(street);
					set.add((String)t._2.get("Illuminazione"));
					set.add((String)t._2.get("NaturaIncidente"));
					parts.addAll(set);
					return parts;

				});

		JavaRDD<List<String>> transactions2 = incidenti2persone.map(
				t -> {
					List<String> parts = new ArrayList<String>();
					int annoNascita = 0; 
					String fasciaEta = FASCIA5;
					Set<String> set = new HashSet<String>();
					//					set.add((String)t._2._1.get("Gruppo"));
					set.add((String)t._2._1.get("NaturaIncidente"));
					//					set.add((String)t._2._2.get("Sesso"));
					String tipoPersona = t._2._2.get("TipoPersona").toString();
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
					}
					//					set.add(fasciaEta);
					set.add("Pippo");
					parts.addAll(set);
					return parts;
				});

		FPGrowth fpg = new FPGrowth().setMinSupport(0);

		FPGrowthModel<String> model = fpg.run(transactions);

		//		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
		//			if (itemset.javaItems().size() >= 3)
		//				System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		//		}

		double minConfidence = 0;
		for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			if(!rule.javaAntecedent().contains("") && rule.javaAntecedent().size() >= 2)
				System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + 1/(Math.abs(Math.log10(rule.confidence()))));
		}
		sc.stop();
	}

	public static void main(final String[] args) {
		new StatIllumPav().run();
	}
}
