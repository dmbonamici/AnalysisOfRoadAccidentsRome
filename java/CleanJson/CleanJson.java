 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CleanJson {

	public static void main(String[] args) throws JsonProcessingException, IOException {

		//INSERT THE PATH OF THE DIRECTORY FOR CLEAN AND CREATE NEW JSON
		File dir = new File("YOURPATH");
		displayDirectoryContents(dir);

	}

	public static void displayDirectoryContents(File dir) {
		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println("directory:" + file.getCanonicalPath());
					displayDirectoryContents(file);
				} else {
					String extension = file.getPath().substring(file.getPath().lastIndexOf('.')+1, file.getPath().length());
					if (extension.contains("json") || extension.contains("txt")) {
						processJson (file.getCanonicalPath());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void processJson (String pathReader) {

		String pathWriter = "YOURPATH";
		String nameWriter = pathReader.substring(pathReader.lastIndexOf('/')+1,pathReader.length()).toLowerCase();
		String parentWriter = new File (pathReader).getParent().substring(new File (pathReader).getParent().lastIndexOf("/"));
		parentWriter = parentWriter.substring(parentWriter.indexOf("_")); //ricorda concatenare
		if (nameWriter.contains("incidenti")) {
			pathWriter = pathWriter.concat("incidenti"+parentWriter+".json");
		}
		else if (nameWriter.contains("veicoli")) {
			pathWriter = pathWriter.concat("veicoli"+parentWriter+".json");
		}
		else if (nameWriter.contains("pedoni")) {
			pathWriter = pathWriter.concat("pedoni"+parentWriter+".json");
		}
		else if (nameWriter.contains("persone")) {
			pathWriter = pathWriter.concat("persone"+parentWriter+".json");
		}
		System.out.println(pathWriter);


		//		deleteJson(pathWriter);
		//		cleanJson(pathReader, pathWriter);
		//		provaJson(pathWriter);

	}

	public static void deleteJson (String path) {
		boolean deleted = new File(path).delete();
		System.out.println("DELETED "+deleted);
	}

	public static void cleanJson (String pathReader,String pathWriter) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(pathReader));
			bw = new BufferedWriter(new FileWriter(pathWriter));
			while ((sCurrentLine = br.readLine()) != null) {
				String pattern = ":\"[^\"]*\"([^\"]*\")?[^\"]*\"[,}]";
				String pattern2 = ":\"([^\"},]*\"[^\"},]*)+\"[,}]";
				String sub = sCurrentLine;
				Pattern r = Pattern.compile(pattern);
				Matcher m = r.matcher(sub);
				
				while (m.find()) {
					String oldString = m.group().substring(m.group().indexOf("\"")+1,m.group().lastIndexOf("\""));
					String newString = m.group().substring(m.group().indexOf("\"")+1,m.group().lastIndexOf("\"")).replace("\"", "");
					sub = sub.replace(oldString, newString);
					m = r.matcher(sub);
				}

				r = Pattern.compile(pattern2);
				m = r.matcher(sub);

				while (m.find()) {
					String oldString = m.group().substring(m.group().indexOf("\"")+1,m.group().lastIndexOf("\""));
					String newString = m.group().substring(m.group().indexOf("\"")+1,m.group().lastIndexOf("\"")).replace("\"", "");
					sub = sub.replace(oldString, newString);
					m = r.matcher(sub);
				}

				sub = sub.replace("\\", "").trim().concat("\n");
				sub = sub.replace("},", "}");
				if (sub.contains("ID")) {
					bw.write(sub);
				}
			}
			System.out.println("JSON CREATE");
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			try {
				if (br != null)
					br.close();
				if (bw != null) {
					bw.close();
				}
				provaJson(pathWriter);
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

	public static void provaJson (String path) {
		try {
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			JsonNode rootNode;

			rootNode = mapper.readTree(new File(path));

			//			JsonNode array = rootNode.findValue("Incidenti");
			//			int i=0;
			//
			//			while (array.has(i)) {
			//				System.out.println(array.get(i).toString());
			//				i++;
			//			}
			System.out.println("JSON VALIDATED");
		} catch (JsonParseException e) {
			e.printStackTrace();

		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

