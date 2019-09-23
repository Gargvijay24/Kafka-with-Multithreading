import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;


public class KwMultiT {
	
	static String topicName = "";
	static Properties props ;
	
	final static File folder = new File("C:\\Self\\Softwares\\KafkaRequirement");
	
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		// TODO Auto-generated method stub
		
		topicName = "KafkaWThread";
	    // create instance for properties to access producer configs   
	    props = new Properties();
	    //Assign localhost id
	    props.put("bootstrap.servers", "localhost:9092");
	    //Set acknowledgements for producer requests.      
	    props.put("acks", "all");
	    //If the request fails, the producer can automatically retry,
	    props.put("retries", 0);
	    //Specify buffer size in config
	    props.put("batch.size", 16384);
	    //Reduce the no of requests less than 0   
	    props.put("linger.ms", 1);
	    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	    props.put("buffer.memory", 33554432);
	    
	    props.put("key.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
	       
	    props.put("value.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
		
		//Set Folder Path
		final File file = new File("C:/Self/Softwares/KafkaRequirement");
		//List all available files on given path
		File[] files = file.listFiles();
		//Count number of files
		int i = files.length;
		
		//Set number of threads equivalent to number of file
		final ExecutorService executorService = Executors.newFixedThreadPool(i);
		
        for(final File f: files){            
            executorService.submit(new Callable<String>() {
    	        public String call() throws Exception {
    	        	return expensiveMethod(file.toString() + "\\" + f.getName());
    	        }
    	    });
            
            //Move files to archive folder after processing
            Path temp = Files.move 
                    (Paths.get(file.toString() + "\\" + f.getName()),  
                    Paths.get("C:\\Users\\Vijay\\Archive\\" + f.getName())); 
              
                    if(temp != null) 
                    { 
                        System.out.println("File moved successfully"); 
                    } 
                    else
                    { 
                        System.out.println("Failed to move the file"); 
                    } 
        }
	}

	private static String expensiveMethod(String param) throws JSONException {
		
		BufferedReader br = null;
        String line = "";
        
        int cnt = 0;
        String[] topRow = new String[0];
        String data = "";
        String[] jsonData;
        
        try {
        	
        	//Read the file and arrange contents in proper order so it can be serialized in Json
        	JSONObject jObject = new JSONObject();
        	
            br = new BufferedReader(new FileReader(param));
            Producer<String, JSONObject> producer = new KafkaProducer
      		         <String, JSONObject>(props);
            while ((line = br.readLine()) != null) {
            	if(cnt==0){
                    String[] l = line.split(",");
                    topRow = new String[l.length];
                    
                    for(int i= 0; i<l.length-1; i++){
                        topRow[i] = l[i];
                    }
                 }
                 else{
                    String[] l = line.split(",");
                    for(int i= 0; i<Math.min(l.length-1, topRow.length+1); i++){
                        if(!l[i].equals("")){
                            String row = "";
                            //row = l[0];
                            row = row + " " + topRow[i];
                            row = row + " " + l[i];
                            jObject.put(topRow[i], l[i]);
                      
                            if(data.equals(""))data = row;
                            else 
                            	data = data + "\n" + row;      
                         }
                      }
                 }
                 cnt++;
                                
                //send Json object to Kafka server
                producer.send(new ProducerRecord<String, JSONObject>(topicName, 
                		jObject));
            }
            

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
		return null;
	    // Do work and return result
	}

}
