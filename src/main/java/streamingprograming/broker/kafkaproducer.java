package streamingprograming.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class kafkaproducer {

	public static void main(String[] args) throws IOException{
		
		String topic = "sensordata";
		Properties properties = new Properties();
       // properties.put("metadata.broker.list", "localhost:9092");
       // properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        File file = new File("./data/streamingdata.txt");
        
        InputStreamReader fr = new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8"));
        BufferedReader read = new BufferedReader(fr);
        String line = read.readLine();

        while (line != null) {
            String[] temp = line.split(",");
            String key = temp[0];
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, line);
            System.out.println("Send topic " + topic);
            producer.send(record);
            System.out.println("Sent message " + line);
            line = read.readLine();
        }

        producer.close();
        System.out.println("The End.");
        System.exit(1);

	}
}
