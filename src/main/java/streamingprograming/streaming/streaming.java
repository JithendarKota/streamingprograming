package streamingprograming.streaming;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import streamingprograming.Hbase.Hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class streaming {

   public streaming(){
	   
   }
	
        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("kafka-Spark").setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(100));
            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", "localhost:9092");
            HashSet<String> topicsSet = new HashSet<>();
            topicsSet.add("streamingdata.txt");

            JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
                    ssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    kafkaParams,
                    topicsSet
            );
            // Get the lines, split them into values

            JavaDStream<String> lines = kafkaStream.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

            JavaDStream<String> values = lines.map((Function<String, String>) x -> {
                System.out.println(x);
                String[] tempArray = x.split(",");

                for (String aTempArray : tempArray) {
                    System.out.println(aTempArray);
                }

                saveToHBbase(tempArray);

                return tempArray[8];

            });

            values.print();
            ssc.start();
            ssc.awaitTermination();
        }
        private static void saveToHBbase(String[] values) throws IOException {
            String tableName = "sensor";
            Admin admin = ConnectionFactory.createConnection().getAdmin();
            Hbase utils = new Hbase();

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
            	  utils.createTable(tableName);
            	  } else if (admin.isTableAvailable(TableName.valueOf(tableName))) {
                utils.insertData(values, tableName);
            }
    }
    
}
