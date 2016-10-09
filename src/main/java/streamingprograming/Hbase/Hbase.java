package streamingprograming.Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.commons.logging.Log;



public class Hbase {
    private static final Log LOGGER = LogFactory.getLog(Hbase.class);
    public Hbase(){
        
    }
    
    
    public void createTable(String tableName) throws IOException {
        Admin admin = ConnectionFactory.createConnection().getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor("col1"));
        tableDescriptor.addFamily(new HColumnDescriptor("col2"));
        tableDescriptor.addFamily(new HColumnDescriptor("col3"));
        LOGGER.debug("Creating Table");
        admin.createTable(tableDescriptor);
        LOGGER.debug("Table Created");
        admin.close();
    }

  
    
    
      
         /*HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
        HTableDescriptor htable = new HTableDescriptor("User"); 
        htable.addFamily( new HColumnDescriptor("Id"));
        htable.addFamily( new HColumnDescriptor("Name"));
        
        System.out.println( "Connecting..." );
        HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
        System.out.println( "Creating Table..." );
        hbase_admin.createTable( htable );
        System.out.println("Done!");
       
      }*/
    	
	
     public void insertData(String[] lines, String tableName) throws IOException {
            Table table = ConnectionFactory.createConnection().getTable(TableName.valueOf(tableName));
            
            Put put = new Put(Bytes.toBytes(lines[0] + "_" + lines[1] + " " + lines[2]));
            
            put.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("cell1"), Bytes.toBytes(lines[0]));
            put.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("cell2"), Bytes.toBytes(lines[1]));
            put.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("cell3"), Bytes.toBytes(lines[2]));
            put.addColumn(Bytes.toBytes("col2"), Bytes.toBytes("cell4"), Bytes.toBytes(lines[3]));
            put.addColumn(Bytes.toBytes("col2"), Bytes.toBytes("cell5"), Bytes.toBytes(lines[4]));
            put.addColumn(Bytes.toBytes("col2"), Bytes.toBytes("cell6"), Bytes.toBytes(lines[5]));
            put.addColumn(Bytes.toBytes("col3"), Bytes.toBytes("cell7"), Bytes.toBytes(lines[6]));

            table.put(put);
            System.out.println("data inserted");
            // closing HTable
            table.close();
        }
}
