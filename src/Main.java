import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class Main {
	
	public static void main(String[] args) throws Exception {
		
		Connection dbConnection = null;
		Statement dbStatement = null;
		ResultSet resultSet = null;
		ArrayList<String> dataRow = new ArrayList<String>();

        String csvFile = "bitfinex.csv";
        FileWriter writer = new FileWriter(csvFile);
        
        ArrayList<String> headers = new ArrayList<String>();
        headers.add("entry_id");
        headers.add("timestamp");
        headers.add("bid_size");
        headers.add("bid_price");
        headers.add("ask_price");
        headers.add("ask_size");
        headers.add("trade_type");
        
        CSVWriter.writeLine(writer, headers);
        
        // registering JDBC driver
     	Class.forName("com.ibm.db2.jcc.DB2Driver");

     	// creating DB connection
     	dbConnection = DriverManager
     					.getConnection("jdbc:db2://hostname:port/"
     									+ "databaseName:user=username;password=password;");
     	
     	dbStatement = dbConnection.createStatement();
     	
     	resultSet = dbStatement.executeQuery("SELECT DISTINCT * FROM bitfinex ORDER BY timestamp");
     	
     	System.out.println("Copying...");
     	
     	long counter = 0;
     	
     	while (resultSet.next()) {
     		
     		counter++;
     		
     		dataRow.add(String.valueOf(resultSet.getLong("entry_id")));
     		dataRow.add(String.valueOf(resultSet.getLong("timestamp")));
     		dataRow.add(String.valueOf(resultSet.getBigDecimal("bid_size")));
     		dataRow.add(String.valueOf(resultSet.getBigDecimal("bid_price")));
     		dataRow.add(String.valueOf(resultSet.getBigDecimal("ask_price")));
     		dataRow.add(String.valueOf(resultSet.getBigDecimal("ask_size")));
     		
     		if( resultSet.getString("trade_type") == null ) {
     			dataRow.add("null");
     		} else {
     			dataRow.add(resultSet.getString("trade_type"));
     		}
     		
     		CSVWriter.writeLine(writer, dataRow);
     		dataRow.clear();
     	}
     	
     	System.out.println(counter + " entries copied to csv file.");
        
        writer.flush();
        writer.close();
	}
}
