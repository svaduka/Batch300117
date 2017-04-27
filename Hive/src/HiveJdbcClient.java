import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
 
public class HiveJdbcClient {
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
 
  public static void main(String[] args) throws SQLException {
    try {
      Class.forName(driverName);
     System.out.println("try block");
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    if(con!=null){
    	System.out.println("Connected to hive");
    }
    Statement stmt = con.createStatement();
    String tableName = "test";
    System.out.println("Executing drop table :"+tableName);
    stmt.executeQuery("drop table if exists " + tableName);
    System.out.println("Executing create table :"+tableName);
    ResultSet res = stmt.executeQuery("create table if not exists " + tableName + " (key int, value string)");
    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
 
   
  }
}

