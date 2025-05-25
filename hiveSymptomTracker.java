import java.sql.*;

public class hiveSymptomTracker {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS symptoms (symptom STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
        stmt.execute("LOAD DATA INPATH '/output/part-r-00000' OVERWRITE INTO TABLE symptoms");
        ResultSet res = stmt.executeQuery("SELECT * FROM symptoms");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getInt(2));
        }
        con.close();
    }
}
