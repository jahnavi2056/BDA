import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class hdfsUploader {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);

        Path local = new Path("input/queries.txt");
        Path hdfs = new Path("/input/queries.txt");

        fs.copyFromLocalFile(local, hdfs);
        System.out.println("File uploaded to HDFS");
        fs.close();
    }
}
