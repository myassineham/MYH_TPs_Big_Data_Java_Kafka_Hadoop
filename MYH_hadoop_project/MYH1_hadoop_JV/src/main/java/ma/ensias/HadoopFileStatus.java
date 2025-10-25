package ma.ensias;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            Path nomcomplet = new Path("/user/root/input", "purchases.txt");
            FileStatus infos = fs.getFileStatus(nomcomplet);

            // Changed 'status' to 'infos' (you defined it as 'infos' but used 'status')
            System.out.println(Long.toString(infos.getLen()) + " octets");
            System.out.println("File Name: " + infos.getPath().getName());
            System.out.println("File Size: " + infos.getLen());
            System.out.println("File Replication: " + infos.getReplication());
            System.out.println("File Block Size: " + infos.getBlockSize());

            // Fixed missing closing parenthesis
            fs.rename(nomcomplet, new Path("/user/root/hadoop_project", "achats.txt"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}