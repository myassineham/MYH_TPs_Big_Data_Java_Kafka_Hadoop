package ma.ensias;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar WriteHDFS.jar <chemin_fichier> <texte>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);

        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(args[1]);
            outStream.close();
            System.out.println("Fichier créé: " + args[0]);
        } else {
            System.out.println("Le fichier existe déjà!");
        }

        fs.close();
    }
}