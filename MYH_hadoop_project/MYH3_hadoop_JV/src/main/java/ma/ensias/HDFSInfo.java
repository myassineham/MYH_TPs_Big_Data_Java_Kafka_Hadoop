package ma.ensias;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

public class HDFSInfo {
    public static void main(String[] args) throws IOException {
        // Vérifier les paramètres
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar HDFSInfo.jar <nom_fichier>");
            System.err.println("Exemple: hadoop jar HDFSInfo.jar /user/root/input/achats.txt");
            System.exit(1);
        }

        // Obtenir un objet qui représente HDFS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Nom du fichier à lire (passé en paramètre)
        Path nomcomplet = new Path(args[0]);

        if (!fs.exists(nomcomplet)) {
            System.out.println("Le fichier n'existe pas");
        } else {
            // Afficher taille du fichier
            FileStatus infos = fs.getFileStatus(nomcomplet);
            System.out.println(Long.toString(infos.getLen()) + " octets");

            // Liste des blocs du fichier
            System.out.println("\nInformations sur les blocs:");
            System.out.println("Block Size: " + infos.getBlockSize());
            System.out.println("Replication: " + infos.getReplication());

        }

        // Fermer HDFS
        fs.close();
    }
}