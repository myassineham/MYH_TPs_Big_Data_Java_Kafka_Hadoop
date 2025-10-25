package ma.ensias;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Constante pour la valeur 1 (réutilisée pour économiser la mémoire)
    private final static IntWritable one = new IntWritable(1);
    // Objet Text réutilisable pour stocker chaque mot
    private Text word = new Text();

    /**
     * Méthode map : appelée pour chaque ligne du fichier d'entrée
     * @param key - offset de la ligne (ignoré ici)
     * @param value - contenu de la ligne
     * @param context - permet d'écrire les résultats
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Divise la ligne en mots (séparés par espaces)
        StringTokenizer itr = new StringTokenizer(value.toString());

        // Pour chaque mot trouvé
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            // Écrire (mot, 1) vers le reducer
            context.write(word, one);
        }
    }
}