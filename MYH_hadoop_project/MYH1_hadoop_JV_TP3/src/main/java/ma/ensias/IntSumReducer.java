package ma.ensias;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * Méthode reduce : appelée pour chaque mot unique avec toutes ses occurrences
     * @param key - le mot
     * @param values - liste de toutes les valeurs (1,1,1...) pour ce mot
     * @param context - permet d'écrire le résultat final
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        // Additionner toutes les valeurs pour ce mot
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);
        // Écrire (mot, total) dans le fichier de sortie
        context.write(key, result);
    }
}