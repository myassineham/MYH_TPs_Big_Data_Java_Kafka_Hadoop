package ma.ensias;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        // Configuration Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // Spécifier la classe principale
        job.setJarByClass(WordCount.class);

        // Spécifier les classes Mapper et Reducer
        job.setMapperClass(TokenizerMapper.class);

        // classe qui fait le shuffling et le reduce
        job.setCombinerClass(IntSumReducer.class);  // Combiner = pré-agrégation locale
        job.setReducerClass(IntSumReducer.class);

        // Spécifier les types de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Spécifier les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Lancer le job et attendre la fin
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}