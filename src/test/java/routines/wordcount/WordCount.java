package routines.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Para executar configure os argumentos da seguinte forma:
// 1 src/test/resources/bible.txt output/wordcount/

/**
 * Driver class para o programa WordCount
 * Responsável por configurar e executar o job MapReduce
 */
public class WordCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length != 3) {
            System.err.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
            return -1;
        }

        // Variáveis de configuração
        Path inputPath;
        Path outputDir;
        int numberOfReducers;
        int exitCode;

        // Parse dos parâmetros de entrada
        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);

        // Definição e configuração do job
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        // Atribuir nome ao job
        job.setJobName("WordCounter");

        // Definir caminho do arquivo/pasta de entrada
        FileInputFormat.addInputPath(job, inputPath);

        // Definir caminho da pasta de saída
        FileOutputFormat.setOutputPath(job, outputDir);

        // Definir formato de entrada (arquivos textuais)
        job.setInputFormatClass(TextInputFormat.class);

        // Definir formato de saída
        job.setOutputFormatClass(TextOutputFormat.class);

        // Especificar a classe do Driver para este job
        job.setJarByClass(WordCount.class);

        // Definir a classe Mapper
        job.setMapperClass(WordCountMapper.class);

        // Definir tipos de chave e valor de saída do Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Definir a classe Reducer
        job.setReducerClass(WordCountReducer.class);

        // Definir tipos de chave e valor de saída do Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Definir número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Opcional: Definir combiner (usar a mesma classe do reducer)
        job.setCombinerClass(WordCountReducer.class);

        // Executar o job e aguardar conclusão
        if (job.waitForCompletion(true) == true) {
            exitCode = 0;
        } else {
            exitCode = 1;
        }

        return exitCode;
    }

    /**
     * Método main da classe driver
     */
    public static void main(String args[]) throws Exception {
        // Utilizar ToolRunner para configurar e executar a aplicação Hadoop
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
}
