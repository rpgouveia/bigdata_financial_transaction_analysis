package routines.amountbycity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/amount_by_city 1 local

/**
 * Driver class para AmountByCity - Soma valores transacionados por cidade
 * Processa dados de transações financeiras em formato CSV
 */
public class AmountByCity extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: AmountByCity <input_path> <output_path> [num_reducers] [local]");
            System.err.println("  input_path: caminho do arquivo CSV de transações");
            System.err.println("  output_path: caminho do diretório de saída");
            System.err.println("  num_reducers: número de reducers (opcional, padrão: 1)");
            System.err.println("  local: para execução local (opcional)");
            return -1;
        }

        // Parse dos parâmetros
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        int numberOfReducers = (args.length > 2) ? Integer.parseInt(args[2]) : 1;
        boolean localMode = (args.length > 3 && "local".equals(args[3]));

        // Configuração
        Configuration conf = this.getConf();

        // Se modo local for especificado
        if (localMode) {
            System.out.println("Configurando para execução local (standalone)...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // Criar e configurar o job
        Job job = Job.getInstance(conf, "amount_by_city");

        // Configuração básica do job
        job.setJarByClass(AmountByCity.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuração dos caminhos
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Configuração do Mapper
        job.setMapperClass(AmountByCityMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Configuração do Reducer
        job.setReducerClass(AmountByCityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Configuração do Combiner (neste caso usar o mesmo reducer)
        job.setCombinerClass(AmountByCityReducer.class);

        // Número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Log de informações
        System.out.println("========================================");
        System.out.println("AmountByCity Job Configuration:");
        System.out.println("  Mode: " + (localMode ? "Local (Standalone)" : "Cluster"));
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + outputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println("  Combiner: Enabled (using Reducer)");
        System.out.println("========================================");

        // Executar o job
        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("Job concluído com sucesso!");

            // Mostrar estatísticas básicas se for modo local
            if (localMode) {
                System.out.println("\nEstatísticas do Job:");
                System.out.println("  Registros processados: " +
                        job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                                "MAP_INPUT_RECORDS").getValue());

                System.out.println("\nPara ver os resultados:");
                System.out.println("  cat " + outputDir + "/part-r-00000");
                System.out.println("  # Para ver os valores em formato monetário:");
                System.out.println("  # Os valores estão em centavos, divida por 100 para reais");
            }

            return 0;
        } else {
            System.err.println("Job falhou!");
            return 1;
        }
    }

    /**
     * Método main - ponto de entrada da aplicação
     */
    public static void main(String[] args) throws Exception {
        // Configuração inicial
        System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"));

        // Log de debug
        System.out.println("Iniciando AmountByCity...");
        System.out.println("HADOOP_HOME: " + System.getenv("HADOOP_HOME"));
        System.out.println("Processando transações financeiras por cidade");

        // Executar com ToolRunner
        int exitCode = ToolRunner.run(new Configuration(), new AmountByCity(), args);

        System.out.println("AmountByCity finalizado com código: " + exitCode);
        System.exit(exitCode);
    }
}