package routines.errorcountbymcc;

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
// src/main/resources/transactions_data.csv output/error_count_by_mcc 1 local

/**
 * Driver class para ErrorCountByMCC - Conta erros por Merchant Category Code
 * Processa dados de transações financeiras em formato CSV agrupando erros por MCC
 */
public class ErrorCountByMCC extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: ErrorCountByMCC <input_path> <output_path> [num_reducers] [local]");
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
        Job job = Job.getInstance(conf, "error_count_by_mcc");

        // Configuração básica do job
        job.setJarByClass(ErrorCountByMCC.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuração dos caminhos
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Configuração do Mapper
        job.setMapperClass(ErrorCountByMCCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Configuração do Reducer
        job.setReducerClass(ErrorCountByMCCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Configuração do Combiner (usar o mesmo reducer como combiner para otimização)
        job.setCombinerClass(ErrorCountByMCCReducer.class);

        // Número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Log de informações
        System.out.println("========================================");
        System.out.println("ErrorCountByMCC Job Configuration:");
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
                System.out.println("  # Contagem de erros por Merchant Category Code (MCC)");
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
        // Log de debug
        System.out.println("Iniciando ErrorCountByMCC...");
        System.out.println("Processando contagem de erros por Merchant Category Code");

        // Executar com ToolRunner
        int exitCode = ToolRunner.run(new Configuration(), new ErrorCountByMCC(), args);

        System.out.println("ErrorCountByMCC finalizado com código: " + exitCode);
        System.exit(exitCode);
    }
}