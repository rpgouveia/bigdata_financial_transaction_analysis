package routines.intermediate.citystatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/city_statistics 1 local

/**
 * Driver class para CityStatistics - Estatísticas completas de transações por cidade
 * Demonstra o uso de Custom Writable no Hadoop MapReduce
 *
 * Esta rotina intermediária processa transações financeiras e calcula para cada cidade:
 * - Número total de transações
 * - Valor total transacionado
 * - Valor médio por transação (ticket médio)
 *
 * O Custom Writable (CityStatistics) encapsula múltiplas métricas relacionadas,
 * demonstrando o uso eficiente de tipos de dados customizados no Hadoop.
 */
public class CityStatistics extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: CityStatistics <input_path> <output_path> [num_reducers] [local]");
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

        // Configuração para modo local
        if (localMode) {
            System.out.println("Configurando para execução local (standalone)...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // Criar e configurar o job
        Job job = Job.getInstance(conf, "city_statistics");

        // Configuração básica do job
        job.setJarByClass(CityStatistics.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuração dos caminhos
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Configuração do Mapper
        job.setMapperClass(CityStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CityStatsWritable.class);

        // Configuração do Combiner (agregação local)
        job.setCombinerClass(CityStatisticsCombiner.class);

        // Configuração do Reducer
        job.setReducerClass(CityStatisticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CityStatsWritable.class); // Custom Writable

        // Número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Log de informações
        System.out.println("========================================");
        System.out.println("CityStatistics Job Configuration:");
        System.out.println("  Mode: " + (localMode ? "Local (Standalone)" : "Cluster"));
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + outputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println("  Combiner: Enabled");
        System.out.println("  Custom Writable: CityStatsWritable");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Esta rotina usa Custom Writable para armazenar");
        System.out.println("múltiplas métricas: contagem, total e média.");
        System.out.println();

        // Executar o job
        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println();
            System.out.println("========================================");
            System.out.println("Job concluído com sucesso!");
            System.out.println("========================================");

            // Mostrar estatísticas se for modo local
            if (localMode) {
                System.out.println();
                System.out.println("Estatísticas do Job:");
                System.out.println("  Registros processados: " +
                        job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                                "MAP_INPUT_RECORDS").getValue());

                System.out.println();
                System.out.println("Para ver os resultados:");
                System.out.println("  cat " + outputDir + "/part-r-00000");
                System.out.println();
                System.out.println("Formato do output:");
                System.out.println("  CIDADE    Transações: N | Total: $X.XX | Média: $Y.YY");
                System.out.println();
                System.out.println("Exemplos de análises possíveis:");
                System.out.println("  - Identificar cidades com maior volume de vendas");
                System.out.println("  - Comparar ticket médio entre cidades");
                System.out.println("  - Detectar cidades com poucos clientes mas alto valor");
                System.out.println("  - Analisar concentração de receita por região");
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
        System.out.println("========================================");
        System.out.println("Iniciando CityStatistics...");
        System.out.println("Rotina Intermediária - Custom Writable");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Objetivo: Calcular estatísticas completas por cidade");
        System.out.println("  - Número de transações");
        System.out.println("  - Valor total transacionado");
        System.out.println("  - Ticket médio (valor médio por transação)");
        System.out.println();

        // Executar com ToolRunner
        int exitCode = ToolRunner.run(new Configuration(), new CityStatistics(), args);

        System.out.println();
        System.out.println("========================================");
        System.out.println("CityStatistics finalizado com código: " + exitCode);
        System.out.println("========================================");

        System.exit(exitCode);
    }
}