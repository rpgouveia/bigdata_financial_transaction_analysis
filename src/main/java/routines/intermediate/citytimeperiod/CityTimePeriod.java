package routines.intermediate.citytimeperiod;

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
// src/main/resources/transactions_data.csv output/city_time_period 1 local

/**
 * Driver class para CityTimePeriod - Análise temporal de transações por cidade
 * Demonstra o uso de Custom Writable com múltiplos contadores independentes
 *
 * Esta rotina intermediária processa transações financeiras e analisa para cada cidade:
 * - Número de transações na MANHÃ (00:00 - 11:59)
 * - Número de transações na TARDE (12:00 - 17:59)
 * - Número de transações na NOITE (18:00 - 23:59)
 * - Período de pico de cada cidade
 *
 * O Custom Writable (CityTimePeriodStatsWritable) encapsula três contadores independentes,
 * demonstrando análise multidimensional eficiente no Hadoop.
 */
public class CityTimePeriod extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: CityTimePeriod <input_path> <output_path> [num_reducers] [local]");
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
        Job job = Job.getInstance(conf, "city_time_period");

        // Configuração básica do job
        job.setJarByClass(CityTimePeriod.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuração dos caminhos
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Configuração do Mapper
        job.setMapperClass(CityTimePeriodMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CityTimePeriodStatsWritable.class);

        // Configuração do Combiner (agregação local)
        job.setCombinerClass(CityTimePeriodCombiner.class);

        // Configuração do Reducer
        job.setReducerClass(CityTimePeriodReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CityTimePeriodStatsWritable.class);  // Custom Writable

        // Número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Log de informações
        System.out.println("========================================");
        System.out.println("CityTimePeriod Job Configuration:");
        System.out.println("  Mode: " + (localMode ? "Local (Standalone)" : "Cluster"));
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + outputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println("  Combiner: Enabled");
        System.out.println("  Custom Writable: CityTimePeriodStatsWritable");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Esta rotina usa Custom Writable para análise temporal");
        System.out.println("com três contadores independentes por período do dia.");
        System.out.println();
        System.out.println("Períodos analisados:");
        System.out.println("  • Manhã: 00:00 - 11:59");
        System.out.println("  • Tarde: 12:00 - 17:59");
        System.out.println("  • Noite: 18:00 - 23:59");
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
                System.out.println("  CIDADE    Manhã: X (Y%) | Tarde: Z (W%) | Noite: A (B%) | Total: N | Pico: [Período]");
                System.out.println();
                System.out.println("Exemplos de análises possíveis:");
                System.out.println("  - Identificar cidades com maior movimento noturno");
                System.out.println("  - Comparar padrões de horário entre regiões");
                System.out.println("  - Detectar cidades com distribuição balanceada");
                System.out.println("  - Planejar operações baseado em períodos de pico");
                System.out.println("  - Analisar comportamento temporal dos consumidores");
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
        System.out.println("Iniciando CityTimePeriod...");
        System.out.println("Rotina Intermediária - Análise Temporal");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Objetivo: Analisar padrões temporais de transações por cidade");
        System.out.println("  - Transações por período do dia (manhã, tarde, noite)");
        System.out.println("  - Identificar períodos de pico por cidade");
        System.out.println("  - Comparar distribuição temporal entre cidades");
        System.out.println();

        // Executar com ToolRunner
        int exitCode = ToolRunner.run(new Configuration(), new CityTimePeriod(), args);

        System.out.println();
        System.out.println("========================================");
        System.out.println("CityTimePeriod finalizado com código: " + exitCode);
        System.out.println("========================================");

        System.exit(exitCode);
    }
}