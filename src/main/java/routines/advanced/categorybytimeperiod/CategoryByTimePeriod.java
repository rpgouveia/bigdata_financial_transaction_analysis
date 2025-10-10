package routines.advanced.categorybytimeperiod;

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
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;
import routines.intermediate.topcategoriesbycity.TopCategoriesResult;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/category_by_timeperiod 1 local

/**
 * ROTINA AVANÇADA MULTI-STEP
 * CategoryByTimePeriod - Top 3 Categorias por Período e Cidade
 *
 * Processamento em 2 etapas encadeadas:
 *
 * JOB 1 - AGREGAÇÃO:
 *   Input:  CSV de transações
 *   Output: Contagens agregadas por cidade-período-MCC
 *
 * JOB 2 - RANKING:
 *   Input:  Output do Job 1
 *   Output: Top 3 categorias por cidade-período
 *
 * Demonstra:
 *   ✓ Chave composta (Cidade + Período)
 *   ✓ Análise multidimensional
 *   ✓ Pipeline multi-step
 *   ✓ Reutilização de componentes
 */
public class CategoryByTimePeriod extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: CategoryByTimePeriod <input_path> <output_path> [num_reducers] [local]");
            System.err.println("  input_path: caminho do arquivo CSV de transações");
            System.err.println("  output_path: caminho do diretório de saída final");
            System.err.println("  num_reducers: número de reducers (opcional, padrão: 1)");
            System.err.println("  local: para execução local (opcional)");
            return -1;
        }

        // Parse dos parâmetros
        Path inputPath = new Path(args[0]);
        Path finalOutputDir = new Path(args[1]);
        int numberOfReducers = (args.length > 2) ? Integer.parseInt(args[2]) : 1;
        boolean localMode = (args.length > 3 && "local".equals(args[3]));

        // Path intermediário (entre Job 1 e Job 2)
        Path intermediateOutputDir = new Path(finalOutputDir.getParent(),
                finalOutputDir.getName() + "_step1_intermediate");

        // Configuração
        Configuration conf = this.getConf();

        // Configuração para modo local
        if (localMode) {
            System.out.println("Configurando para execução local (standalone)...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // ================================================================
        // JOB 1: AGREGAÇÃO
        // ================================================================
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║          JOB 1: AGREGAÇÃO                      ║");
        System.out.println("║  Somando transações por cidade-período-MCC     ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println();

        Job job1 = Job.getInstance(conf, "step1_aggregation");
        job1.setJarByClass(CategoryByTimePeriod.class);

        // Input/Output
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, intermediateOutputDir);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Mapper
        job1.setMapperClass(Step1AggregationMapper.class);
        job1.setMapOutputKeyClass(CityPeriodKey.class);
        job1.setMapOutputValueClass(MCCTransactionCount.class);

        // Combiner (otimização)
        job1.setCombinerClass(Step1AggregationCombiner.class);

        // Reducer (agregação)
        job1.setReducerClass(Step1AggregationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setNumReduceTasks(numberOfReducers);

        System.out.println("Job 1 - Configuração:");
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + intermediateOutputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println();

        // Executar Job 1
        long startTime1 = System.currentTimeMillis();
        boolean success1 = job1.waitForCompletion(true);
        long endTime1 = System.currentTimeMillis();

        if (!success1) {
            System.err.println("✗ Job 1 falhou!");
            return 1;
        }

        System.out.println();
        System.out.println("✓ Job 1 completo em " + (endTime1 - startTime1) + "ms");
        System.out.println("  Registros processados: " +
                job1.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                        "MAP_INPUT_RECORDS").getValue());

        // ================================================================
        // JOB 2: RANKING
        // ================================================================
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║          JOB 2: RANKING                        ║");
        System.out.println("║  Identificando Top 3 por cidade-período        ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println();

        Job job2 = Job.getInstance(conf, "step2_ranking");
        job2.setJarByClass(CategoryByTimePeriod.class);

        // Input/Output
        FileInputFormat.addInputPath(job2, intermediateOutputDir);
        FileOutputFormat.setOutputPath(job2, finalOutputDir);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Mapper
        job2.setMapperClass(Step2RankingMapper.class);
        job2.setMapOutputKeyClass(CityPeriodKey.class);
        job2.setMapOutputValueClass(MCCTransactionCount.class);

        // Reducer
        job2.setReducerClass(Step2RankingReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(TopCategoriesResult.class);

        job2.setNumReduceTasks(numberOfReducers);

        System.out.println("Job 2 - Configuração:");
        System.out.println("  Input: " + intermediateOutputDir);
        System.out.println("  Output: " + finalOutputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println();

        // Executar Job 2
        long startTime2 = System.currentTimeMillis();
        boolean success2 = job2.waitForCompletion(true);
        long endTime2 = System.currentTimeMillis();

        if (!success2) {
            System.err.println("✗ Job 2 falhou!");
            return 1;
        }

        System.out.println();
        System.out.println("✓ Job 2 completo em " + (endTime2 - startTime2) + "ms");
        System.out.println("  Registros processados: " +
                job2.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                        "MAP_INPUT_RECORDS").getValue());

        // ================================================================
        // FINALIZAÇÃO
        // ================================================================
        long totalTime = (endTime2 - startTime1);

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║      MULTI-STEP PROCESSING COMPLETO            ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Tempo total: " + totalTime + "ms");
        System.out.println("  Job 1 (Agregação): " + (endTime1 - startTime1) + "ms");
        System.out.println("  Job 2 (Ranking): " + (endTime2 - startTime2) + "ms");
        System.out.println();

        // Informações úteis
        if (localMode) {
            System.out.println("Arquivos gerados:");
            System.out.println("  Intermediário: " + intermediateOutputDir);
            System.out.println("  Final: " + finalOutputDir);
            System.out.println();
            System.out.println("Para ver os resultados:");
            System.out.println("  cat " + finalOutputDir + "/part-r-00000");
            System.out.println();
            System.out.println("Para remover arquivos intermediários:");
            System.out.println("  rm -rf " + intermediateOutputDir);
            System.out.println();
        }

        return 0;
    }

    /**
     * Método main - ponto de entrada da aplicação
     */
    public static void main(String[] args) throws Exception {
        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║   CategoryByTimePeriod - MULTI-STEP            ║");
        System.out.println("║   Rotina Avançada com Processamento em Etapas ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Objetivo: Identificar padrões de consumo por horário");
        System.out.println("  → Top 3 categorias por período e cidade");
        System.out.println();
        System.out.println("Arquitetura Multi-Step:");
        System.out.println("  1️⃣  Job 1 → Agregação de contagens");
        System.out.println("  2️⃣  Job 2 → Ranking top 3 categorias");
        System.out.println();
        System.out.println("Conceitos demonstrados:");
        System.out.println("  ✓ Chave composta (Cidade + Período)");
        System.out.println("  ✓ Análise multidimensional");
        System.out.println("  ✓ Pipeline multi-step encadeado");
        System.out.println("  ✓ Combiner para otimização");
        System.out.println();

        // Executar com ToolRunner
        int exitCode = ToolRunner.run(new Configuration(),
                new CategoryByTimePeriod(), args);

        System.out.println();
        System.out.println("Processo finalizado com código: " + exitCode);
        System.exit(exitCode);
    }
}