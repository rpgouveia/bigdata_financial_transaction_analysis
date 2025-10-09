package routines.advanced.riskpipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver para pipeline de análise de risco com múltiplos steps.
 *
 * MULTI-STEP MAPREDUCE PIPELINE (3 Jobs Encadeados):
 *
 * Step 1: Client Profile Builder
 *   - Input: Transações brutas (CSV)
 *   - Output: Perfis agregados por cliente
 *   - Função: Calcular estatísticas comportamentais
 *
 * Step 2: Risk Category Classifier
 *   - Input: Output do Step 1
 *   - Output: Clientes classificados por categoria de risco
 *   - Função: Calcular risk score e categorizar
 *
 * Step 3: Final Risk Report Generator
 *   - Input: Output do Step 2
 *   - Output: Relatórios consolidados por categoria
 *   - Função: Gerar rankings e estatísticas finais
 *
 * Uso: hadoop jar risk-pipeline.jar RiskAnalysisPipeline <input> <output>
 */
public class RiskAnalysisPipeline extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Uso: RiskAnalysisPipeline <input_path> <output_path>");
            System.err.println();
            System.err.println("Este pipeline executa 3 jobs encadeados:");
            System.err.println("  1. Client Profile Builder");
            System.err.println("  2. Risk Category Classifier");
            System.err.println("  3. Final Risk Report Generator");
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Paths intermediários para os steps
        String step1Output = outputPath + "_step1_profiles";
        String step2Output = outputPath + "_step2_classifications";
        String step3Output = outputPath + "_step3_final";

        Configuration conf = getConf();

        System.out.println("\n============================================================");
        System.out.println("    RISK ANALYSIS PIPELINE - MULTI-STEP MAPREDUCE");
        System.out.println("============================================================");
        System.out.println("Input: " + inputPath);
        System.out.println("Final Output: " + step3Output);
        System.out.println("============================================================\n");

        long totalStartTime = System.currentTimeMillis();

        // ===== STEP 1: CLIENT PROFILE BUILDER =====
        System.out.println("\n>>> STEP 1: Building Client Profiles...");
        long step1Start = System.currentTimeMillis();

        boolean step1Success = runStep1(conf, inputPath, step1Output);

        if (!step1Success) {
            System.err.println("ERRO: Step 1 falhou!");
            return 1;
        }

        long step1Duration = System.currentTimeMillis() - step1Start;
        System.out.println(">>> STEP 1 COMPLETED in " + (step1Duration / 1000) + " seconds");

        // ===== STEP 2: RISK CATEGORY CLASSIFIER =====
        System.out.println("\n>>> STEP 2: Classifying Risk Categories...");
        long step2Start = System.currentTimeMillis();

        boolean step2Success = runStep2(conf, step1Output, step2Output);

        if (!step2Success) {
            System.err.println("ERRO: Step 2 falhou!");
            return 1;
        }

        long step2Duration = System.currentTimeMillis() - step2Start;
        System.out.println(">>> STEP 2 COMPLETED in " + (step2Duration / 1000) + " seconds");

        // ===== STEP 3: FINAL RISK REPORT GENERATOR =====
        System.out.println("\n>>> STEP 3: Generating Final Risk Reports...");
        long step3Start = System.currentTimeMillis();

        boolean step3Success = runStep3(conf, step2Output, step3Output);

        if (!step3Success) {
            System.err.println("ERRO: Step 3 falhou!");
            return 1;
        }

        long step3Duration = System.currentTimeMillis() - step3Start;
        System.out.println(">>> STEP 3 COMPLETED in " + (step3Duration / 1000) + " seconds");

        // Estatísticas finais
        long totalDuration = System.currentTimeMillis() - totalStartTime;

        System.out.println("\n============================================================");
        System.out.println("       PIPELINE EXECUTION SUMMARY");
        System.out.println("============================================================");
        System.out.println("Step 1 Duration: " + (step1Duration / 1000) + " seconds");
        System.out.println("Step 2 Duration: " + (step2Duration / 1000) + " seconds");
        System.out.println("Step 3 Duration: " + (step3Duration / 1000) + " seconds");
        System.out.println("------------------------------------------------------------");
        System.out.println("Total Duration: " + (totalDuration / 1000) + " seconds");
        System.out.println("============================================================");
        System.out.println("\nFinal Output Location: " + step3Output);
        System.out.println("Intermediate Outputs:");
        System.out.println("  - Step 1: " + step1Output);
        System.out.println("  - Step 2: " + step2Output);
        System.out.println("============================================================\n");

        return 0;
    }

    /**
     * Step 1: Client Profile Builder
     * Agrega transações e cria perfis comportamentais.
     */
    private boolean runStep1(Configuration conf, String input, String output)
            throws Exception {

        // Remove output anterior se existir
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Step 1 - Client Profile Builder");
        job.setJarByClass(RiskAnalysisPipeline.class);

        // Mapper e Reducer
        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        // Output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ClientProfileWritable.class);

        // Paths
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    /**
     * Step 2: Risk Category Classifier
     * Classifica clientes em categorias de risco.
     */
    private boolean runStep2(Configuration conf, String input, String output)
            throws Exception {

        // Remove output anterior se existir
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Step 2 - Risk Category Classifier");
        job.setJarByClass(RiskAnalysisPipeline.class);

        // Mapper e Reducer
        job.setMapperClass(Step2Mapper.class);
        job.setReducerClass(Step2Reducer.class);

        // Output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ClientRiskWritable.class);

        // Paths
        FileInputFormat.addInputPath(job, new Path(input + "/part-r-*"));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    /**
     * Step 3: Final Risk Report Generator
     * Gera relatórios consolidados por categoria.
     */
    private boolean runStep3(Configuration conf, String input, String output)
            throws Exception {

        // Remove output anterior se existir
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Step 3 - Final Risk Report Generator");
        job.setJarByClass(RiskAnalysisPipeline.class);

        // Mapper e Reducer
        job.setMapperClass(Step3Mapper.class);
        job.setReducerClass(Step3Reducer.class);

        // Output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Paths
        FileInputFormat.addInputPath(job, new Path(input + "/part-r-*"));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    /**
     * Main method - ponto de entrada da aplicação.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new RiskAnalysisPipeline(), args);
        System.exit(exitCode);
    }
}