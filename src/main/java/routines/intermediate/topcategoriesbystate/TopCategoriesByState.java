package routines.intermediate.topcategoriesbystate;

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

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/top_categories_by_state 1 local

/**
 * Driver class para TopCategoriesByState- Top 3 Categorias por Estado
 * Demonstra o uso de Custom Writable com agregação complexa e ranking
 *
 * Esta rotina intermediária processa transações financeiras e identifica para cada estado:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em MCC codes)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 *
 * O Custom Writable (MCCCountWritable) encapsula código MCC e contagem,
 * e o Reducer implementa lógica de agregação com HashMap e ranking.
 */

public class TopCategoriesByState extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByState <input_path> <output_path> [num_reducers] [local]");
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
        Job job = Job.getInstance(conf, "top_categories_by_state");

        // Configuração básica do job
        job.setJarByClass(TopCategoriesByState.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuração dos caminhos
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Configuração do Mapper
        job.setMapperClass(TopCategoriesByStateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MCCTransactionCount.class);

        // Configuração do Reducer
        job.setReducerClass(TopCategoriesByStateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Número de reducers
        job.setNumReduceTasks(numberOfReducers);

        // Logs informativos
        System.out.println("========================================");
        System.out.println("TopCategoriesByState Job Configuration:");
        System.out.println("  Mode: " + (localMode ? "Local (Standalone)" : "Cluster"));
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + outputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println("  Combiner: Disabled (ranking requires all data)");
        System.out.println("  Custom Writable: MCCCountWritable (reutilizado)");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Esta rotina usa Custom Writable com agregação");
        System.out.println("e ranking para identificar as top 3 categorias");
        System.out.println("de produtos/serviços mais populares em cada ESTADO.");
        System.out.println();

        // Executar o job
        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println();
            System.out.println("========================================");
            System.out.println("Job concluído com sucesso!");
            System.out.println("========================================");

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
                System.out.println("  ESTADO    Top-1: MCC (Descrição) Count | Top-2: ... | Top-3: ...");
                System.out.println();
                System.out.println("Exemplo:");
                System.out.println("  CA    Top-1: 5812 (Restaurants) 3500 | Top-2: 5411 (Supermarkets) 2100 | Top-3: 5541 (Service Stations) 1800");
            }

            return 0;
        } else {
            System.err.println("Job falhou!");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Iniciando TopCategoriesByState...");
        System.out.println("Rotina Intermediária - Agregação e Ranking por ESTADO");
        System.out.println("========================================");
        System.out.println("Objetivo: Identificar as top 3 categorias por estado");
        System.out.println("  - Baseado em códigos MCC (Merchant Category Code)");
        System.out.println("  - Demonstra agregação com HashMap");
        System.out.println("  - Demonstra sorting e ranking");
        System.out.println();

        int exitCode = ToolRunner.run(new Configuration(), new TopCategoriesByState(), args);

        System.out.println();
        System.out.println("========================================");
        System.out.println("TopCategoriesByState finalizado com código: " + exitCode);
        System.out.println("========================================");

        System.exit(exitCode);
    }
}
