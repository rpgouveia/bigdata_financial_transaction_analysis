package routines.advanced.clientbehaviorchipuse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/client_behavior/client_behavior_stage1 output/client_behavior/client_behavior_final 1 local

/**
 * Driver multi-estágio:
 * Job 1: agrega por client_id e emite perfis por UF (um registro por cliente na UF predominante)
 * Job 2: agrega por UF os perfis (Low/Med/High) e ranqueia cidades High Risk.
 *
 * Args:
 *   [0] input_path CSV
 *   [1] stage1_output_path
 *   [2] final_output_path
 *   [3] num_reducers (opcional, padrão 1)
 *   [4] "local" (opcional)
 *
 * Parâmetros configuráveis (exemplo):
 *   -Drisk.error.high=0.05 -Drisk.error.med=0.02 -Drisk.chip.low=0.50 -Drisk.chip.med=0.70 \
 *   -Drisk.avg_amount.high_cents=10000 -Drisk.max_amount.high_cents=50000
 */
public class ClientBehaviorChipUse extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ClientBehaviorChipUse <input_csv> <stage1_out> <final_out> [num_reducers] [local]");
            return -1;
        }

        Path input = new Path(args[0]);
        Path stage1Out = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        int reducers = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
        boolean local = (args.length > 4 && "local".equalsIgnoreCase(args[4]));

        Configuration conf = this.getConf();

        if (local) {
            System.out.println("Executando em modo local...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // Defaults de risco (podem ser sobrescritos com -D)
        conf.setFloat("risk.error.high", conf.getFloat("risk.error.high", 0.05f));
        conf.setFloat("risk.error.med",  conf.getFloat("risk.error.med",  0.02f));
        conf.setFloat("risk.chip.low",   conf.getFloat("risk.chip.low",   0.50f));
        conf.setFloat("risk.chip.med",   conf.getFloat("risk.chip.med",   0.70f));
        conf.setLong("risk.avg_amount.high_cents", conf.getLong("risk.avg_amount.high_cents", 10000L));  // $100
        conf.setLong("risk.max_amount.high_cents", conf.getLong("risk.max_amount.high_cents", 50000L));  // $500

        // -------------------------
        // Job 1: agrega por client_id
        // -------------------------
        Job job1 = Job.getInstance(conf, "client_behavior_stage1_by_client");
        job1.setJarByClass(ClientBehaviorChipUse.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, input);

        job1.setMapperClass(ClientAggMapper.class);
        job1.setMapOutputKeyClass(Text.class);                // client_id
        job1.setMapOutputValueClass(TransactionMiniWritable.class);

        // Sem combiner no Job 1 (precisamos de todos os eventos do cliente)
        job1.setReducerClass(ClientAggReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        job1.setNumReduceTasks(reducers);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, stage1Out);

        System.out.println("== Job 1 ==");
        System.out.println("Input: " + input);
        System.out.println("Output: " + stage1Out);
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 falhou");
            return 1;
        }

        // -------------------------
        // Job 2: agrega por UF
        // -------------------------
        Job job2 = Job.getInstance(conf, "client_behavior_stage2_by_state");
        job2.setJarByClass(ClientBehaviorChipUse.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, stage1Out);

        job2.setMapperClass(StateAggMapper.class);
        job2.setMapOutputKeyClass(Text.class);                // state (UF)
        job2.setMapOutputValueClass(StateClientAggWritable.class);

        job2.setCombinerClass(StateAggCombiner.class);

        job2.setReducerClass(StateAggReducer.class);
        job2.setOutputKeyClass(Text.class);                   // state (UF)
        job2.setOutputValueClass(StateSummaryWritable.class);

        job2.setNumReduceTasks(reducers);

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, finalOut);

        System.out.println("== Job 2 ==");
        System.out.println("Input: " + stage1Out);
        System.out.println("Output: " + finalOut);

        boolean ok = job2.waitForCompletion(true);
        System.out.println(ok ? "Pipeline concluído com sucesso" : "Pipeline falhou");
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Iniciando ClientBehaviorChipUse (AVANÇADO)");
        System.out.println(" - Perfil do cliente: chip vs. falhas");
        System.out.println(" - Encadeado: Job1(cliente) → Job2(estado)");
        System.out.println("========================================");

        int exit = ToolRunner.run(new ClientBehaviorChipUse(), args);
        System.out.println("Finalizado com código: " + exit);
        System.exit(exit);
    }
}
