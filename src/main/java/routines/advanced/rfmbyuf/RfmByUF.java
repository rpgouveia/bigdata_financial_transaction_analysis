package routines.advanced.rfmbyuf;

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
// src/main/resources/transactions_data.csv output/rfmbyuf/stage1 output/rfmbyuf/rfmbyuf_final 1 local

/*
* RFM-lite do Cliente (Recência-Frequência-Monetário) por UF:
Job 1 (por cliente): calcula recência (dias desde a última compra até uma data de referência), frequência (nº de transações) e monetário (ticket médio), bucketiza em LOW/MED/HIGH VALUE via -D e emite 1 linha textual por cliente na UF predominante.
KEY: <STATE>
VALUE: 1:low:med:high|CITY=1 (CITY só se HIGH)

Job 2 (por UF): agrega total/low/med/high e ranqueia top cidades (High Value).
* */

public class RfmByUF extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: RfmByUF <input_csv> <stage1_out> <final_out> [num_reducers] [local] [-D...]");
            return -1;
        }
        Path input   = new Path(args[0]);
        Path stage1  = new Path(args[1]);
        Path finalOut= new Path(args[2]);
        int reducers = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
        boolean local= (args.length > 4 && "local".equalsIgnoreCase(args[4]));

        Configuration conf = getConf();
        if (local) {
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // >>> Defaults calibrados para o seu CSV de 2010
        applyDefaultParams(conf);
        // <<<

        // ===== Job 1
        Job job1 = Job.getInstance(conf, "rfm_client_stage1");
        job1.setJarByClass(RfmByUF.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, stage1);

        job1.setMapperClass(RfmClientMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TransactionRfmWritable.class);

        job1.setReducerClass(RfmClientReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(reducers);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed.");
            return 1;
        }

        // ===== Job 2
        Job job2 = Job.getInstance(conf, "rfm_state_aggregate_final");
        job2.setJarByClass(RfmByUF.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, stage1);
        FileOutputFormat.setOutputPath(job2, finalOut);

        job2.setMapperClass(StateAggMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(StateClientAggWritable.class);

        job2.setCombinerClass(StateAggCombiner.class);
        job2.setReducerClass(StateAggReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(reducers);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    /** ÚNICO lugar com defaults (respeita -D se o usuário passar na linha de comando). */
    private static void applyDefaultParams(Configuration conf) {
        // Âncora temporal no fim do seu dataset
        setIfMissing(conf, "rfm.reference.date", "2010-05-31"); // yyyy-MM-dd

        // Recency (em dias) — bons cortes para janela Jan–Mai/2010
        setIfMissing(conf, "rfm.recency.high_days", "30");
        setIfMissing(conf, "rfm.recency.med_days",  "90");

        // Frequency — calibrados para base densa (troque se quiser p50/p75/p90 reais)
        setIfMissing(conf, "rfm.freq.med",  "400");
        setIfMissing(conf, "rfm.freq.high", "900");

        // Monetary (média por cliente, em centavos) — use quantis do seu overview
        setIfMissing(conf, "rfm.monetary.med_cents",  "8000");   // $80,00
        setIfMissing(conf, "rfm.monetary.high_cents", "20000");  // $200,00

        // Hotspots
        setIfMissing(conf, "top.cities", "10");

        // (Opcional) mínimos para estabilidade de percentuais
        setIfMissing(conf, "min.client.tx", "10");   // ignora cliente com < 10 transações (se usado no reducer)
        setIfMissing(conf, "min.uf.clients", "20");  // só reporta UF com >= 20 clientes (se usado no reducer)
    }

    private static void setIfMissing(Configuration conf, String key, String value) {
        if (conf.get(key) == null) conf.set(key, value);
    }

    public static void main(String[] args) throws Exception {
        int ec = ToolRunner.run(new Configuration(), new RfmByUF(), args);
        System.exit(ec);
    }
}
