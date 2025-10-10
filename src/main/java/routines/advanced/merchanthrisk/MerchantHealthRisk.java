package routines.advanced.merchanthrisk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Para executar, configure os argumentos nesta ordem (exemplo):
// src/main/resources/transactions_data.csv output/merchant_stage1 output/merchant_final 1 local
//
// Onde:
//  - input_csv      : arquivo CSV de transações
//  - stage1_out     : saída intermediária do Job 1 (por merchant → UF)
//  - final_out      : saída final do Job 2 (por UF)
//  - num_reducers   : número de reducers (opcional, padrão 1)
//  - local          : string "local" para rodar em modo standalone (opcional)
//
// Parâmetros -D úteis (opcionais, com defaults embutidos):
//  -Dhealth.revenue.med_cents=500000
//  -Dhealth.revenue.high_cents=2000000
//  -Dhealth.avg.med_cents=8000
//  -Dhealth.tx.min_for_avg=100
//  -Drisk.error.med=0.02 -Drisk.error.high=0.05
//  -Drisk.online.med=0.70 -Drisk.online.high=0.90
//  -Drisk.max.high_cents=500000
//  -Dtop.merchants.k=5 -Dmin.uf.merchants=5

/**
 * Driver para MerchantHealthRisk - Radar de Saúde e Risco por UF (2 jobs encadeados)
 *
 * Esta rotina "avançada" realiza:
 *  - JOB 1 (por merchant_id): agrega transações do comerciante, calcula métricas,
 *    classifica Health (A/B/C) e Risk (LOW/MED/HIGH), identifica UF/cidade predominantes
 *    e emite 1 linha textual por UF predominante do merchant.
 *
 *  - JOB 2 (por UF): consolida contadores de Health e Risk, mantém top-K merchants por valor
 *    e "hotspots" (cidades com mais merchants de alto risco) e emite saída legível por estado.
 *
 * Destaques pedagógicos:
 *  - Uso de Custom Writables compostos (com mapas + top-K) e Combiner associativo.
 *  - Thresholds parametrizáveis via -D (sem recompilar).
 *  - Intercâmbio texto entre Jobs para facilitar depuração (cat/grep).
 */
public class MerchantHealthRisk extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Verificação de argumentos mínimos
        if (args.length < 3) {
            System.err.println("Usage: MerchantHealthRisk <input_csv> <stage1_out> <final_out> [num_reducers] [local] [-D...]");
            return -1;
        }

        // Parse básico
        Path input    = new Path(args[0]);
        Path stage1   = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        int reducers  = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
        boolean local = (args.length > 4 && "local".equalsIgnoreCase(args[4]));

        // Configuração base
        Configuration conf = getConf();

        // Modo local (Standalone)
        if (local) {
            System.out.println("Configurando execução local (Standalone)...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        // Defaults dos parâmetros (-D pode sobrescrever)
        applyDefaultParams(conf);

        // =======================
        // JOB 1 - Merchant → UF
        // =======================
        Job j1 = Job.getInstance(conf, "merchant_health_risk_stage1");
        j1.setJarByClass(MerchantHealthRisk.class);

        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, stage1);

        // Map: emite (merchant_id, TransactionMiniWritable)
        j1.setMapperClass(MerchantAggMapper.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(TransactionMiniWritable.class);

        // Reduce: agrega por merchant_id, classifica e emite texto (UF → linha por merchant)
        j1.setReducerClass(MerchantAggReducer.class);
        j1.setOutputKeyClass(Text.class); // UF predominante
        j1.setOutputValueClass(Text.class);

        j1.setNumReduceTasks(reducers);

        // Logs informativos
        System.out.println("========================================");
        System.out.println("JOB 1 - merchant_health_risk_stage1");
        System.out.println("  Input : " + input);
        System.out.println("  Output: " + stage1);
        System.out.println("  Reducers: " + reducers);
        System.out.println("  Intercâmbio: Texto (legível p/ debug)");
        System.out.println("========================================");

        if (!j1.waitForCompletion(true)) {
            System.err.println("Job 1 falhou.");
            return 1;
        }

        // =======================
        // JOB 2 - UF (consolidação)
        // =======================
        Job j2 = Job.getInstance(conf, "merchant_health_risk_final");
        j2.setJarByClass(MerchantHealthRisk.class);

        j2.setInputFormatClass(TextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(j2, stage1);
        FileOutputFormat.setOutputPath(j2, finalOut);

        // Map: reconstrói objeto agregado por UF a partir do texto
        j2.setMapperClass(StateAggMapper.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(StateMerchantAggWritable.class);

        // Combiner: merge associativo (contadores + mapas + top-K)
        j2.setCombinerClass(StateAggCombiner.class);

        // Reduce: consolida UF e emite linha final legível (distribuições + top-K + hotspots)
        j2.setReducerClass(StateAggReducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        j2.setNumReduceTasks(reducers);

        System.out.println("========================================");
        System.out.println("JOB 2 - merchant_health_risk_final");
        System.out.println("  Input : " + stage1);
        System.out.println("  Output: " + finalOut);
        System.out.println("  Reducers: " + reducers);
        System.out.println("  Combiner: Habilitado (merge associativo)");
        System.out.println("========================================");

        boolean ok = j2.waitForCompletion(true);

        if (ok) {
            System.out.println();
            System.out.println("========================================");
            System.out.println("Pipeline concluído com sucesso!");
            System.out.println("Como inspecionar:");
            System.out.println("  - Saída intermediária:  cat " + stage1 + "/part-r-00000");
            System.out.println("  - Saída final        :  cat " + finalOut + "/part-r-00000");
            System.out.println("========================================");
            return 0;
        } else {
            System.err.println("Job 2 falhou.");
            return 1;
        }
    }

    /** Seta padrões apenas se usuário não passou via -D */
    private static void applyDefaultParams(Configuration conf) {
        setIfMissing(conf, "health.revenue.med_cents",  "500000");
        setIfMissing(conf, "health.revenue.high_cents", "2000000");
        setIfMissing(conf, "health.avg.med_cents",      "8000");
        setIfMissing(conf, "health.tx.min_for_avg",     "100");

        setIfMissing(conf, "risk.error.med",  "0.02");
        setIfMissing(conf, "risk.error.high", "0.05");
        setIfMissing(conf, "risk.online.med", "0.70");
        setIfMissing(conf, "risk.online.high","0.90");
        setIfMissing(conf, "risk.max.high_cents", "500000");

        setIfMissing(conf, "top.merchants.k", "5");
        setIfMissing(conf, "min.uf.merchants","5");
    }

    private static void setIfMissing(Configuration conf, String key, String value) {
        if (conf.get(key) == null) conf.set(key, value);
    }

    /**
     * Main - ponto de entrada
     */
    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Iniciando MerchantHealthRisk (2 jobs)...");
        System.out.println("Objetivo: Radar de Saúde (A/B/C) e Risco (LOW/MED/HIGH) por UF");
        System.out.println("Com Top-K merchants por valor e hotspots de alto risco por cidade");
        System.out.println("========================================");
        int ec = ToolRunner.run(new Configuration(), new MerchantHealthRisk(), args);
        System.out.println("MerchantHealthRisk finalizado com código: " + ec);
        System.exit(ec);
    }
}
