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
import routines.intermediate.topcategoriesbycity.TopCategoriesResult;

public class TopCategoriesByState extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByState <input_path> <output_path> [num_reducers] [local]");
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        int numberOfReducers = (args.length > 2) ? Integer.parseInt(args[2]) : 1;
        boolean localMode = (args.length > 3 && "local".equals(args[3]));

        Configuration conf = this.getConf();

        if (localMode) {
            System.out.println("Configurando para execução local (standalone)...");
            conf.set("fs.defaultFS", "file:///");
            conf.set("mapreduce.framework.name", "local");
            conf.set("mapreduce.jobtracker.address", "local");
        }

        Job job = Job.getInstance(conf, "top_categories_by_state");

        job.setJarByClass(TopCategoriesByState.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setMapperClass(TopCategoriesByStateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MCCTransactionCount.class);

        job.setReducerClass(TopCategoriesByStateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TopCategoriesResult.class);

        job.setNumReduceTasks(numberOfReducers);

        System.out.println("========================================");
        System.out.println("TopCategoriesByState Job Configuration:");
        System.out.println("  Mode: " + (localMode ? "Local" : "Cluster"));
        System.out.println("  Input: " + inputPath);
        System.out.println("  Output: " + outputDir);
        System.out.println("  Reducers: " + numberOfReducers);
        System.out.println("  Combiner: Disabled (ranking requires all data)");
        System.out.println("  Reutiliza: MCCTransactionCount, TopCategoriesResult, MCCDescriptionMapper");
        System.out.println("========================================");

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("Job concluído com sucesso!");
            return 0;
        } else {
            System.err.println("Job falhou!");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando TopCategoriesByState...");
        int exitCode = ToolRunner.run(new Configuration(), new TopCategoriesByState(), args);
        System.exit(exitCode);
    }
}