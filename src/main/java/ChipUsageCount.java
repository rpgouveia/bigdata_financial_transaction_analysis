// Contar tipos de transação

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ChipUsageCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = files.length > 0 ? new Path(files[0]) : new Path("src/main/resources/transactions_data.csv");

        // arquivo de saida
        Path output = files.length > 1 ? new Path(files[1]) : new Path("output/chip_usage");

        // criacao do job e seu nome
        Job j = new Job(c, "chip_usage");

        // registro das classes
        j.setJarByClass(ChipUsageCount.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("id,")) return;

            String[] parts = splitCsv(line);
            if (parts.length < 12) return;

            // id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)
            String raw = parts[5] == null ? "" : parts[5].trim();
            if (raw.isEmpty()) raw = "unknown";

            con.write(new Text(raw), new IntWritable(1));
        }

        private static String[] splitCsv(String line) {
            java.util.List<String> out = new java.util.ArrayList<>();
            StringBuilder cur = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < line.length(); i++) {
                char ch = line.charAt(i);
                if (ch == '\"') {
                    inQuotes = !inQuotes;
                } else if (ch == ',' && !inQuotes) {
                    out.add(cur.toString());
                    cur.setLength(0);
                } else {
                    cur.append(ch);
                }
            }
            out.add(cur.toString());
            return out.toArray(new String[0]);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            con.write(key, new IntWritable(sum));
        }
    }
}
