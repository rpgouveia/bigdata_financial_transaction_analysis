// Soma de valor transacionado por cliente (client_id)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AmountByClient {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("src/main/resources/transactions_data.csv");

        // arquivo de saida
        Path output = new Path("output/amount_by_client");

        // criacao do job e seu nome
        Job j = new Job(c, "amount_by_client");

        // registro das classes
        j.setJarByClass(AmountByClient.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("id,")) return;

            String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            // id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)
            if (cols.length < 12) return;

            String clientId = cols[2].trim();
            String amountStr = cols[4].trim();

            if (clientId.isEmpty() || amountStr.isEmpty()) return;

            con.write(new Text(clientId), new Text(amountStr));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            double total = 0.0;
            for (Text t : values) {
                String s = t.toString();
                if (s == null) continue;
                s = s.replace("\"", "").replace("$", "").replace(",", "").trim();
                if (s.isEmpty()) continue;
                try {
                    total += Double.parseDouble(s);
                } catch (NumberFormatException ignored) { }
            }

            con.write(new Text(key.toString()), new DoubleWritable(total));
        }
    }
}
