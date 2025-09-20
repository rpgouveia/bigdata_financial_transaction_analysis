// Soma do valor transacionado por cidade (merchant_city)

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
import java.math.BigDecimal;

public class AmountByCity {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = files.length > 0 ? new Path(files[0]) : new Path("src/main/resources/transactions_data.csv");

        // arquivo de saida
        Path output = files.length > 1 ? new Path(files[1]) : new Path("output/amount_by_city");

        // criacao do job e seu nome
        Job j = new Job(c, "amount_by_city");

        // registro das classes
        j.setJarByClass(AmountByCity.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LongWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("id,")) return; // ignora cabeçalho

            String[] parts = splitCsv(line);
            if (parts.length < 12) return;

            // id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)
            String city = parts[7] == null ? "" : parts[7].trim(); // merchant_city
            if (city.isEmpty()) city = "UNKNOWN";

            String amountStr = parts[4] == null ? "" : parts[4].trim(); // amount
            long cents = parseAmountToCents(amountStr);
            if (cents == Long.MIN_VALUE) return;

            con.write(new Text(city), new LongWritable(cents));
        }

        // Split de CSV simples que respeita aspas (sem dependência externa)
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

        // Remove símbolos e separadores comuns e converte para centavos
        private static long parseAmountToCents(String raw) {
            if (raw == null) return Long.MIN_VALUE;
            String s = raw.trim();
            if (s.isEmpty()) return Long.MIN_VALUE;
            // remove aspas, moeda e espaços
            s = s.replace("\"","")
                    .replace("R$", "")
                    .replace("$", "")
                    .replace(" ", "");
            // troca separador decimal vírgula por ponto; remove separador de milhar
            // exemplo: "1.234,56" -> "1234.56" | "1,234.56" -> "1234.56"
            if (s.matches(".*\\d,\\d{1,2}$") && !s.contains(".")) {
                s = s.replace(".", "");      // pontos como milhar
                s = s.replace(",", ".");     // vírgula decimal
            } else {
                s = s.replace(",", "");      // vírgula como milhar
            }
            try {
                BigDecimal bd = new BigDecimal(s).movePointRight(2);
                // HALF_UP sem constantes deprecated
                long cents = bd.setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();
                return cents;
            } catch (Exception e) {
                return Long.MIN_VALUE;
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            long sum = 0L;
            for (LongWritable v : values) sum += v.get();
            con.write(key, new LongWritable(sum)); // soma em centavos
        }
    }
}
