package routines.intermediate.HouseData;

// Exemplo do professor sobre chaves customizadas
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/*
*  Objetivo: Calcular a Média de preço das casas por cidade e status
*
*
* */

public class CityStatusAvgPriceDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/HouseData.csv");
        Path output = new Path("output/HouseData");

        Job job = Job.getInstance(conf, "CityStatus-AvgPrice");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(AvgPriceMapper.class);
        job.setReducerClass(AvgPriceReducer.class);
        job.setCombinerClass(AvgPriceCombiner.class);

        job.setMapOutputKeyClass(CityStatusKey.class);
        job.setMapOutputValueClass(AvgPriceValue.class);

        job.setOutputKeyClass(CityStatusKey.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(),
                new CityStatusAvgPriceDriver(),
                args);

        System.exit(result);
    }

    public static class AvgPriceMapper extends Mapper<LongWritable, Text, CityStatusKey, AvgPriceValue>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.startsWith("brokered_by")) {
                String[] columns = line.split(",");
                String city = columns[7];
                String status = columns[1];
                String priceS = columns[2];
                if(!priceS.isEmpty() && !status.isEmpty() && !city.isEmpty()){
                    float price = Float.parseFloat(priceS);

                    CityStatusKey k = new CityStatusKey(city.toUpperCase(), status.toUpperCase());
                    AvgPriceValue v = new AvgPriceValue(price, 1);

                    context.write(k, v);
                }

            }
        }
    }

    public static class AvgPriceCombiner extends Reducer<CityStatusKey, AvgPriceValue, CityStatusKey, AvgPriceValue> {
        public void reduce(CityStatusKey key, Iterable<AvgPriceValue> values, Context context)
                throws IOException, InterruptedException {
            float sumP = 0;
            int sumN = 0;

            for(AvgPriceValue v: values){
                sumP += v.getPrice();
                sumN += v.getN();
            }

            AvgPriceValue value = new AvgPriceValue(sumP, sumN);
            context.write(key, value);
        }
    }

    public static class AvgPriceReducer extends Reducer<CityStatusKey, AvgPriceValue, CityStatusKey, FloatWritable> {
        public void reduce(CityStatusKey key, Iterable<AvgPriceValue> values, Context context)
                throws IOException, InterruptedException {
            float sumP = 0;
            int sumN = 0;

            for(AvgPriceValue v: values){
                sumP += v.getPrice();
                sumN += v.getN();
            }

            FloatWritable avg = new FloatWritable(sumP/sumN);
            context.write(key, avg);
        }
    }
}
