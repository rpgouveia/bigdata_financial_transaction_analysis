package routines.advanced.riskpipeline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Step 3 Mapper - Final Risk Report Generator
 * Lê classificações do Step 2 e emite por categoria para agregação final.
 *
 * Input: Output do Step 2 (risk_category -> ClientRiskWritable)
 * Output: risk_category -> dados do cliente
 */
public class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text riskCategory = new Text();
    private Text clientData = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        try {
            // Parse do output do Step 2
            // Formato: riskCategory \t clientId \t riskCategory \t riskScore \t riskFactors \t ...
            String[] fields = line.split("\t");

            // Esperamos 7 campos (key + 6 campos do risk)
            if (fields.length < 7) {
                context.getCounter("Step3", "INVALID_RECORDS").increment(1);
                return;
            }

            // Extrai categoria de risco (primeiro campo)
            String category = fields[0].trim();

            // Emite: risk_category -> linha completa
            riskCategory.set(category);
            clientData.set(line);

            context.write(riskCategory, clientData);
            context.getCounter("Step3", "VALID_RECORDS").increment(1);

        } catch (Exception e) {
            context.getCounter("Step3", "PARSE_ERRORS").increment(1);
        }
    }
}