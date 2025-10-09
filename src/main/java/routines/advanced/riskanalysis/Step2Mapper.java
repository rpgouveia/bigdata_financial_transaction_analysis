package routines.advanced.riskanalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Step 2 Mapper - Risk Category Classifier
 * Lê perfis do Step 1 e emite identidade para processamento no Reducer.
 *
 * Input: Output do Step 1 (client_id -> ClientProfileWritable)
 * Output: client_id -> perfil (para classificação no Reducer)
 */
public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text clientId = new Text();
    private Text profile = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        try {
            // Parse do output do Step 1
            // Formato: clientId \t transactionCount \t totalAmount \t ...
            String[] fields = line.split("\t");

            if (fields.length < 13) {
                context.getCounter("Step2", "INVALID_RECORDS").increment(1);
                return;
            }

            // Extrai client_id (primeiro campo)
            String client = fields[0].trim();

            // Emite: client_id -> linha completa do perfil
            clientId.set(client);
            profile.set(line);

            context.write(clientId, profile);
            context.getCounter("Step2", "VALID_RECORDS").increment(1);

        } catch (Exception e) {
            context.getCounter("Step2", "PARSE_ERRORS").increment(1);
        }
    }
}