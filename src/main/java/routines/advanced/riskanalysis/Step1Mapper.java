package routines.advanced.riskanalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Step 1 Mapper - Client Profile Builder
 * Lê transações brutas e emite por client_id para agregação.
 *
 * Input: Transações do dataset
 * Output: client_id -> linha completa (para processamento no Reducer)
 */
public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text clientId = new Text();
    private Text transaction = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Ignora linhas vazias ou cabeçalho
        if (line.isEmpty() || line.startsWith("id,date") || line.startsWith("id")) {
            return;
        }

        try {
            // Parse da linha CSV
            String[] fields = line.split(",");

            if (fields.length < 12) {
                context.getCounter("Step1", "INVALID_RECORDS").increment(1);
                return;
            }

            // Extrai client_id
            String client = fields[2].trim();

            // Emite: client_id -> linha completa
            clientId.set(client);
            transaction.set(line);

            context.write(clientId, transaction);
            context.getCounter("Step1", "VALID_RECORDS").increment(1);

        } catch (Exception e) {
            context.getCounter("Step1", "PARSE_ERRORS").increment(1);
        }
    }
}