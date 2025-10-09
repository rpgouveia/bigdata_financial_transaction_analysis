package routines.advanced.riskanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Step 1 Reducer - Client Profile Builder
 * Agrega todas as transações de um cliente e calcula perfil comportamental.
 *
 * Input: client_id -> lista de transações
 * Output: client_id -> ClientProfileWritable
 */
public class Step1Reducer extends Reducer<Text, Text, Text, ClientProfileWritable> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String clientId = key.toString();

        // Estruturas para agregação
        int transactionCount = 0;
        double totalAmount = 0.0;
        Set<String> cities = new HashSet<>();
        Set<String> mccs = new HashSet<>();
        Set<String> cards = new HashSet<>();
        long firstTransaction = Long.MAX_VALUE;
        long lastTransaction = Long.MIN_VALUE;
        int onlineCount = 0;
        int swipeCount = 0;
        int errorCount = 0;
        int chargebackCount = 0;

        // Processa todas as transações do cliente
        for (Text value : values) {
            try {
                String[] fields = value.toString().split(",");

                if (fields.length < 12) continue;

                // Extrai campos
                String dateStr = fields[1].trim();
                String cardId = fields[3].trim();
                String amountStr = fields[4].trim();
                String useChip = fields[5].trim();
                String merchantCity = fields[7].trim();
                String mcc = fields[10].trim();
                String errors = fields[11].trim();

                // Converte data para timestamp
                Date date = dateFormat.parse(dateStr);
                long timestamp = date.getTime();

                // Converte amount
                double amount = Double.parseDouble(amountStr.replace("$", ""));

                // Agrega informações
                transactionCount++;
                totalAmount += Math.abs(amount);

                if (!merchantCity.isEmpty() && !merchantCity.equals("ONLINE")) {
                    cities.add(merchantCity);
                }

                if (!mcc.isEmpty()) {
                    mccs.add(mcc);
                }

                cards.add(cardId);

                // Timestamps
                if (timestamp < firstTransaction) firstTransaction = timestamp;
                if (timestamp > lastTransaction) lastTransaction = timestamp;

                // Tipo de transação
                if (useChip.contains("Online")) {
                    onlineCount++;
                } else {
                    swipeCount++;
                }

                // Erros - só conta se tiver conteúdo real
                // Campo vazio = sem erro, Campo com texto = erro
                if (errors != null && !errors.trim().isEmpty() &&
                        !errors.equalsIgnoreCase("null") && !errors.equals("N/A")) {
                    errorCount++;
                }

                // Chargebacks (valores negativos)
                if (amount < 0) {
                    chargebackCount++;
                }

            } catch (Exception e) {
                context.getCounter("Step1", "REDUCER_ERRORS").increment(1);
            }
        }

        // Calcula média
        double avgAmount = transactionCount > 0 ? totalAmount / transactionCount : 0.0;

        // Cria perfil do cliente
        ClientProfileWritable profile = new ClientProfileWritable(
                clientId,
                transactionCount,
                totalAmount,
                avgAmount,
                cities.size(),
                mccs.size(),
                cards.size(),
                firstTransaction,
                lastTransaction,
                onlineCount,
                swipeCount,
                errorCount,
                chargebackCount
        );

        context.write(key, profile);
        context.getCounter("Step1", "PROFILES_CREATED").increment(1);
    }
}