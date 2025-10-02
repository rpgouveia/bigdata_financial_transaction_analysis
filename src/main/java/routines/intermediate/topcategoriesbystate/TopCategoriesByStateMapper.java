package routines.intermediate.topcategoriesbystate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper para análise de categorias (MCC) por estado
 * Emite pares (Estado, MCCCountWritable) para cada transação
 */

public class TopCategoriesByStateMapper extends Mapper<LongWritable, Text, Text, routines.intermediate.topcategoriesbycity.MCCCountWritable> {

    private Text outputKey = new Text();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        // Ignorar cabeçalho
        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            // Parse da linha CSV
            String[] parts = splitCsv(line);

            // Verificar campos mínimos
            if (parts.length < 12) {
                invalidRecords++;
                return;
            }

            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String stateRaw = parts[8];
            String mccRaw = parts[10];

            String state = processState(stateRaw);
            String mcc = processMCC(mccRaw);

            if (!state.isEmpty() && !mcc.isEmpty() && !mcc.equals("UNKNOWN_MCC")) {
                routines.intermediate.topcategoriesbycity.MCCCountWritable out =
                        new routines.intermediate.topcategoriesbycity.MCCCountWritable(mcc, 1);

                outputKey.set(state);
                context.write(outputKey, out);
                validRecords++;
            } else {
                invalidRecords++;
            }

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Erro processando linha: " + e.getMessage());
        }

        if (recordsProcessed % 50000 == 0) {
            context.setStatus(String.format("Processados %d registros. Válidos: %d, Inválidos: %d",
                    recordsProcessed, validRecords, invalidRecords));
        }
    }

    /**
     * Normaliza/valida a sigla do estado
     */
    private String processState(String stateRaw) {
        if (stateRaw == null) return "UNKNOWN";
        String s = stateRaw.trim().replace("\"", "").toUpperCase();
        if (s.isEmpty() || s.equals("NULL") || s.equals("N/A")) return "UNKNOWN";
        // Se vier como "CA", "NY", "TX" etc. já está ok; se vier nome completo, mantém em upper.
        return s;
    }

    /**
     * Processa e valida o código MCC
     */
    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) {
            return "UNKNOWN_MCC";
        }
        String mcc = mccRaw.trim().replace("\"", "");
        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) {
            return "UNKNOWN_MCC";
        }
        return mcc.matches("\\d+") ? mcc : "UNKNOWN_MCC";
    }

    /**
     * Split de CSV que respeita aspas
     */
    private static String[] splitCsv(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '\"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                result.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(ch);
            }
        }

        result.add(currentField.toString());
        return result.toArray(new String[0]);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Mapper (por ESTADO):");
        System.out.println("  Total de registros processados: " + recordsProcessed);
        System.out.println("  Cabeçalhos ignorados: " + headerSkipped);
        System.out.println("  Registros válidos: " + validRecords);
        System.out.println("  Registros inválidos: " + invalidRecords);

        if (recordsProcessed > 0) {
            double successRate = (double) validRecords / recordsProcessed * 100;
            System.out.println("  Taxa de sucesso: " + String.format("%.2f%%", successRate));
        }
        System.out.println("========================================");
        super.cleanup(context);
    }
}
