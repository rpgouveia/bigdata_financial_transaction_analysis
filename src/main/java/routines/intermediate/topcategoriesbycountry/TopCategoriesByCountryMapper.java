package routines.intermediate.topcategoriesbycountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import routines.intermediate.topcategoriesbycity.MCCCountWritable;

/**
 * Mapper para análise de categorias (MCC) por país
 * Emite pares (País, MCCCountWritable) para cada transação
 * Filtra APENAS países (rejeita estados dos EUA)
 */
public class TopCategoriesByCountryMapper extends Mapper<LongWritable, Text, Text, MCCCountWritable> {

    private Text outputKey = new Text();

    // Estados dos EUA - serão REJEITADOS
    private static final Set<String> US_STATES = new HashSet<>(Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    ));

    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            String[] parts = splitCsv(line);
            if (parts.length < 12) {
                invalidRecords++;
                return;
            }

            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String locationRaw = parts[8];  // Campo que contém estado OU país
            String mccRaw = parts[10];

            String country = processCountry(locationRaw);
            String mcc = processMCC(mccRaw);

            // Só processar se for país válido (NÃO estado dos EUA)
            if (country != null && !mcc.isEmpty() && !mcc.equals("UNKNOWN_MCC")) {
                MCCCountWritable out = new MCCCountWritable(mcc, 1);
                outputKey.set(country);
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
     * Valida se é um país (NÃO estado dos EUA)
     * Retorna null se for estado dos EUA (para rejeitar)
     * Retorna o nome do país se for válido
     */
    private String processCountry(String locationRaw) {
        if (locationRaw == null) return null;

        String location = locationRaw.trim().replace("\"", "").toUpperCase();

        if (location.isEmpty() || location.equals("NULL") || location.equals("N/A")) {
            return null;
        }

        // REJEITAR se for estado dos EUA
        if (US_STATES.contains(location)) {
            return null;
        }

        // Aceitar se for país (tudo que não é estado dos EUA)
        return location;
    }

    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) return "UNKNOWN_MCC";
        String mcc = mccRaw.trim().replace("\"", "");
        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) return "UNKNOWN_MCC";
        return mcc.matches("\\d+") ? mcc : "UNKNOWN_MCC";
    }

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
        System.out.println("Estatísticas do Mapper (APENAS PAÍSES):");
        System.out.println("  Total de registros processados: " + recordsProcessed);
        System.out.println("  Cabeçalhos ignorados: " + headerSkipped);
        System.out.println("  Registros válidos (países): " + validRecords);
        System.out.println("  Registros rejeitados (estados EUA/inválidos): " + invalidRecords);

        if (recordsProcessed > 0) {
            double successRate = (double) validRecords / recordsProcessed * 100;
            System.out.println("  Taxa de processamento (países): " + String.format("%.2f%%", successRate));
        }
        System.out.println("========================================");
        super.cleanup(context);
    }
}