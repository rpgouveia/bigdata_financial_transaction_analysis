package routines.intermediate.topcategoriesbycountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;

/**
 * Mapper para análise de categorias (MCC) por país
 * Emite pares (País, MCCTransactionCount) para cada transação internacional
 *
 * Filtra APENAS países, ou seja, REJEITA transações que ocorrem nos estados dos EUA.
 * Processa cada linha do CSV e extrai:
 * - merchant_state (campo 8): País onde a transação ocorreu
 * - mcc (campo 10): Código da categoria do comerciante
 *
 * Reutiliza MCCTransactionCount do pacote topcategoriesbycity
 */
public class TopCategoriesByCountryMapper extends Mapper<LongWritable, Text, Text, MCCTransactionCount> {

    // Objetos reutilizáveis
    private Text outputKey = new Text();

    // Conjunto de estados dos EUA que serão REJEITADOS por este Mapper
    private static final Set<String> US_STATES_TO_REJECT = new HashSet<>(Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    ));

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    /**
     * Método map - processa cada linha do CSV
     * @param key Offset da linha no arquivo
     * @param value Conteúdo da linha CSV
     * @param context Contexto para emitir resultados
     */
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

            String locationRaw = parts[8];  // Campo que pode ser estado OU país
            String mccRaw = parts[10];

            // Processar país (com validação para excluir estados dos EUA)
            String country = processCountry(locationRaw);

            // Processar MCC
            String mcc = processMCC(mccRaw);

            // Só processar se for um país válido (NÃO um estado dos EUA) e MCC válido
            if (country != null && !mcc.isEmpty() && !mcc.equals("UNKNOWN_MCC")) {
                MCCTransactionCount out = new MCCTransactionCount(mcc, 1);
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

        // Log de progresso
        if (recordsProcessed % 50000 == 0) {
            context.setStatus(String.format("Processados %d registros. Válidos: %d, Inválidos: %d",
                    recordsProcessed, validRecords, invalidRecords));
        }
    }

    /**
     * Valida se o local é um país (ou seja, NÃO é um estado dos EUA)
     * @param locationRaw Valor bruto do campo merchant_state
     * @return Nome do país em uppercase, ou null se for um estado dos EUA ou inválido
     */
    private String processCountry(String locationRaw) {
        if (locationRaw == null) return null;

        String location = locationRaw.trim().replace("\"", "").toUpperCase();

        if (location.isEmpty() || location.equals("NULL") || location.equals("N/A")) {
            return null;
        }

        // REJEITAR se for um estado dos EUA, retornando null
        if (US_STATES_TO_REJECT.contains(location)) {
            return null;
        }

        // Aceitar como país se não for um estado dos EUA
        return location;
    }

    /**
     * Processa e valida o código MCC
     * @param mccRaw Valor bruto do campo MCC
     * @return Código MCC limpo, ou "UNKNOWN_MCC" se inválido
     */
    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) {
            return "UNKNOWN_MCC";
        }
        String mcc = mccRaw.trim().replace("\"", "");
        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) {
            return "UNKNOWN_MCC";
        }
        // Validar se é numérico
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

    /**
     * Cleanup - estatísticas finais do Mapper
     */
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