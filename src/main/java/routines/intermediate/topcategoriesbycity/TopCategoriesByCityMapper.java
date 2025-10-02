package routines.intermediate.topcategoriesbycity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper para análise de categorias (MCC) por cidade
 * Emite pares (cidade, MCCCountWritable) para cada transação
 */
public class TopCategoriesByCityMapper extends Mapper<LongWritable, Text, Text, MCCCountWritable> {

    // Objetos reutilizáveis
    private Text outputKey = new Text();
    private MCCCountWritable outputValue = new MCCCountWritable();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    /**
     * Método map - processa cada linha do CSV
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

            String cityRaw = parts[7];
            String mccRaw = parts[10];

            // Processar cidade
            String city = processCityName(cityRaw);

            // Processar MCC
            String mcc = processMCC(mccRaw);

            if (!city.isEmpty() && !mcc.isEmpty() && !mcc.equals("UNKNOWN_MCC")) {
                // Criar MCCCountWritable com count=1 para esta transação
                MCCCountWritable mccCount = new MCCCountWritable(mcc, 1);

                // Emitir resultado
                outputKey.set(city);
                context.write(outputKey, mccCount);
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
     * Processa e limpa o nome da cidade
     */
    private String processCityName(String cityRaw) {
        if (cityRaw == null || cityRaw.trim().isEmpty()) {
            return "UNKNOWN";
        }

        String city = cityRaw.trim()
                .replace("\"", "")
                .toUpperCase();

        if (city.isEmpty() || city.equals("NULL") || city.equals("N/A")) {
            return "UNKNOWN";
        }

        return city;
    }

    /**
     * Processa e valida o código MCC
     */
    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) {
            return "UNKNOWN_MCC";
        }

        String mcc = mccRaw.trim()
                .replace("\"", "");

        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) {
            return "UNKNOWN_MCC";
        }

        // Validar se é numérico (MCC codes são numéricos)
        if (mcc.matches("\\d+")) {
            return mcc;
        } else {
            return "UNKNOWN_MCC";
        }
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
     * Cleanup - estatísticas finais
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Mapper:");
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