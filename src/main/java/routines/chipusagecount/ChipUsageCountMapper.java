package routines.chipusagecount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class para ChipUsageCount
 * Processa cada linha de transação CSV e emite pares (tipo_transacao, 1)
 */
public class ChipUsageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objetos reutilizáveis para otimização
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    /**
     * Método map - processa cada linha do arquivo CSV
     * @param key Offset da linha no arquivo
     * @param value Conteúdo da linha CSV
     * @param context Contexto para emitir resultados
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        // Ignorar cabeçalho do CSV
        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            // Fazer parsing da linha CSV usando a função robusta
            String[] parts = splitCsv(line);

            // Verificar se tem o número mínimo de campos esperados
            if (parts.length < 12) {
                invalidRecords++;
                context.setStatus("Linha inválida (poucos campos): " + parts.length + " campos");
                return;
            }

            // Extrair dados da transação
            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String useChipRaw = parts[5];

            // Processar tipo de transação
            String transactionType = processTransactionType(useChipRaw);

            // Se processamento foi bem-sucedido, emitir resultado
            if (!transactionType.isEmpty()) {
                outputKey.set(transactionType);
                context.write(outputKey, one);
                validRecords++;
            } else {
                invalidRecords++;
            }

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Erro processando linha: " + e.getMessage());
        }

        // Log de progresso a cada 50000 registros
        if (recordsProcessed % 50000 == 0) {
            context.setStatus("Processados " + recordsProcessed + " registros. " +
                    "Válidos: " + validRecords + ", Inválidos: " + invalidRecords);
        }
    }

    /**
     * Processa e classifica o tipo de transação baseado no campo use_chip
     */
    private String processTransactionType(String useChipRaw) {
        if (useChipRaw == null || useChipRaw.trim().isEmpty()) {
            return "Unknown Transaction";
        }

        String useChip = useChipRaw.trim()
                .replace("\"", "")  // Remove aspas
                .toUpperCase();     // Padroniza em maiúsculas

        // Se já está em formato descritivo, manter
        if (useChip.contains("TRANSACTION")) {
            return capitalizeWords(useChip);
        }

        // Mapear valores simples para descrições
        switch (useChip) {
            case "Y":
            case "YES":
            case "TRUE":
            case "1":
                return "Chip Transaction";

            case "N":
            case "NO":
            case "FALSE":
            case "0":
                return "Swipe Transaction";

            case "ONLINE":
                return "Online Transaction";

            case "CONTACTLESS":
                return "Contactless Transaction";

            case "NULL":
            case "N/A":
            case "":
                return "Unknown Transaction";

            default:
                // Se não reconhecido, usar valor original capitalizado
                return capitalizeWords(useChip + " Transaction");
        }
    }

    /**
     * Capitaliza palavras para formatação consistente
     */
    private String capitalizeWords(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        StringBuilder result = new StringBuilder();
        String[] words = input.toLowerCase().split("\\s+");

        for (int i = 0; i < words.length; i++) {
            if (i > 0) {
                result.append(" ");
            }
            if (!words[i].isEmpty()) {
                result.append(Character.toUpperCase(words[i].charAt(0)));
                if (words[i].length() > 1) {
                    result.append(words[i].substring(1));
                }
            }
        }

        return result.toString();
    }

    /**
     * Split de CSV que respeita aspas e trata campos com vírgulas
     * Mantém a implementação robusta do código original
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

        // Adicionar último campo
        result.add(currentField.toString());

        return result.toArray(new String[0]);
    }

    /**
     * Método cleanup - chamado no final do processamento
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