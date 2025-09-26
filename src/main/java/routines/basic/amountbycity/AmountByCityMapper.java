package routines.basic.amountbycity;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class para AmountByCity
 * Processa cada linha de transação CSV e emite pares (cidade, valor_em_centavos)
 */
public class AmountByCityMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // Objetos reutilizáveis para otimização
    private Text outputKey = new Text();
    private LongWritable outputValue = new LongWritable();

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

        // Fazer parsing da linha CSV
        String[] parts = splitCsv(line);

        // Verificar se tem o número mínimo de campos esperados
        if (parts.length < 12) {
            invalidRecords++;
            context.setStatus("Linha inválida (poucos campos): " + parts.length + " campos");
            return;
        }

        try {
            // Extrair dados da transação
            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String cityRaw = parts[7];
            String amountRaw = parts[4];

            // Processar cidade
            String city = processCityName(cityRaw);

            // Processar valor monetário
            long amountInCents = parseAmountToCents(amountRaw);

            // Se parsing foi bem-sucedido, emitir resultado
            if (amountInCents != Long.MIN_VALUE && !city.isEmpty()) {
                outputKey.set(city);
                outputValue.set(amountInCents);
                context.write(outputKey, outputValue);
                validRecords++;
            } else {
                invalidRecords++;
            }

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Erro processando linha: " + e.getMessage());
        }

        // Log de progresso a cada 10000 registros
        if (recordsProcessed % 10000 == 0) {
            context.setStatus("Processados " + recordsProcessed + " registros. " +
                    "Válidos: " + validRecords + ", Inválidos: " + invalidRecords);
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
                .replace("\"", "")  // Remove aspas
                .toUpperCase();     // Padroniza em maiúsculas

        if (city.isEmpty() || city.equals("NULL") || city.equals("N/A")) {
            return "UNKNOWN";
        }

        return city;
    }

    /**
     * Split de CSV que respeita aspas e trata campos com vírgulas
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
     * Converte string de valor monetário para centavos (long)
     * Formato esperado: $14.57 (formato americano do dataset Kaggle)
     */
    private static long parseAmountToCents(String rawAmount) {
        if (rawAmount == null || rawAmount.trim().isEmpty()) {
            return Long.MIN_VALUE;
        }

        try {
            String cleanAmount = rawAmount.trim()
                    .replace("\"", "")      // Remove aspas
                    .replace("$", "")       // Remove símbolo Dólar
                    .replace(" ", "");      // Remove espaços

            if (cleanAmount.isEmpty()) {
                return Long.MIN_VALUE;
            }

            // Formato americano: 1,234.56 (vírgula = separador de milhar, ponto = decimal)
            // Remove vírgulas que são separadores de milhares
            cleanAmount = cleanAmount.replace(",", "");

            // Converter para BigDecimal para precisão
            BigDecimal amount = new BigDecimal(cleanAmount);

            // Converter para centavos (multiplicar por 100)
            BigDecimal amountInCents = amount.movePointRight(2);

            // Arredondar e converter para long
            return amountInCents.setScale(0, RoundingMode.HALF_UP).longValueExact();

        } catch (Exception e) {
            // Em caso de erro de parsing, retornar valor inválido
            return Long.MIN_VALUE;
        }
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