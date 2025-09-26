package routines.basic.amountbyclient;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class para AmountByClient
 * Processa cada linha de transação CSV e emite pares (client_id, valor_em_centavos)
 */
public class AmountByClientMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

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

        try {
            // Usar regex sofisticado para split CSV respeitando aspas
            // Este regex trata campos com vírgulas dentro de aspas corretamente
            String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Verificar se tem o número mínimo de campos esperados
            if (cols.length < 12) {
                invalidRecords++;
                context.setStatus("Linha inválida (poucos campos): " + cols.length + " campos");
                return;
            }

            // Extrair dados da transação
            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String clientIdRaw = cols[2];
            String amountRaw = cols[4];

            // Processar client_id
            String clientId = processClientId(clientIdRaw);

            // Processar valor monetário
            long amountInCents = parseAmountToCents(amountRaw);

            // Se parsing foi bem-sucedido, emitir resultado
            if (amountInCents != Long.MIN_VALUE && !clientId.isEmpty()) {
                outputKey.set(clientId);
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

        // Log de progresso a cada 50000 registros (dataset grande)
        if (recordsProcessed % 50000 == 0) {
            context.setStatus("Processados " + recordsProcessed + " registros. " +
                    "Válidos: " + validRecords + ", Inválidos: " + invalidRecords);
        }
    }

    /**
     * Processa e limpa o client_id
     */
    private String processClientId(String clientIdRaw) {
        if (clientIdRaw == null || clientIdRaw.trim().isEmpty()) {
            return "";  // Será filtrado como inválido
        }

        String clientId = clientIdRaw.trim()
                .replace("\"", "");  // Remove aspas

        if (clientId.isEmpty() || clientId.equals("NULL") || clientId.equals("N/A")) {
            return "";
        }

        return clientId;
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