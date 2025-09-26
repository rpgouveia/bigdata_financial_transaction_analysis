package routines.basic.errorcountbymcc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class para ErrorCountByMCC
 * Processa cada linha de transação CSV e emite pares (MCC, 1) apenas para transações com erro
 */
public class ErrorCountByMCCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objetos reutilizáveis para otimização
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;
    private long recordsWithErrors = 0;
    private long recordsWithoutErrors = 0;

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

            String mccRaw = cols[10];
            String errorRaw = cols[11];

            // Processar MCC
            String mcc = processMCC(mccRaw);

            // Processar campo de erro
            String errorStatus = processError(errorRaw);

            // Se há erro válido e MCC válido, emitir resultado
            if (!mcc.isEmpty() && hasError(errorStatus)) {
                outputKey.set(mcc);
                context.write(outputKey, one);
                recordsWithErrors++;
                validRecords++;
            } else if (!mcc.isEmpty() && !hasError(errorStatus)) {
                recordsWithoutErrors++;
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
                    "Válidos: " + validRecords + ", Com erros: " + recordsWithErrors +
                    ", Sem erros: " + recordsWithoutErrors);
        }
    }

    /**
     * Processa e valida o código MCC (Merchant Category Code)
     */
    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) {
            return "UNKNOWN_MCC";
        }

        String mcc = mccRaw.trim()
                .replace("\"", "");  // Remove aspas

        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) {
            return "UNKNOWN_MCC";
        }

        // Validar se é um código MCC numérico válido (normalmente 4 dígitos)
        if (mcc.matches("\\d{4}")) {
            return mcc;
        } else if (mcc.matches("\\d+")) {
            // Se é numérico mas não 4 dígitos, ainda aceitar
            return mcc;
        } else {
            // Se não é numérico, usar como está mas indicar que é não-padrão
            return mcc.toUpperCase();
        }
    }

    /**
     * Processa o campo de erro
     */
    private String processError(String errorRaw) {
        if (errorRaw == null || errorRaw.trim().isEmpty()) {
            return "NO_ERROR";
        }

        String error = errorRaw.trim()
                .replace("\"", "");  // Remove aspas

        if (error.isEmpty()) {
            return "NO_ERROR";
        }

        return error;
    }

    /**
     * Determina se há erro baseado no conteúdo do campo de erro
     */
    private boolean hasError(String errorStatus) {
        if (errorStatus == null || errorStatus.isEmpty()) {
            return false;
        }

        // Considera que há erro se o campo não está vazio e não é "NO_ERROR"
        return !"NO_ERROR".equalsIgnoreCase(errorStatus.trim());
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
        System.out.println("  Registros com erros: " + recordsWithErrors);
        System.out.println("  Registros sem erros: " + recordsWithoutErrors);

        if (recordsProcessed > 0) {
            double successRate = (double) validRecords / recordsProcessed * 100;
            double errorRate = (double) recordsWithErrors / validRecords * 100;
            System.out.println("  Taxa de sucesso: " + String.format("%.2f%%", successRate));
            System.out.println("  Taxa de erro: " + String.format("%.2f%%", errorRate));
        }

        System.out.println("========================================");
        super.cleanup(context);
    }
}