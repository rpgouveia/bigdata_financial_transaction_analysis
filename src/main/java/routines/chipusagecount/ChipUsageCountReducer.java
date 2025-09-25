package routines.chipusagecount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class para ChipUsageCount
 * Agrega as contagens por tipo de transação e emite o resultado final
 */
public class ChipUsageCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Objeto reutilizável para o resultado
    private IntWritable result = new IntWritable();

    // Contadores para estatísticas
    private long totalTransactionTypes = 0;
    private long totalTransactions = 0;
    private String mostCommonType = "";
    private int highestCount = 0;
    private String leastCommonType = "";
    private int lowestCount = Integer.MAX_VALUE;

    /**
     * Método reduce - agrega contagens para cada tipo de transação
     * @param key Tipo de transação
     * @param values Lista de contagens (sempre 1) para este tipo
     * @param context Contexto para emitir o resultado
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String transactionType = key.toString();
        int typeCount = 0;

        // Somar todas as contagens para este tipo
        for (IntWritable value : values) {
            typeCount += value.get();
        }

        // Emitir o resultado (tipo_transacao, contagem_total)
        result.set(typeCount);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalTransactionTypes++;
        totalTransactions += typeCount;

        // Rastrear tipo mais comum
        if (typeCount > highestCount) {
            highestCount = typeCount;
            mostCommonType = transactionType;
        }

        // Rastrear tipo menos comum
        if (typeCount < lowestCount) {
            lowestCount = typeCount;
            leastCommonType = transactionType;
        }

        // Log para tipos com muitas transações
        if (typeCount > 10000) {
            context.setStatus("Tipo '" + transactionType + "' tem " + typeCount + " transações");
        }

        // Log de progresso
        if (totalTransactionTypes % 10 == 0) {
            context.setStatus("Processados " + totalTransactionTypes + " tipos de transação");
        }
    }

    /**
     * Método cleanup - chamado no final do processamento
     * Emite estatísticas finais
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Reducer:");
        System.out.println("  Total de tipos de transação: " + totalTransactionTypes);
        System.out.println("  Total de transações: " + totalTransactions);

        if (totalTransactionTypes > 0 && totalTransactions > 0) {
            double averagePerType = (double) totalTransactions / totalTransactionTypes;
            System.out.println("  Média de transações por tipo: " + String.format("%.2f", averagePerType));

            System.out.println("  Tipo mais comum:");
            double mostCommonPercentage = (double) highestCount / totalTransactions * 100;
            System.out.println("    " + mostCommonType + ": " + highestCount +
                    " (" + String.format("%.2f%%", mostCommonPercentage) + ")");

            if (lowestCount != Integer.MAX_VALUE) {
                System.out.println("  Tipo menos comum:");
                double leastCommonPercentage = (double) lowestCount / totalTransactions * 100;
                System.out.println("    " + leastCommonType + ": " + lowestCount +
                        " (" + String.format("%.2f%%", leastCommonPercentage) + ")");
            }

            // Análise de distribuição
            if (totalTransactionTypes == 2) {
                System.out.println("  Distribuição (2 tipos detectados):");
                if (mostCommonPercentage > 80) {
                    System.out.println("    Forte predominância do tipo: " + mostCommonType);
                } else if (mostCommonPercentage > 60) {
                    System.out.println("    Moderada predominância do tipo: " + mostCommonType);
                } else {
                    System.out.println("    Distribuição relativamente balanceada");
                }
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Os valores representam contagem total de transações por tipo.");
        System.out.println("========================================");

        super.cleanup(context);
    }
}