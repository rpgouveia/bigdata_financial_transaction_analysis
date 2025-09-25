package routines.amountbyclient;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class para AmountByClient
 * Agrega os valores transacionados por cliente e emite o resultado final formatado
 */
public class AmountByClientReducer extends Reducer<Text, LongWritable, Text, Text> {

    // Objeto reutilizável para o resultado
    private Text result = new Text();

    // Formatador para duas casas decimais
    private DecimalFormat decimalFormat = new DecimalFormat("0.00");

    // Contadores para estatísticas
    private long totalClients = 0;
    private long totalAmountInCents = 0;
    private String highestAmountClient = "";
    private long highestAmount = 0;
    private String lowestAmountClient = "";
    private long lowestAmount = Long.MAX_VALUE;
    private long totalTransactions = 0;

    /**
     * Método reduce - agrega valores para cada cliente
     * @param key ID do cliente
     * @param values Lista de valores (em centavos) para este cliente
     * @param context Contexto para emitir o resultado
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        String clientId = key.toString();
        long clientTotalInCents = 0;
        long clientTransactionCount = 0;

        // Somar todos os valores para este cliente
        for (LongWritable value : values) {
            clientTotalInCents += value.get();
            clientTransactionCount++;
            totalTransactions++;
        }

        // Converter centavos para dólares e formatar com 2 casas decimais
        double clientTotalInDollars = clientTotalInCents / 100.0;
        String formattedAmount = decimalFormat.format(clientTotalInDollars);

        // Emitir o resultado (cliente, valor_formatado)
        result.set(formattedAmount);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalClients++;
        totalAmountInCents += clientTotalInCents;

        // Rastrear cliente com maior valor
        if (clientTotalInCents > highestAmount) {
            highestAmount = clientTotalInCents;
            highestAmountClient = clientId;
        }

        // Rastrear cliente com menor valor (incluindo valores negativos)
        if (clientTotalInCents < lowestAmount) {
            lowestAmount = clientTotalInCents;
            lowestAmountClient = clientId;
        }

        // Log para clientes com alto número de transações
        if (clientTransactionCount > 100) {
            context.setStatus("Cliente '" + clientId + "' tem " + clientTransactionCount +
                    " transações totalizando " + formatToCurrency(clientTotalInCents));
        }

        // Log de progresso a cada 10000 clientes processados
        if (totalClients % 10000 == 0) {
            context.setStatus("Processados " + totalClients + " clientes");
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
        System.out.println("  Total de clientes processados: " + totalClients);
        System.out.println("  Total de transações: " + totalTransactions);
        System.out.println("  Valor total geral: " + formatToCurrency(totalAmountInCents));

        if (totalClients > 0) {
            long averagePerClient = totalAmountInCents / totalClients;
            double averageTransactionsPerClient = (double) totalTransactions / totalClients;

            System.out.println("  Valor médio por cliente: " + formatToCurrency(averagePerClient));
            System.out.println("  Transações médias por cliente: " + String.format("%.2f", averageTransactionsPerClient));

            System.out.println("  Cliente com maior volume:");
            System.out.println("    " + highestAmountClient + ": " + formatToCurrency(highestAmount));

            if (lowestAmount != Long.MAX_VALUE) {
                System.out.println("  Cliente com menor volume:");
                System.out.println("    " + lowestAmountClient + ": " + formatToCurrency(lowestAmount));

                // Alertar sobre valores negativos
                if (lowestAmount < 0) {
                    System.out.println("  ATENÇÃO: Detectado valor negativo - possíveis estornos/reembolsos");
                }
            }

            // Estatísticas adicionais sobre distribuição
            if (totalAmountInCents > 0) {
                double percentageTopClient = (double) highestAmount / totalAmountInCents * 100;
                System.out.println("  Cliente top representa " + String.format("%.2f%%", percentageTopClient) + " do volume total");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Os valores no arquivo de saída estão formatados em dólares com 2 casas decimais.");
        System.out.println("========================================");

        super.cleanup(context);
    }

    /**
     * Converte centavos para formato de moeda legível (apenas para logs)
     */
    private String formatToCurrency(long centavos) {
        double dollars = centavos / 100.0;
        return String.format("$%.2f", dollars);
    }
}