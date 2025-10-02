package routines.intermediate.citystatistics;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer para agregação final de estatísticas por cidade
 * Emite resultados formatados com contagem, total e média
 */
public class CityStatisticsReducer extends Reducer<Text, CityStatsWritable, Text, Text> {

    // Objeto reutilizável para resultado
    private Text result = new Text();

    // Estatísticas globais
    private long totalCities = 0;
    private long totalTransactions = 0;
    private long totalAmountInCents = 0;

    // Cidades com maior volume
    private String cityWithMostTransactions = "";
    private long highestTransactionCount = 0;

    private String cityWithHighestTotal = "";
    private long highestTotalAmount = 0;

    private String cityWithHighestAverage = "";
    private long highestAverageAmount = 0;

    private String cityWithLowestAverage = "";
    private long lowestAverageAmount = Long.MAX_VALUE;

    /**
     * Método reduce - agrega estatísticas finais por cidade
     */
    @Override
    protected void reduce(Text key, Iterable<CityStatsWritable> values, Context context)
            throws IOException, InterruptedException {

        String cityName = key.toString();
        long cityTransactionCount = 0;
        long cityTotalAmountInCents = 0;

        // Somar todos os valores para esta cidade
        for (CityStatsWritable stats : values) {
            cityTransactionCount += stats.getTransactionCount();
            cityTotalAmountInCents += stats.getTotalAmountInCents();
        }

        // Calcular média
        long cityAverageInCents = cityTransactionCount > 0 ?
                cityTotalAmountInCents / cityTransactionCount : 0;

        // Criar objeto final com estatísticas agregadas
        CityStatsWritable finalStats = new CityStatsWritable(cityTransactionCount, cityTotalAmountInCents);

        // Formatar resultado
        String formattedResult = finalStats.toOutputString();
        result.set(formattedResult);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalCities++;
        totalTransactions += cityTransactionCount;
        totalAmountInCents += cityTotalAmountInCents;

        // Rastrear cidade com mais transações
        if (cityTransactionCount > highestTransactionCount) {
            highestTransactionCount = cityTransactionCount;
            cityWithMostTransactions = cityName;
        }

        // Rastrear cidade com maior valor total
        if (cityTotalAmountInCents > highestTotalAmount) {
            highestTotalAmount = cityTotalAmountInCents;
            cityWithHighestTotal = cityName;
        }

        // Rastrear cidade com maior ticket médio
        if (cityTransactionCount >= 10 && cityAverageInCents > highestAverageAmount) {
            highestAverageAmount = cityAverageInCents;
            cityWithHighestAverage = cityName;
        }

        // Rastrear cidade com menor ticket médio (mínimo 10 transações)
        if (cityTransactionCount >= 10 && cityAverageInCents < lowestAverageAmount) {
            lowestAverageAmount = cityAverageInCents;
            cityWithLowestAverage = cityName;
        }

        // Log de progresso
        if (totalCities % 100 == 0) {
            context.setStatus("Processadas " + totalCities + " cidades");
        }
    }

    /**
     * Cleanup - emite estatísticas finais
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas Globais do Reducer:");
        System.out.println("  Total de cidades: " + totalCities);
        System.out.println("  Total de transações: " + totalTransactions);
        System.out.println("  Valor total geral: " + formatToCurrency(totalAmountInCents));
        System.out.println();

        if (totalTransactions > 0 && totalCities > 0) {
            long globalAverageInCents = totalAmountInCents / totalTransactions;
            long avgTransactionsPerCity = totalTransactions / totalCities;
            long avgAmountPerCity = totalAmountInCents / totalCities;

            System.out.println("  Médias Globais:");
            System.out.println("    Ticket médio geral: " + formatToCurrency(globalAverageInCents));
            System.out.println("    Transações por cidade: " + avgTransactionsPerCity);
            System.out.println("    Valor médio por cidade: " + formatToCurrency(avgAmountPerCity));
            System.out.println();

            System.out.println("  Rankings:");
            System.out.println("    Cidade com mais transações:");
            System.out.println("      " + cityWithMostTransactions + ": " +
                    highestTransactionCount + " transações");

            System.out.println("    Cidade com maior volume financeiro:");
            System.out.println("      " + cityWithHighestTotal + ": " +
                    formatToCurrency(highestTotalAmount));

            if (!cityWithHighestAverage.isEmpty()) {
                System.out.println("    Cidade com maior ticket médio (min. 10 transações):");
                System.out.println("      " + cityWithHighestAverage + ": " +
                        formatToCurrency(highestAverageAmount));
            }

            if (!cityWithLowestAverage.isEmpty() && lowestAverageAmount != Long.MAX_VALUE) {
                System.out.println("    Cidade com menor ticket médio (min. 10 transações):");
                System.out.println("      " + cityWithLowestAverage + ": " +
                        formatToCurrency(lowestAverageAmount));
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Resultados mostram contagem de transações, valor total");
        System.out.println("      e valor médio por transação para cada cidade.");
        System.out.println("========================================");

        super.cleanup(context);
    }

    /**
     * Converte centavos para formato de moeda legível
     */
    private String formatToCurrency(long centavos) {
        double dollars = centavos / 100.0;
        return String.format("$%.2f", dollars);
    }
}