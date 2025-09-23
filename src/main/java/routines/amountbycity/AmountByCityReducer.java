package routines.amountbycity;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class para AmountByCity
 * Agrega os valores transacionados por cidade e emite o resultado final
 */
public class AmountByCityReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    // Objeto reutilizável para o resultado
    private LongWritable result = new LongWritable();

    // Contadores para estatísticas
    private long totalCities = 0;
    private long totalAmountInCents = 0;
    private String highestAmountCity = "";
    private long highestAmount = 0;
    private String lowestAmountCity = "";
    private long lowestAmount = Long.MAX_VALUE;

    /**
     * Método reduce - agrega valores para cada cidade
     * @param key Nome da cidade
     * @param values Lista de valores (em centavos) para esta cidade
     * @param context Contexto para emitir o resultado
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        String cityName = key.toString();
        long cityTotalInCents = 0;
        long transactionCount = 0;

        // Somar todos os valores para esta cidade
        for (LongWritable value : values) {
            cityTotalInCents += value.get();
            transactionCount++;
        }

        // Emitir o resultado (cidade, total_em_centavos)
        result.set(cityTotalInCents);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalCities++;
        totalAmountInCents += cityTotalInCents;

        // Rastrear cidade com maior valor
        if (cityTotalInCents > highestAmount) {
            highestAmount = cityTotalInCents;
            highestAmountCity = cityName;
        }

        // Rastrear cidade com menor valor
        if (cityTotalInCents < lowestAmount) {
            lowestAmount = cityTotalInCents;
            lowestAmountCity = cityName;
        }

        // Log para cidades com alto volume de transações
        if (transactionCount > 1000) {
            context.setStatus("Cidade '" + cityName + "' tem " + transactionCount +
                    " transações totalizando " + formatToCurrency(cityTotalInCents));
        }

        // Log de progresso a cada 100 cidades processadas
        if (totalCities % 100 == 0) {
            context.setStatus("Processadas " + totalCities + " cidades");
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
        System.out.println("  Total de cidades processadas: " + totalCities);
        System.out.println("  Valor total geral: " + formatToCurrency(totalAmountInCents));

        if (totalCities > 0) {
            long averagePerCity = totalAmountInCents / totalCities;
            System.out.println("  Valor médio por cidade: " + formatToCurrency(averagePerCity));

            System.out.println("  Cidade com maior volume:");
            System.out.println("    " + highestAmountCity + ": " + formatToCurrency(highestAmount));

            if (lowestAmount != Long.MAX_VALUE) {
                System.out.println("  Cidade com menor volume:");
                System.out.println("    " + lowestAmountCity + ": " + formatToCurrency(lowestAmount));
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Os valores no arquivo de saída estão em centavos.");
        System.out.println("      Para converter para reais, divida por 100.");
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