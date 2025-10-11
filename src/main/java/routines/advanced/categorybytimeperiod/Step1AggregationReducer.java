package routines.advanced.categorybytimeperiod;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;

/**
 * Step 1 Reducer - Agregação de contagens por cidade-período-MCC
 *
 * Este reducer é a primeira etapa do pipeline multi-step.
 * Sua responsabilidade é APENAS agregar (somar) as contagens,
 * sem fazer nenhum ranking ou ordenação.
 *
 * OTIMIZAÇÃO: Usa Custom Writables no output para evitar parsing no Job 2
 *
 * Input:  (CityPeriodKey, MCCTransactionCount)
 * Output: (CityPeriodKey, MCCTransactionCount) - agregado
 */
public class Step1AggregationReducer extends Reducer<CityPeriodKey, MCCTransactionCount, CityPeriodKey, MCCTransactionCount> {

    // Estatísticas
    private long totalRecords = 0;
    private long totalCityPeriods = 0;

    /**
     * Reduce - Soma todas as contagens para cada cidade-período-MCC
     */
    @Override
    protected void reduce(CityPeriodKey key,
                          Iterable<MCCTransactionCount> values,
                          Context context) throws IOException, InterruptedException {

        // Agregar contagens por MCC
        Map<String, Long> mccCounts = new HashMap<>();

        for (MCCTransactionCount mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();
            mccCounts.put(mcc, mccCounts.getOrDefault(mcc, 0L) + count);
        }

        // Emitir cada MCC com sua contagem agregada
        for (Map.Entry<String, Long> entry : mccCounts.entrySet()) {
            MCCTransactionCount aggregated = new MCCTransactionCount(
                    entry.getKey(), entry.getValue());
            context.write(key, aggregated);
            totalRecords++;
        }

        totalCityPeriods++;

        // Log de progresso
        if (totalCityPeriods % 100 == 0) {
            context.setStatus(String.format("Step 1: Processados %d cidade-períodos, %d registros",
                    totalCityPeriods, totalRecords));
        }
    }

    /**
     * Cleanup - estatísticas finais do Job 1
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Step 1 - Agregação Completa:");
        System.out.println("  Cidade-períodos processados: " + totalCityPeriods);
        System.out.println("  Registros agregados emitidos: " + totalRecords);

        if (totalCityPeriods > 0) {
            double avgMccsPerCityPeriod = (double) totalRecords / totalCityPeriods;
            System.out.println("  Média de MCCs por cidade-período: " +
                    String.format("%.2f", avgMccsPerCityPeriod));
        }

        System.out.println();
        System.out.println("  Formato de saída: Custom Writables (binário)");
        System.out.println("  Output: (CityPeriodKey, MCCTransactionCount)");
        System.out.println("  Arquivo intermediário otimizado para Job 2");
        System.out.println("========================================");
        super.cleanup(context);
    }
}