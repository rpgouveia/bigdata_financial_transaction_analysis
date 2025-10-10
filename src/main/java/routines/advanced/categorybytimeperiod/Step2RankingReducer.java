package routines.advanced.categorybytimeperiod;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;
import routines.intermediate.topcategoriesbycity.TopCategoriesResult;

/**
 * Step 2 Reducer - Ranking das Top 3 categorias por cidade-período
 *
 * Este reducer é a etapa final do pipeline multi-step.
 * Ele recebe os dados agregados do Job 1 (via Step2Mapper) e
 * identifica as 3 categorias mais frequentes para cada cidade-período.
 *
 * Input:  (CityPeriodKey, MCCTransactionCount)
 * Output: (Text, TopCategoriesResult) - Top 3 categorias formatadas
 */
public class Step2RankingReducer extends Reducer<CityPeriodKey, MCCTransactionCount, Text, TopCategoriesResult> {

    // Estatísticas globais
    private long totalCityPeriods = 0;
    private String mostDiverseCityPeriod = "";
    private int highestCategoryCount = 0;

    // Análise por período
    private Map<String, Long> transactionsByPeriod = new HashMap<>();
    private Map<String, Integer> citiesByPeriod = new HashMap<>();

    /**
     * Reduce - Identifica top 3 categorias para cada cidade-período
     */
    @Override
    protected void reduce(CityPeriodKey key,
                          Iterable<MCCTransactionCount> values,
                          Context context) throws IOException, InterruptedException {

        String displayKey = key.toDisplayString();
        String timePeriod = key.getTimePeriod();

        // Lista para ordenar
        List<MCCTransactionCount> mccList = new ArrayList<>();
        long totalTransactions = 0;

        // Coletar todos os MCCs (já agregados pelo Job 1)
        for (MCCTransactionCount mcc : values) {
            // Criar cópia porque Hadoop reutiliza objetos
            MCCTransactionCount copy = new MCCTransactionCount(
                    mcc.getMccCode(),
                    mcc.getCount()
            );
            mccList.add(copy);
            totalTransactions += mcc.getCount();
        }

        // Ordenar por contagem (decrescente)
        mccList.sort((a, b) -> Long.compare(b.getCount(), a.getCount()));

        // Pegar top 3
        int topN = Math.min(3, mccList.size());
        String[] topMCCs = new String[topN];
        long[] topCounts = new long[topN];

        for (int i = 0; i < topN; i++) {
            topMCCs[i] = mccList.get(i).getMccCode();
            topCounts[i] = mccList.get(i).getCount();
        }

        // Criar e emitir resultado
        TopCategoriesResult result = new TopCategoriesResult(topMCCs, topCounts, topN);
        context.write(new Text(displayKey), result);

        // Atualizar estatísticas
        totalCityPeriods++;

        int uniqueCategories = mccList.size();
        if (uniqueCategories > highestCategoryCount) {
            highestCategoryCount = uniqueCategories;
            mostDiverseCityPeriod = displayKey;
        }

        // Estatísticas por período
        transactionsByPeriod.put(timePeriod,
                transactionsByPeriod.getOrDefault(timePeriod, 0L) + totalTransactions);
        citiesByPeriod.put(timePeriod,
                citiesByPeriod.getOrDefault(timePeriod, 0) + 1);

        // Log de progresso
        if (totalCityPeriods % 100 == 0) {
            context.setStatus("Step 2: Processados " + totalCityPeriods + " cidade-períodos");
        }
    }

    /**
     * Cleanup - estatísticas finais do Job 2
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Step 2 - Ranking Completo:");
        System.out.println("  Total de cidade-períodos: " + totalCityPeriods);
        System.out.println();

        if (!mostDiverseCityPeriod.isEmpty()) {
            System.out.println("  Maior diversidade:");
            System.out.println("    " + mostDiverseCityPeriod + ": " +
                    highestCategoryCount + " categorias");
            System.out.println();
        }

        // Análise por período
        System.out.println("  Resumo por Período:");
        long totalTransactions = transactionsByPeriod.values().stream()
                .mapToLong(Long::longValue).sum();

        for (String period : Arrays.asList("MORNING", "AFTERNOON", "NIGHT")) {
            String periodName = getPeriodName(period);
            long periodTxns = transactionsByPeriod.getOrDefault(period, 0L);
            int cityCount = citiesByPeriod.getOrDefault(period, 0);

            if (periodTxns > 0) {
                double pct = (periodTxns * 100.0) / totalTransactions;
                System.out.println(String.format("    %s: %d transações (%.2f%%) em %d cidades",
                        periodName, periodTxns, pct, cityCount));
            }
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("Multi-Step Processing Completo!");
        System.out.println("Resultado: Top 3 categorias por cidade-período");
        System.out.println("========================================");

        super.cleanup(context);
    }

    /**
     * Converte código do período para nome legível
     */
    private String getPeriodName(String period) {
        switch (period) {
            case "MORNING":
                return "Manhã";
            case "AFTERNOON":
                return "Tarde";
            case "NIGHT":
                return "Noite";
            default:
                return period;
        }
    }
}