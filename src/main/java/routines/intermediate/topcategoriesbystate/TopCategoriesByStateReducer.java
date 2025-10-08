package routines.intermediate.topcategoriesbystate;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;
import routines.intermediate.topcategoriesbycity.TopCategoriesResult;
import routines.intermediate.topcategoriesbycity.MCCDescriptionMapper;

/**
 * Reducer para identificar as top 3 categorias (MCC) por Estado
 * Demonstra agregação complexa com ranking
 */
public class TopCategoriesByStateReducer extends Reducer<Text, MCCTransactionCount, Text, TopCategoriesResult> {

    private TopCategoriesResult result = new TopCategoriesResult();

    // Estatísticas globais
    private long totalStates = 0;
    private long totalCategories = 0;

    // Rankings globais de diversidade
    private String stateWithMostDiversity = "";
    private int highestUniqueMCCCount = 0;

    private String stateWithLeastDiversity = "";
    private int lowestUniqueMCCCount = Integer.MAX_VALUE;

    @Override
    protected void reduce(Text key, Iterable<MCCTransactionCount> values, Context context)
            throws IOException, InterruptedException {

        String stateName = key.toString();

        // Agregar contagens por MCC
        Map<String, Long> mccCounts = new HashMap<>();
        for (MCCTransactionCount mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();
            mccCounts.put(mcc, mccCounts.getOrDefault(mcc, 0L) + count);
        }

        // Ordenar por contagem (decrescente)
        List<Map.Entry<String, Long>> mccList = new ArrayList<>(mccCounts.entrySet());
        mccList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Top 3
        int topN = Math.min(3, mccList.size());

        // Construir arrays para o TopCategoriesResult
        String[] topMCCs = new String[topN];
        long[] topCounts = new long[topN];

        for (int i = 0; i < topN; i++) {
            Map.Entry<String, Long> entry = mccList.get(i);
            topMCCs[i] = entry.getKey();
            topCounts[i] = entry.getValue();
        }

        // Criar e emitir TopCategoriesResult
        TopCategoriesResult topCategories = new TopCategoriesResult(topMCCs, topCounts, topN);
        context.write(key, topCategories);

        // Estatísticas globais
        totalStates++;
        totalCategories += mccCounts.size();

        int uniqueMCCCount = mccCounts.size();

        if (uniqueMCCCount > highestUniqueMCCCount && uniqueMCCCount >= 5) {
            highestUniqueMCCCount = uniqueMCCCount;
            stateWithMostDiversity = stateName;
        }

        if (uniqueMCCCount < lowestUniqueMCCCount && uniqueMCCCount >= 1) {
            lowestUniqueMCCCount = uniqueMCCCount;
            stateWithLeastDiversity = stateName;
        }

        if (totalStates % 100 == 0) {
            context.setStatus("Processados " + totalStates + " estados");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas Globais do Reducer (por ESTADO):");
        System.out.println("  Total de estados: " + totalStates);
        System.out.println("  Total de categorias únicas processadas: " + totalCategories);

        if (totalStates > 0) {
            double avgCategoriesPerState = (double) totalCategories / totalStates;
            System.out.println("  Média de categorias por estado: " + String.format("%.2f", avgCategoriesPerState));
            System.out.println();

            System.out.println("  Diversidade de Mercado:");
            if (!stateWithMostDiversity.isEmpty()) {
                System.out.println("    Estado com maior diversidade:");
                System.out.println("      " + stateWithMostDiversity + ": " + highestUniqueMCCCount + " categorias diferentes");
            }
            if (!stateWithLeastDiversity.isEmpty() && lowestUniqueMCCCount != Integer.MAX_VALUE) {
                System.out.println("    Estado com menor diversidade:");
                System.out.println("      " + stateWithLeastDiversity + ": " + lowestUniqueMCCCount + " categorias diferentes");
            }

            System.out.println();
            System.out.println("  Insights:");
            if (avgCategoriesPerState > 20) {
                System.out.println("    Alta diversidade comercial no dataset (nível estado)");
            } else if (avgCategoriesPerState > 10) {
                System.out.println("    Diversidade comercial moderada (nível estado)");
            } else {
                System.out.println("    Baixa diversidade comercial (nível estado)");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Resultados mostram as top 3 categorias mais frequentes");
        System.out.println("      em cada ESTADO, baseado em códigos MCC.");
        System.out.println("========================================");

        super.cleanup(context);
    }
}