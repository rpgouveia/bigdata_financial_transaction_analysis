package routines.intermediate.topcategoriesbycity;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer para identificar as top 3 categorias (MCC) por cidade
 * Demonstra agregação complexa com ranking
 * Emite TopCategoriesResult
 */
public class TopCategoriesByCityReducer extends Reducer<Text, MCCTransactionCount, Text, TopCategoriesResult> {

    // Objeto reutilizável para resultado
    private TopCategoriesResult result = new TopCategoriesResult();

    // Estatísticas globais
    private long totalCities = 0;
    private long totalCategories = 0;

    // Rankings globais
    private String cityWithMostDiversity = "";
    private int highestUniqueMCCCount = 0;

    private String cityWithLeastDiversity = "";
    private int lowestUniqueMCCCount = Integer.MAX_VALUE;

    /**
     * Método reduce - identifica top 3 categorias para cada cidade
     */
    @Override
    protected void reduce(Text key, Iterable<MCCTransactionCount> values, Context context)
            throws IOException, InterruptedException {

        String cityName = key.toString();

        // HashMap para agregar contagens por MCC
        Map<String, Long> mccCounts = new HashMap<>();

        // Agregar todas as contagens para esta cidade
        for (MCCTransactionCount mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();

            mccCounts.put(mcc, mccCounts.getOrDefault(mcc, 0L) + count);
        }

        // Converter para lista para sorting
        List<Map.Entry<String, Long>> mccList = new ArrayList<>(mccCounts.entrySet());

        // Ordenar por contagem (decrescente)
        mccList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Pegar top 3 (ou menos se não houver 3 categorias)
        int topN = Math.min(3, mccList.size());

        // Construir arrays para o TopCategoriesWritable
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

        // Atualizar estatísticas globais
        totalCities++;
        totalCategories += mccCounts.size();

        // Rastrear diversidade de categorias
        int uniqueMCCCount = mccCounts.size();

        if (uniqueMCCCount > highestUniqueMCCCount && uniqueMCCCount >= 5) {
            highestUniqueMCCCount = uniqueMCCCount;
            cityWithMostDiversity = cityName;
        }

        if (uniqueMCCCount < lowestUniqueMCCCount && uniqueMCCCount >= 1) {
            lowestUniqueMCCCount = uniqueMCCCount;
            cityWithLeastDiversity = cityName;
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
        System.out.println("  Total de categorias únicas processadas: " + totalCategories);

        if (totalCities > 0) {
            double avgCategoriesPerCity = (double) totalCategories / totalCities;
            System.out.println("  Média de categorias por cidade: " +
                    String.format("%.2f", avgCategoriesPerCity));
            System.out.println();

            System.out.println("  Diversidade de Mercado:");

            if (!cityWithMostDiversity.isEmpty()) {
                System.out.println("    Cidade com maior diversidade:");
                System.out.println("      " + cityWithMostDiversity + ": " +
                        highestUniqueMCCCount + " categorias diferentes");
            }

            if (!cityWithLeastDiversity.isEmpty() && lowestUniqueMCCCount != Integer.MAX_VALUE) {
                System.out.println("    Cidade com menor diversidade:");
                System.out.println("      " + cityWithLeastDiversity + ": " +
                        lowestUniqueMCCCount + " categorias diferentes");
            }

            System.out.println();
            System.out.println("  Insights:");
            if (avgCategoriesPerCity > 20) {
                System.out.println("    Alta diversidade comercial no dataset");
            } else if (avgCategoriesPerCity > 10) {
                System.out.println("    Diversidade comercial moderada");
            } else {
                System.out.println("    Baixa diversidade comercial (poucas categorias)");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Resultados mostram as top 3 categorias mais frequentes");
        System.out.println("      em cada cidade, baseado em códigos MCC.");
        System.out.println("========================================");

        super.cleanup(context);
    }
}