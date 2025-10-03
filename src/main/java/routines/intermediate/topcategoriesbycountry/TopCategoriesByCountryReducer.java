package routines.intermediate.topcategoriesbycountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCCountWritable;
import routines.intermediate.topcategoriesbycity.MCCCodeMapper;

/**
 * Reducer para identificar as top 3 categorias (MCC) por País
 * Demonstra agregação complexa com ranking
 */
public class TopCategoriesByCountryReducer extends Reducer<Text, MCCCountWritable, Text, Text> {

    private Text result = new Text();

    // Estatísticas globais
    private long totalCountries = 0;
    private long totalCategories = 0;
    private long totalTransactions = 0;

    // Rankings globais
    private String countryWithMostTransactions = "";
    private long highestTransactionCount = 0;

    private String countryWithMostDiversity = "";
    private int highestUniqueMCCCount = 0;

    private String countryWithLeastDiversity = "";
    private int lowestUniqueMCCCount = Integer.MAX_VALUE;

    @Override
    protected void reduce(Text key, Iterable<MCCCountWritable> values, Context context)
            throws IOException, InterruptedException {

        String countryName = key.toString();

        // Agregar contagens por MCC
        Map<String, Long> mccCounts = new HashMap<>();
        long countryTotalTransactions = 0;

        for (MCCCountWritable mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();
            mccCounts.put(mcc, mccCounts.getOrDefault(mcc, 0L) + count);
            countryTotalTransactions += count;
        }

        // Ordenar por contagem (decrescente)
        List<Map.Entry<String, Long>> mccList = new ArrayList<>(mccCounts.entrySet());
        mccList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Top 3
        int topN = Math.min(3, mccList.size());

        // Construir linha de resultado
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topN; i++) {
            Map.Entry<String, Long> entry = mccList.get(i);
            String mcc = entry.getKey();
            long count = entry.getValue();

            String description = MCCCodeMapper.getDescription(mcc);

            if (i > 0) sb.append(" | ");
            sb.append(String.format("Top-%d: %s (%s) %d", i + 1, mcc, description, count));
        }

        result.set(sb.toString());
        context.write(key, result);

        // Estatísticas globais
        totalCountries++;
        totalCategories += mccCounts.size();
        totalTransactions += countryTotalTransactions;

        int uniqueMCCCount = mccCounts.size();

        // País com mais transações
        if (countryTotalTransactions > highestTransactionCount) {
            highestTransactionCount = countryTotalTransactions;
            countryWithMostTransactions = countryName;
        }

        // País com maior diversidade
        if (uniqueMCCCount > highestUniqueMCCCount && uniqueMCCCount >= 5) {
            highestUniqueMCCCount = uniqueMCCCount;
            countryWithMostDiversity = countryName;
        }

        // País com menor diversidade
        if (uniqueMCCCount < lowestUniqueMCCCount && uniqueMCCCount >= 1) {
            lowestUniqueMCCCount = uniqueMCCCount;
            countryWithLeastDiversity = countryName;
        }

        if (totalCountries % 20 == 0) {
            context.setStatus("Processados " + totalCountries + " países");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas Globais do Reducer (por PAÍS):");
        System.out.println("  Total de países: " + totalCountries);
        System.out.println("  Total de categorias únicas processadas: " + totalCategories);
        System.out.println("  Total de transações internacionais: " + totalTransactions);

        if (totalCountries > 0) {
            double avgCategoriesPerCountry = (double) totalCategories / totalCountries;
            double avgTransactionsPerCountry = (double) totalTransactions / totalCountries;

            System.out.println("  Média de categorias por país: " + String.format("%.2f", avgCategoriesPerCountry));
            System.out.println("  Média de transações por país: " + String.format("%.2f", avgTransactionsPerCountry));
            System.out.println();

            System.out.println("  Rankings Globais:");

            if (!countryWithMostTransactions.isEmpty()) {
                System.out.println("    País com mais transações:");
                System.out.println("      " + countryWithMostTransactions + ": " +
                        highestTransactionCount + " transações");
            }

            if (!countryWithMostDiversity.isEmpty()) {
                System.out.println("    País com maior diversidade comercial:");
                System.out.println("      " + countryWithMostDiversity + ": " +
                        highestUniqueMCCCount + " categorias diferentes");
            }

            if (!countryWithLeastDiversity.isEmpty() && lowestUniqueMCCCount != Integer.MAX_VALUE) {
                System.out.println("    País com menor diversidade comercial:");
                System.out.println("      " + countryWithLeastDiversity + ": " +
                        lowestUniqueMCCCount + " categorias diferentes");
            }

            System.out.println();
            System.out.println("  Insights:");
            if (avgCategoriesPerCountry > 15) {
                System.out.println("    Alta diversidade comercial internacional");
            } else if (avgCategoriesPerCountry > 8) {
                System.out.println("    Diversidade comercial moderada");
            } else {
                System.out.println("    Baixa diversidade (transações concentradas em poucas categorias)");
            }

            if (avgTransactionsPerCountry < 50) {
                System.out.println("    Volume internacional relativamente baixo no dataset");
            } else if (avgTransactionsPerCountry > 200) {
                System.out.println("    Alto volume de transações internacionais");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Resultados mostram as top 3 categorias mais frequentes");
        System.out.println("      em cada PAÍS, baseado em códigos MCC.");
        System.out.println("      Exclui transações dos estados dos EUA.");
        System.out.println("========================================");

        super.cleanup(context);
    }
}