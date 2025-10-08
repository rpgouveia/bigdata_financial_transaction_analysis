package routines.intermediate.topcategoriesbycountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;
import routines.intermediate.topcategoriesbycity.MCCDescriptionMapper;
import routines.intermediate.topcategoriesbycity.TopCategoriesResult;

/**
 * Reducer para identificar as top 3 categorias (MCC) por País
 * Demonstra agregação complexa com ranking
 *
 * Para cada país:
 * - Agrega todas as transações por código MCC
 * - Ordena por contagem (decrescente)
 * - Seleciona as top 3 categorias mais frequentes
 * - Emite resultado estruturado como TopCategoriesResult
 *
 * Além disso, calcula estatísticas globais sobre as transações internacionais:
 * - Total de países processados
 * - Total de transações internacionais
 * - País com maior volume de transações
 * - Diversidade comercial (países com mais/menos categorias únicas)
 * - Média de categorias e transações por país
 *
 * Reutiliza classes do pacote topcategoriesbycity:
 * - MCCTransactionCount (input)
 * - TopCategoriesResult (output)
 * - MCCDescriptionMapper (descrições)
 */
public class TopCategoriesByCountryReducer extends Reducer<Text, MCCTransactionCount, Text, TopCategoriesResult> {

    // Objeto reutilizável para resultado
    private Text result = new Text();

    // Estatísticas globais
    private long totalCountries = 0;
    private long totalCategories = 0;
    private long totalTransactions = 0;

    // Rankings globais de volume e diversidade
    private String countryWithMostTransactions = "";
    private long highestTransactionCount = 0;

    private String countryWithMostDiversity = "";
    private int highestUniqueMCCCount = 0;

    private String countryWithLeastDiversity = "";
    private int lowestUniqueMCCCount = Integer.MAX_VALUE;

    /**
     * Método reduce - agrega e ranqueia categorias para cada país
     * @param key Nome do país (ex: "CANADA", "MEXICO")
     * @param values Lista de MCCTransactionCount para este país
     * @param context Contexto para emitir resultado
     */
    @Override
    protected void reduce(Text key, Iterable<MCCTransactionCount> values, Context context)
            throws IOException, InterruptedException {

        String countryName = key.toString();

        // HashMap para agregar contagens por MCC
        Map<String, Long> mccCounts = new HashMap<>();
        long countryTotalTransactions = 0;

        // Agregar todas as contagens para este país
        for (MCCTransactionCount mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();
            mccCounts.put(mcc, mccCounts.getOrDefault(mcc, 0L) + count);
            countryTotalTransactions += count;
        }

        // Converter para lista para sorting
        List<Map.Entry<String, Long>> mccList = new ArrayList<>(mccCounts.entrySet());

        // Ordenar por contagem (decrescente - maior primeiro)
        mccList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Pegar top 3 (ou menos se não houver 3 categorias)
        int topN = Math.min(3, mccList.size());

        // Construr os arrays para o TopCategoriesResult
        String[] topMCCs = new String[topN];
        long[] topCounts = new long[topN];

        for (int i = 0; i < topN; i++) {
            Map.Entry<String, Long> entry = mccList.get(i);
            topMCCs[i] = entry.getKey();
            topCounts[i] = entry.getValue();
        }

        // Criar e emitir o objeto TopCategoriesResult
        TopCategoriesResult topCategories = new TopCategoriesResult(topMCCs, topCounts, topN);
        context.write(key, topCategories);

        // Atualizar estatísticas globais
        totalCountries++;
        totalCategories += mccCounts.size();
        totalTransactions += countryTotalTransactions;

        int uniqueMCCCount = mccCounts.size();

        // Rastrear país com mais transações
        if (countryTotalTransactions > highestTransactionCount) {
            highestTransactionCount = countryTotalTransactions;
            countryWithMostTransactions = countryName;
        }

        // Rastrear país com maior diversidade (mais categorias únicas)
        if (uniqueMCCCount > highestUniqueMCCCount && uniqueMCCCount >= 5) {
            highestUniqueMCCCount = uniqueMCCCount;
            countryWithMostDiversity = countryName;
        }

        // Rastrear país com menor diversidade (menos categorias únicas)
        if (uniqueMCCCount < lowestUniqueMCCCount && uniqueMCCCount >= 1) {
            lowestUniqueMCCCount = uniqueMCCCount;
            countryWithLeastDiversity = countryName;
        }

        // Log de progresso
        if (totalCountries % 20 == 0) {
            context.setStatus("Processados " + totalCountries + " países");
        }
    }

    /**
     * Cleanup - emite estatísticas finais do Reducer
     */
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

            System.out.println("  Rankings Globais (Internacional):");

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
                System.out.println("    Alta diversidade comercial no mercado internacional.");
            } else if (avgCategoriesPerCountry > 8) {
                System.out.println("    Diversidade comercial moderada entre os países.");
            } else {
                System.out.println("    Baixa diversidade (transações concentradas em poucas categorias).");
            }

            if (avgTransactionsPerCountry < 50) {
                System.out.println("    Volume internacional relativamente baixo por país no dataset.");
            } else if (avgTransactionsPerCountry > 200) {
                System.out.println("    Alto volume de transações internacionais por país.");
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