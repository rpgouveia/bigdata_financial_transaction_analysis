package routines.errorcountbymcc;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class para ErrorCountByMCC
 * Agrega as contagens de erro por MCC e emite o resultado final
 */
public class ErrorCountByMCCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Objeto reutilizável para o resultado
    private IntWritable result = new IntWritable();

    // Contadores para estatísticas
    private long totalMCCs = 0;
    private long totalErrors = 0;
    private String mostErrorProneMCC = "";
    private int highestErrorCount = 0;
    private String leastErrorProneMCC = "";
    private int lowestErrorCount = Integer.MAX_VALUE;

    /**
     * Método reduce - agrega contagens de erro para cada MCC
     * @param key Código MCC (Merchant Category Code)
     * @param values Lista de contagens (sempre 1) para este MCC
     * @param context Contexto para emitir o resultado
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String mccCode = key.toString();
        int errorCount = 0;

        // Somar todas as contagens de erro para este MCC
        for (IntWritable value : values) {
            errorCount += value.get();
        }

        // Emitir o resultado (MCC, contagem_de_erros)
        result.set(errorCount);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalMCCs++;
        totalErrors += errorCount;

        // Rastrear MCC com mais erros
        if (errorCount > highestErrorCount) {
            highestErrorCount = errorCount;
            mostErrorProneMCC = mccCode;
        }

        // Rastrear MCC com menos erros
        if (errorCount < lowestErrorCount) {
            lowestErrorCount = errorCount;
            leastErrorProneMCC = mccCode;
        }

        // Log para MCCs com muitos erros
        if (errorCount > 100) {
            context.setStatus("MCC '" + mccCode + "' tem " + errorCount + " erros");
        }

        // Log de progresso a cada 50 MCCs processados
        if (totalMCCs % 50 == 0) {
            context.setStatus("Processados " + totalMCCs + " códigos MCC");
        }
    }

    /**
     * Método cleanup - chamado no final do processamento
     * Emite estatísticas finais com descrições MCC reais
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Reducer:");
        System.out.println("  Total de códigos MCC com erros: " + totalMCCs);
        System.out.println("  Total de erros processados: " + totalErrors);

        if (totalMCCs > 0 && totalErrors > 0) {
            double averageErrorsPerMCC = (double) totalErrors / totalMCCs;
            System.out.println("  Média de erros por MCC: " + String.format("%.2f", averageErrorsPerMCC));

            // Usar descrições reais dos MCCs
            System.out.println("  MCC com mais erros:");
            String mostErrorDescription = MCCDescriptionMapper.getDescription(mostErrorProneMCC);
            String mostErrorCause = MCCDescriptionMapper.analyzeErrorCause(mostErrorProneMCC);
            double mostErrorPronePercentage = (double) highestErrorCount / totalErrors * 100;
            System.out.println("    " + mostErrorProneMCC + " (" + mostErrorDescription + ")");
            System.out.println("    Erros: " + highestErrorCount +
                    " (" + String.format("%.2f%%", mostErrorPronePercentage) + " do total)");
            System.out.println("    Possível causa: " + mostErrorCause);

            if (lowestErrorCount != Integer.MAX_VALUE) {
                System.out.println("  MCC com menos erros:");
                String leastErrorDescription = MCCDescriptionMapper.getDescription(leastErrorProneMCC);
                double leastErrorPronePercentage = (double) lowestErrorCount / totalErrors * 100;
                System.out.println("    " + leastErrorProneMCC + " (" + leastErrorDescription + ")");
                System.out.println("    Erros: " + lowestErrorCount +
                        " (" + String.format("%.2f%%", leastErrorPronePercentage) + " do total)");
            }

            // Análise de distribuição por categoria
            System.out.println("  Análise por categoria:");
            String mostErrorCategory = MCCDescriptionMapper.getGeneralCategory(mostErrorProneMCC);
            System.out.println("    Categoria com mais erros: " + mostErrorCategory);

            if (mostErrorPronePercentage > 30) {
                System.out.println("    Concentração alta em " + mostErrorDescription);
                System.out.println("    Recomendação: Investigar sistemas específicos desta categoria");
            } else if (mostErrorPronePercentage < 5) {
                System.out.println("    Erros bem distribuídos entre diferentes categorias");
                System.out.println("    Recomendação: Problema sistêmico geral");
            } else {
                System.out.println("    Distribuição moderada de erros");
                System.out.println("    Recomendação: Focar nas categorias principais");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Apenas MCCs com erros aparecem nos resultados.");
        System.out.println("      Códigos MCC representam categorias de comerciantes específicas.");
        System.out.println("      Use MCCDescriptionMapper para interpretar códigos adicionais.");
        System.out.println("========================================");

        super.cleanup(context);
    }

    /**
     * Remove o método interpretCommonMCCs() antigo - agora usa MCCDescriptionMapper
     */
}