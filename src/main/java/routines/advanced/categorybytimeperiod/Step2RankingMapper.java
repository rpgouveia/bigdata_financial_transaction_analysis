package routines.advanced.categorybytimeperiod;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;

/**
 * Step 2 Mapper - Lê resultado do Job 1 e prepara para ranking
 *
 * Este mapper é a segunda etapa do pipeline multi-step.
 * Ele lê o arquivo intermediário gerado pelo Job 1 e reconstrói
 * as estruturas de dados necessárias para o ranking no Reducer.
 *
 * Input:  Arquivo do Job 1 (formato: "CITY\tPERIOD\tMCC\tCOUNT")
 * Output: (CityPeriodKey, MCCTransactionCount)
 */
public class Step2RankingMapper extends Mapper<LongWritable, Text, CityPeriodKey, MCCTransactionCount> {

    private CityPeriodKey outputKey = new CityPeriodKey();
    private MCCTransactionCount outputValue = new MCCTransactionCount();

    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long invalidRecords = 0;

    /**
     * Map - Parse do formato intermediário do Job 1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        try {
            // Parse: "CITY\tPERIOD\tMCC\tCOUNT"
            String[] parts = line.split("\t");

            if (parts.length >= 4) {
                String cityName = parts[0];
                String timePeriod = parts[1];
                String mcc = parts[2];
                long count = Long.parseLong(parts[3]);

                // Criar chave composta
                outputKey.setCityName(cityName);
                outputKey.setTimePeriod(timePeriod);

                // Criar valor
                outputValue.setMccCode(mcc);
                outputValue.setCount(count);

                // Emitir
                context.write(outputKey, outputValue);
                validRecords++;
            } else {
                invalidRecords++;
            }

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Step 2 Mapper - Erro: " + e.getMessage());
        }

        // Log de progresso
        if (recordsProcessed % 10000 == 0) {
            context.setStatus(String.format("Step 2 Mapper: Processados %d registros (Válidos: %d)",
                    recordsProcessed, validRecords));
        }
    }

    /**
     * Cleanup - estatísticas finais do Mapper do Job 2
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Step 2 Mapper - Estatísticas:");
        System.out.println("  Registros processados: " + recordsProcessed);
        System.out.println("  Registros válidos: " + validRecords);
        System.out.println("  Registros inválidos: " + invalidRecords);

        if (recordsProcessed > 0) {
            double successRate = (validRecords * 100.0) / recordsProcessed;
            System.out.println("  Taxa de sucesso: " + String.format("%.2f%%", successRate));
        }

        System.out.println();
        System.out.println("  Dados preparados para ranking no Reducer");
        System.out.println("========================================");
        super.cleanup(context);
    }
}