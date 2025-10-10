package routines.advanced.categorybytimeperiod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;

/**
 * Step 1 Mapper - Análise de categorias por período do dia e cidade
 *
 * Primeira etapa do pipeline multi-step.
 * Processa CSV de transações e classifica cada transação em:
 * - Cidade
 * - Período do dia (MORNING/AFTERNOON/NIGHT)
 * - Categoria MCC
 *
 * Input:  CSV de transações
 * Output: (CityPeriodKey, MCCTransactionCount)
 */
public class Step1AggregationMapper extends Mapper<LongWritable, Text, CityPeriodKey, MCCTransactionCount> {

    // Objetos reutilizáveis
    private CityPeriodKey outputKey = new CityPeriodKey();
    private MCCTransactionCount outputValue = new MCCTransactionCount();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    // Contadores por período
    private long morningCount = 0;
    private long afternoonCount = 0;
    private long nightCount = 0;

    /**
     * Enum para períodos do dia
     */
    private enum TimePeriod {
        MORNING,    // 00:00 - 11:59
        AFTERNOON,  // 12:00 - 17:59
        NIGHT       // 18:00 - 23:59
    }

    /**
     * Método map - processa cada linha do CSV
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        // Ignorar cabeçalho
        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            // Parse da linha CSV
            String[] parts = splitCsv(line);

            // Verificar campos mínimos
            if (parts.length < 12) {
                invalidRecords++;
                return;
            }

            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String dateTimeRaw = parts[1];
            String cityRaw = parts[7];
            String mccRaw = parts[10];

            // Processar campos
            String city = processCityName(cityRaw);
            String mcc = processMCC(mccRaw);
            TimePeriod period = determineTimePeriod(dateTimeRaw);

            if (!city.isEmpty() && !mcc.equals("UNKNOWN_MCC") && period != null) {
                // Criar chave composta (cidade + período)
                String periodStr = period.name();
                outputKey.setCityName(city);
                outputKey.setTimePeriod(periodStr);

                // Criar valor (MCC com contagem 1)
                MCCTransactionCount mccCount = new MCCTransactionCount(mcc, 1);

                // Emitir
                context.write(outputKey, mccCount);
                validRecords++;

                // Atualizar estatísticas
                switch (period) {
                    case MORNING:
                        morningCount++;
                        break;
                    case AFTERNOON:
                        afternoonCount++;
                        break;
                    case NIGHT:
                        nightCount++;
                        break;
                }
            } else {
                invalidRecords++;
            }

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Erro processando linha: " + e.getMessage());
        }

        // Log de progresso
        if (recordsProcessed % 50000 == 0) {
            context.setStatus(String.format(
                    "Step 1 Mapper: Processados %d registros. Válidos: %d (M:%d T:%d N:%d)",
                    recordsProcessed, validRecords, morningCount, afternoonCount, nightCount));
        }
    }

    /**
     * Determina o período do dia baseado no timestamp
     */
    private TimePeriod determineTimePeriod(String dateTimeRaw) {
        if (dateTimeRaw == null || dateTimeRaw.trim().isEmpty()) {
            return null;
        }

        try {
            String dateTime = dateTimeRaw.trim().replace("\"", "");
            String[] parts = dateTime.split(" ");
            if (parts.length < 2) {
                return null;
            }

            String timePart = parts[1];
            String[] timeParts = timePart.split(":");
            if (timeParts.length < 1) {
                return null;
            }

            int hour = Integer.parseInt(timeParts[0]);

            if (hour >= 0 && hour < 12) {
                return TimePeriod.MORNING;
            } else if (hour >= 12 && hour < 18) {
                return TimePeriod.AFTERNOON;
            } else if (hour >= 18 && hour < 24) {
                return TimePeriod.NIGHT;
            }

            return null;

        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Processa e limpa o nome da cidade
     */
    private String processCityName(String cityRaw) {
        if (cityRaw == null || cityRaw.trim().isEmpty()) {
            return "UNKNOWN";
        }

        String city = cityRaw.trim()
                .replace("\"", "")
                .toUpperCase();

        if (city.isEmpty() || city.equals("NULL") || city.equals("N/A")) {
            return "UNKNOWN";
        }

        return city;
    }

    /**
     * Processa e valida o código MCC
     */
    private String processMCC(String mccRaw) {
        if (mccRaw == null || mccRaw.trim().isEmpty()) {
            return "UNKNOWN_MCC";
        }

        String mcc = mccRaw.trim().replace("\"", "");

        if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) {
            return "UNKNOWN_MCC";
        }

        if (mcc.matches("\\d+")) {
            return mcc;
        } else {
            return "UNKNOWN_MCC";
        }
    }

    /**
     * Split de CSV que respeita aspas
     */
    private static String[] splitCsv(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '\"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                result.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(ch);
            }
        }

        result.add(currentField.toString());
        return result.toArray(new String[0]);
    }

    /**
     * Cleanup - estatísticas finais do Step 1 Mapper
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long totalValid = morningCount + afternoonCount + nightCount;

        System.out.println("========================================");
        System.out.println("Step 1 Mapper - Estatísticas:");
        System.out.println("  Total de registros processados: " + recordsProcessed);
        System.out.println("  Cabeçalhos ignorados: " + headerSkipped);
        System.out.println("  Registros válidos: " + validRecords);
        System.out.println("  Registros inválidos: " + invalidRecords);
        System.out.println();

        if (totalValid > 0) {
            System.out.println("  Distribuição por período:");
            System.out.println("    Manhã (0h-11h): " + morningCount +
                    String.format(" (%.2f%%)", morningCount * 100.0 / totalValid));
            System.out.println("    Tarde (12h-17h): " + afternoonCount +
                    String.format(" (%.2f%%)", afternoonCount * 100.0 / totalValid));
            System.out.println("    Noite (18h-23h): " + nightCount +
                    String.format(" (%.2f%%)", nightCount * 100.0 / totalValid));
        }

        if (recordsProcessed > 0) {
            double successRate = (double) validRecords / recordsProcessed * 100;
            System.out.println();
            System.out.println("  Taxa de sucesso: " + String.format("%.2f%%", successRate));
        }

        System.out.println("========================================");
        super.cleanup(context);
    }
}