package routines.intermediate.citytimeperiod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper para análise temporal de transações por cidade
 * Classifica cada transação em período do dia (manhã, tarde, noite)
 */
public class CityTimePeriodMapper extends Mapper<LongWritable, Text, Text, CityTimePeriodStatsWritable> {

    // Objetos reutilizáveis
    private Text outputKey = new Text();
    private CityTimePeriodStatsWritable outputValue = new CityTimePeriodStatsWritable();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;
    private long morningTransactions = 0;
    private long afternoonTransactions = 0;
    private long nightTransactions = 0;

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

            // Processar cidade
            String city = processCityName(cityRaw);

            // Determinar período do dia
            TimePeriod period = determineTimePeriod(dateTimeRaw);

            if (!city.isEmpty() && period != null) {
                // Criar CityTimePeriodStatsWritable com esta transação
                CityTimePeriodStatsWritable stats = new CityTimePeriodStatsWritable();

                switch (period) {
                    case MORNING:
                        stats.incrementMorning();
                        morningTransactions++;
                        break;
                    case AFTERNOON:
                        stats.incrementAfternoon();
                        afternoonTransactions++;
                        break;
                    case NIGHT:
                        stats.incrementNight();
                        nightTransactions++;
                        break;
                }

                // Emitir resultado
                outputKey.set(city);
                context.write(outputKey, stats);
                validRecords++;
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
                    "Processados %d registros. Manhã: %d, Tarde: %d, Noite: %d",
                    recordsProcessed, morningTransactions, afternoonTransactions, nightTransactions));
        }
    }

    /**
     * Determina o período do dia baseado no timestamp
     * Formato esperado: "2010-01-01 00:01:00"
     */
    private TimePeriod determineTimePeriod(String dateTimeRaw) {
        if (dateTimeRaw == null || dateTimeRaw.trim().isEmpty()) {
            return null;
        }

        try {
            String dateTime = dateTimeRaw.trim().replace("\"", "");

            // Extrair a parte da hora: "2010-01-01 00:01:00" -> "00:01:00"
            String[] parts = dateTime.split(" ");
            if (parts.length < 2) {
                return null;
            }

            String timePart = parts[1]; // "00:01:00"
            String[] timeParts = timePart.split(":");
            if (timeParts.length < 1) {
                return null;
            }

            int hour = Integer.parseInt(timeParts[0]);

            // Classificar por período
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
     * Cleanup - estatísticas finais
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long total = morningTransactions + afternoonTransactions + nightTransactions;

        System.out.println("========================================");
        System.out.println("Estatísticas do Mapper:");
        System.out.println("  Total de registros processados: " + recordsProcessed);
        System.out.println("  Cabeçalhos ignorados: " + headerSkipped);
        System.out.println("  Registros válidos: " + validRecords);
        System.out.println("  Registros inválidos: " + invalidRecords);
        System.out.println();
        System.out.println("  Distribuição por período:");

        if (total > 0) {
            System.out.println("    Manhã (0h-11h): " + morningTransactions +
                    String.format(" (%.2f%%)", morningTransactions * 100.0 / total));
            System.out.println("    Tarde (12h-17h): " + afternoonTransactions +
                    String.format(" (%.2f%%)", afternoonTransactions * 100.0 / total));
            System.out.println("    Noite (18h-23h): " + nightTransactions +
                    String.format(" (%.2f%%)", nightTransactions * 100.0 / total));
        }

        System.out.println("========================================");
        super.cleanup(context);
    }
}