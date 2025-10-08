package routines.intermediate.citytimeperiod;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer para agregação final de estatísticas temporais por cidade
 * Emite resultados como CityTimePeriodStatsWritable
 */
public class CityTimePeriodReducer extends Reducer<Text, CityTimePeriodStatsWritable, Text, CityTimePeriodStatsWritable> {

    // Objeto reutilizável para resultado
    private CityTimePeriodStatsWritable result = new CityTimePeriodStatsWritable();

    // Estatísticas globais
    private long totalCities = 0;
    private long totalMorningTransactions = 0;
    private long totalAfternoonTransactions = 0;
    private long totalNightTransactions = 0;

    // Cidades com padrões específicos
    private String mostMorningOrientedCity = "";
    private double highestMorningPercentage = 0;

    private String mostAfternoonOrientedCity = "";
    private double highestAfternoonPercentage = 0;

    private String mostNightOrientedCity = "";
    private double highestNightPercentage = 0;

    private String mostBalancedCity = "";
    private double smallestVariance = Double.MAX_VALUE;

    /**
     * Método reduce - agrega estatísticas finais por cidade
     */
    @Override
    protected void reduce(Text key, Iterable<CityTimePeriodStatsWritable> values, Context context)
            throws IOException, InterruptedException {

        String cityName = key.toString();
        long cityMorning = 0;
        long cityAfternoon = 0;
        long cityNight = 0;

        // Somar todos os valores para esta cidade
        for (CityTimePeriodStatsWritable stats : values) {
            cityMorning += stats.getMorningCount();
            cityAfternoon += stats.getAfternoonCount();
            cityNight += stats.getNightCount();
        }

        // Criar objeto final com estatísticas agregadas
        CityTimePeriodStatsWritable finalStats = new CityTimePeriodStatsWritable(
                cityMorning, cityAfternoon, cityNight);

        // Emitir objeto diretamente (Custom Writable)
        context.write(key, finalStats);

        // Atualizar estatísticas globais
        totalCities++;
        totalMorningTransactions += cityMorning;
        totalAfternoonTransactions += cityAfternoon;
        totalNightTransactions += cityNight;

        // Análise de padrões (apenas para cidades com volume significativo)
        long cityTotal = finalStats.getTotalCount();
        if (cityTotal >= 50) {
            double morningPct = finalStats.getMorningPercentage();
            double afternoonPct = finalStats.getAfternoonPercentage();
            double nightPct = finalStats.getNightPercentage();

            // Rastrear cidade mais orientada para manhã
            if (morningPct > highestMorningPercentage) {
                highestMorningPercentage = morningPct;
                mostMorningOrientedCity = cityName;
            }

            // Rastrear cidade mais orientada para tarde
            if (afternoonPct > highestAfternoonPercentage) {
                highestAfternoonPercentage = afternoonPct;
                mostAfternoonOrientedCity = cityName;
            }

            // Rastrear cidade mais orientada para noite
            if (nightPct > highestNightPercentage) {
                highestNightPercentage = nightPct;
                mostNightOrientedCity = cityName;
            }

            // Rastrear cidade mais balanceada (menor variância)
            double mean = cityTotal / 3.0;
            double variance = Math.pow(cityMorning - mean, 2) +
                    Math.pow(cityAfternoon - mean, 2) +
                    Math.pow(cityNight - mean, 2);

            if (variance < smallestVariance) {
                smallestVariance = variance;
                mostBalancedCity = cityName;
            }
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
        long totalTransactions = totalMorningTransactions + totalAfternoonTransactions + totalNightTransactions;

        System.out.println("========================================");
        System.out.println("Estatísticas Globais do Reducer:");
        System.out.println("  Total de cidades: " + totalCities);
        System.out.println("  Total de transações: " + totalTransactions);
        System.out.println();

        if (totalTransactions > 0) {
            double globalMorningPct = (totalMorningTransactions * 100.0) / totalTransactions;
            double globalAfternoonPct = (totalAfternoonTransactions * 100.0) / totalTransactions;
            double globalNightPct = (totalNightTransactions * 100.0) / totalTransactions;

            System.out.println("  Distribuição Global por Período:");
            System.out.println("    Manhã (0h-11h): " + totalMorningTransactions +
                    String.format(" (%.2f%%)", globalMorningPct));
            System.out.println("    Tarde (12h-17h): " + totalAfternoonTransactions +
                    String.format(" (%.2f%%)", globalAfternoonPct));
            System.out.println("    Noite (18h-23h): " + totalNightTransactions +
                    String.format(" (%.2f%%)", globalNightPct));
            System.out.println();

            // Identificar período de pico global
            String globalPeakPeriod;
            if (totalMorningTransactions >= totalAfternoonTransactions &&
                    totalMorningTransactions >= totalNightTransactions) {
                globalPeakPeriod = "Manhã";
            } else if (totalAfternoonTransactions >= totalNightTransactions) {
                globalPeakPeriod = "Tarde";
            } else {
                globalPeakPeriod = "Noite";
            }
            System.out.println("  Período de Pico Global: " + globalPeakPeriod);
            System.out.println();

            if (totalCities > 0) {
                long avgMorningPerCity = totalMorningTransactions / totalCities;
                long avgAfternoonPerCity = totalAfternoonTransactions / totalCities;
                long avgNightPerCity = totalNightTransactions / totalCities;

                System.out.println("  Médias por Cidade:");
                System.out.println("    Transações na Manhã: " + avgMorningPerCity);
                System.out.println("    Transações na Tarde: " + avgAfternoonPerCity);
                System.out.println("    Transações na Noite: " + avgNightPerCity);
                System.out.println();
            }

            System.out.println("  Padrões de Cidades (min. 50 transações):");

            if (!mostMorningOrientedCity.isEmpty()) {
                System.out.println("    Mais orientada para Manhã:");
                System.out.println("      " + mostMorningOrientedCity +
                        String.format(" (%.2f%% pela manhã)", highestMorningPercentage));
            }

            if (!mostAfternoonOrientedCity.isEmpty()) {
                System.out.println("    Mais orientada para Tarde:");
                System.out.println("      " + mostAfternoonOrientedCity +
                        String.format(" (%.2f%% pela tarde)", highestAfternoonPercentage));
            }

            if (!mostNightOrientedCity.isEmpty()) {
                System.out.println("    Mais orientada para Noite:");
                System.out.println("      " + mostNightOrientedCity +
                        String.format(" (%.2f%% pela noite)", highestNightPercentage));
            }

            if (!mostBalancedCity.isEmpty()) {
                System.out.println("    Distribuição mais balanceada:");
                System.out.println("      " + mostBalancedCity + " (menor variância entre períodos)");
            }
        }

        System.out.println("========================================");
        System.out.println("NOTA: Resultados mostram distribuição de transações por");
        System.out.println("      período do dia para cada cidade.");
        System.out.println("      Períodos: Manhã (0h-11h), Tarde (12h-17h), Noite (18h-23h)");
        System.out.println("========================================");

        super.cleanup(context);
    }
}