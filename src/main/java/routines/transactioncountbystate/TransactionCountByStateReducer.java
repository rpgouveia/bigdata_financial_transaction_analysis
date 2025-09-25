package routines.transactioncountbystate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class para TransactionCountByState
 * Agrega as contagens de transações por estado e emite resultado com análise geográfica
 */
public class TransactionCountByStateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Objeto reutilizável para o resultado
    private IntWritable result = new IntWritable();

    // Contadores para estatísticas
    private long totalStates = 0;
    private long totalTransactions = 0;
    private String mostActiveState = "";
    private int highestCount = 0;
    private String leastActiveState = "";
    private int lowestCount = Integer.MAX_VALUE;
    private long onlineTransactions = 0;
    private long unknownStateTransactions = 0;

    // Mapa para análise por região
    private Map<String, Long> regionCounts = new HashMap<>();

    /**
     * Método reduce - agrega contagens para cada estado
     * @param key Estado (código de 2 letras, ONLINE, ou UNKNOWN)
     * @param values Lista de contagens (sempre 1) para este estado
     * @param context Contexto para emitir o resultado
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String state = key.toString();
        int stateCount = 0;

        // Somar todas as contagens para este estado
        for (IntWritable value : values) {
            stateCount += value.get();
        }

        // Emitir o resultado (estado, contagem_total)
        result.set(stateCount);
        context.write(key, result);

        // Atualizar estatísticas globais
        totalStates++;
        totalTransactions += stateCount;

        // Rastrear transações especiais
        if ("ONLINE".equals(state)) {
            onlineTransactions = stateCount;
        } else if ("UNKNOWN".equals(state)) {
            unknownStateTransactions = stateCount;
        }

        // Rastrear estado mais ativo (excluindo ONLINE)
        if (!state.equals("ONLINE") && !state.equals("UNKNOWN") && stateCount > highestCount) {
            highestCount = stateCount;
            mostActiveState = state;
        }

        // Rastrear estado menos ativo (excluindo especiais e apenas estados válidos)
        if (!state.equals("ONLINE") && !state.equals("UNKNOWN") &&
                isValidUSState(state) && stateCount < lowestCount) {
            lowestCount = stateCount;
            leastActiveState = state;
        }

        // Análise por região geográfica
        if (isValidUSState(state)) {
            String region = getUSRegion(state);
            regionCounts.put(region, regionCounts.getOrDefault(region, 0L) + stateCount);
        }

        // Log para estados com muitas transações
        if (stateCount > 10000 && !state.equals("ONLINE")) {
            context.setStatus("Estado '" + state + "' (" + getStateName(state) +
                    ") tem " + stateCount + " transações");
        }

        // Log de progresso
        if (totalStates % 10 == 0) {
            context.setStatus("Processados " + totalStates + " estados/regiões");
        }
    }

    /**
     * Método cleanup - chamado no final do processamento
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Reducer:");
        System.out.println("  Total de estados/regiões: " + totalStates);
        System.out.println("  Total de transações: " + totalTransactions);

        if (totalStates > 0 && totalTransactions > 0) {
            // Estatísticas básicas
            double averagePerState = (double) (totalTransactions - onlineTransactions - unknownStateTransactions)
                    / (totalStates - (onlineTransactions > 0 ? 1 : 0) - (unknownStateTransactions > 0 ? 1 : 0));
            System.out.println("  Média de transações por estado físico: " + String.format("%.2f", averagePerState));

            // Análise de transações online
            if (onlineTransactions > 0) {
                double onlinePercentage = (double) onlineTransactions / totalTransactions * 100;
                System.out.println("  Transações online: " + onlineTransactions +
                        " (" + String.format("%.2f%%", onlinePercentage) + " do total)");
            }

            // Análise de estados desconhecidos
            if (unknownStateTransactions > 0) {
                double unknownPercentage = (double) unknownStateTransactions / totalTransactions * 100;
                System.out.println("  Estados desconhecidos: " + unknownStateTransactions +
                        " (" + String.format("%.2f%%", unknownPercentage) + " do total)");
            }

            // Estado mais ativo
            if (!mostActiveState.isEmpty()) {
                double mostActivePercentage = (double) highestCount / totalTransactions * 100;
                System.out.println("  Estado mais ativo:");
                System.out.println("    " + mostActiveState + " (" + getStateName(mostActiveState) +
                        "): " + highestCount + " transações (" +
                        String.format("%.2f%%", mostActivePercentage) + " do total)");
            }

            // Estado menos ativo
            if (!leastActiveState.isEmpty() && lowestCount != Integer.MAX_VALUE) {
                double leastActivePercentage = (double) lowestCount / totalTransactions * 100;
                System.out.println("  Estado menos ativo:");
                System.out.println("    " + leastActiveState + " (" + getStateName(leastActiveState) +
                        "): " + lowestCount + " transações (" +
                        String.format("%.2f%%", leastActivePercentage) + " do total)");
            }

            // Análise por região
            analyzeByRegion();
        }

        System.out.println("========================================");
        System.out.println("NOTA: Códigos de 2 letras representam estados americanos.");
        System.out.println("      ONLINE = transações de e-commerce sem localização física.");
        System.out.println("      UNKNOWN = estados não identificados ou inválidos.");
        System.out.println("========================================");

        super.cleanup(context);
    }

    /**
     * Análise de distribuição por região geográfica dos EUA
     */
    private void analyzeByRegion() {
        if (regionCounts.isEmpty()) {
            return;
        }

        System.out.println("  Análise por região geográfica:");

        // Encontrar região com mais transações
        String topRegion = "";
        long topRegionCount = 0;
        for (Map.Entry<String, Long> entry : regionCounts.entrySet()) {
            if (entry.getValue() > topRegionCount) {
                topRegionCount = entry.getValue();
                topRegion = entry.getKey();
            }

            double regionPercentage = (double) entry.getValue() / totalTransactions * 100;
            System.out.println("    " + entry.getKey() + ": " + entry.getValue() +
                    " (" + String.format("%.2f%%", regionPercentage) + ")");
        }

        System.out.println("  Região dominante: " + topRegion + " com " + topRegionCount + " transações");

        // Análise de distribuição
        if (regionCounts.size() >= 4) { // Se temos representação das 4 regiões principais
            System.out.println("  Distribuição geográfica: Bem distribuída entre regiões");
        } else {
            System.out.println("  Distribuição geográfica: Concentrada em poucas regiões");
        }
    }

    /**
     * Mapeia estados para regiões geográficas dos EUA
     */
    private String getUSRegion(String state) {
        if (state == null || state.length() != 2) {
            return "Unknown";
        }

        switch (state) {
            // Northeast
            case "CT": case "ME": case "MA": case "NH": case "NJ":
            case "NY": case "PA": case "RI": case "VT":
                return "Northeast";

            // Southeast
            case "AL": case "AR": case "FL": case "GA": case "KY":
            case "LA": case "MS": case "NC": case "SC": case "TN":
            case "VA": case "WV": case "MD": case "DE": case "DC":
                return "Southeast";

            // Midwest
            case "IL": case "IN": case "IA": case "KS": case "MI":
            case "MN": case "MO": case "NE": case "ND": case "OH":
            case "SD": case "WI":
                return "Midwest";

            // Southwest
            case "AZ": case "NM": case "OK": case "TX":
                return "Southwest";

            // West
            case "AK": case "CA": case "CO": case "HI": case "ID":
            case "MT": case "NV": case "OR": case "UT": case "WA": case "WY":
                return "West";

            default:
                return "Unknown";
        }
    }

    /**
     * Verifica se é um código de estado americano válido
     */
    private boolean isValidUSState(String state) {
        if (state == null || state.length() != 2) {
            return false;
        }
        return !state.equals("ONLINE") && !state.equals("UNKNOWN") &&
                getUSRegion(state) != null && !getUSRegion(state).equals("Unknown");
    }

    /**
     * Obtém o nome completo do estado baseado no código
     */
    private String getStateName(String code) {
        if (code == null || code.length() != 2) {
            return "Unknown State";
        }

        Map<String, String> stateNames = new HashMap<>();
        stateNames.put("AL", "Alabama"); stateNames.put("AK", "Alaska");
        stateNames.put("AZ", "Arizona"); stateNames.put("AR", "Arkansas");
        stateNames.put("CA", "California"); stateNames.put("CO", "Colorado");
        stateNames.put("CT", "Connecticut"); stateNames.put("DE", "Delaware");
        stateNames.put("FL", "Florida"); stateNames.put("GA", "Georgia");
        stateNames.put("HI", "Hawaii"); stateNames.put("ID", "Idaho");
        stateNames.put("IL", "Illinois"); stateNames.put("IN", "Indiana");
        stateNames.put("IA", "Iowa"); stateNames.put("KS", "Kansas");
        stateNames.put("KY", "Kentucky"); stateNames.put("LA", "Louisiana");
        stateNames.put("ME", "Maine"); stateNames.put("MD", "Maryland");
        stateNames.put("MA", "Massachusetts"); stateNames.put("MI", "Michigan");
        stateNames.put("MN", "Minnesota"); stateNames.put("MS", "Mississippi");
        stateNames.put("MO", "Missouri"); stateNames.put("MT", "Montana");
        stateNames.put("NE", "Nebraska"); stateNames.put("NV", "Nevada");
        stateNames.put("NH", "New Hampshire"); stateNames.put("NJ", "New Jersey");
        stateNames.put("NM", "New Mexico"); stateNames.put("NY", "New York");
        stateNames.put("NC", "North Carolina"); stateNames.put("ND", "North Dakota");
        stateNames.put("OH", "Ohio"); stateNames.put("OK", "Oklahoma");
        stateNames.put("OR", "Oregon"); stateNames.put("PA", "Pennsylvania");
        stateNames.put("RI", "Rhode Island"); stateNames.put("SC", "South Carolina");
        stateNames.put("SD", "South Dakota"); stateNames.put("TN", "Tennessee");
        stateNames.put("TX", "Texas"); stateNames.put("UT", "Utah");
        stateNames.put("VT", "Vermont"); stateNames.put("VA", "Virginia");
        stateNames.put("WA", "Washington"); stateNames.put("WV", "West Virginia");
        stateNames.put("WI", "Wisconsin"); stateNames.put("WY", "Wyoming");
        stateNames.put("DC", "District of Columbia");

        return stateNames.getOrDefault(code, "Unknown State: " + code);
    }
}