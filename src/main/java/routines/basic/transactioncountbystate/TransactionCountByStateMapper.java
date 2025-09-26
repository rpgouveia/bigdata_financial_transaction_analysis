package routines.basic.transactioncountbystate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class para TransactionCountByState
 * Processa cada linha de transação CSV e emite pares (estado, 1)
 */
public class TransactionCountByStateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objetos reutilizáveis para otimização
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    // Contadores para estatísticas
    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;
    private long onlineTransactions = 0;
    private long unknownStates = 0;

    // Set para rastrear estados únicos vistos durante o processamento
    private Set<String> statesEncountered = new HashSet<>();

    /**
     * Método map - processa cada linha do arquivo CSV
     * @param key Offset da linha no arquivo
     * @param value Conteúdo da linha CSV
     * @param context Contexto para emitir resultados
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        recordsProcessed++;
        String line = value.toString();

        // Ignorar cabeçalho do CSV
        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            // Usar regex sofisticado para split CSV respeitando aspas
            String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Verificar se tem o número mínimo de campos esperados
            if (cols.length < 12) {
                invalidRecords++;
                context.setStatus("Linha inválida (poucos campos): " + cols.length + " campos");
                return;
            }

            // Extrair dados da transação
            // Estrutura CSV: id(0),date(1),client_id(2),card_id(3),amount(4),use_chip(5),
            //                merchant_id(6),merchant_city(7),merchant_state(8),zip(9),mcc(10),errors(11)

            String merchantStateRaw = cols[8];
            String merchantCityRaw = cols[7];

            // Processar estado do comerciante
            String state = processMerchantState(merchantStateRaw, merchantCityRaw);

            // Rastrear tipos especiais de transação
            if ("ONLINE".equals(state)) {
                onlineTransactions++;
            } else if ("UNKNOWN".equals(state)) {
                unknownStates++;
            }

            // Adicionar ao set de estados encontrados
            statesEncountered.add(state);

            // Emitir resultado
            outputKey.set(state);
            context.write(outputKey, one);
            validRecords++;

        } catch (Exception e) {
            invalidRecords++;
            context.setStatus("Erro processando linha: " + e.getMessage());
        }

        // Log de progresso a cada 50000 registros
        if (recordsProcessed % 50000 == 0) {
            context.setStatus("Processados " + recordsProcessed + " registros. " +
                    "Válidos: " + validRecords + ", Estados únicos: " + statesEncountered.size());
        }
    }

    /**
     * Processa e valida o estado do comerciante
     */
    private String processMerchantState(String stateRaw, String cityRaw) {
        // Primeiro, processar o campo de estado
        String state = null;
        if (stateRaw != null && !stateRaw.trim().isEmpty()) {
            state = stateRaw.trim()
                    .replace("\"", "")  // Remove aspas
                    .toUpperCase();     // Padroniza em maiúsculas
        }

        // Se estado está vazio ou inválido, verificar se é transação online
        if (state == null || state.isEmpty() || state.equals("NULL") || state.equals("N/A")) {
            // Verificar se é transação online baseada na cidade
            if (cityRaw != null && "ONLINE".equalsIgnoreCase(cityRaw.trim().replace("\"", ""))) {
                return "ONLINE";
            } else {
                return "UNKNOWN";
            }
        }

        // Validar códigos de estado americanos (2 letras)
        if (isValidUSState(state)) {
            return state;
        } else {
            // Se não é um código de estado válido, verificar outros casos
            if (state.equals("ONLINE")) {
                return "ONLINE";
            } else {
                return "UNKNOWN";
            }
        }
    }

    /**
     * Verifica se é um código de estado americano válido
     */
    private boolean isValidUSState(String state) {
        if (state == null || state.length() != 2) {
            return false;
        }

        // Lista completa de códigos de estado americanos
        Set<String> validStates = new HashSet<>();
        validStates.add("AL"); validStates.add("AK"); validStates.add("AZ"); validStates.add("AR");
        validStates.add("CA"); validStates.add("CO"); validStates.add("CT"); validStates.add("DE");
        validStates.add("FL"); validStates.add("GA"); validStates.add("HI"); validStates.add("ID");
        validStates.add("IL"); validStates.add("IN"); validStates.add("IA"); validStates.add("KS");
        validStates.add("KY"); validStates.add("LA"); validStates.add("ME"); validStates.add("MD");
        validStates.add("MA"); validStates.add("MI"); validStates.add("MN"); validStates.add("MS");
        validStates.add("MO"); validStates.add("MT"); validStates.add("NE"); validStates.add("NV");
        validStates.add("NH"); validStates.add("NJ"); validStates.add("NM"); validStates.add("NY");
        validStates.add("NC"); validStates.add("ND"); validStates.add("OH"); validStates.add("OK");
        validStates.add("OR"); validStates.add("PA"); validStates.add("RI"); validStates.add("SC");
        validStates.add("SD"); validStates.add("TN"); validStates.add("TX"); validStates.add("UT");
        validStates.add("VT"); validStates.add("VA"); validStates.add("WA"); validStates.add("WV");
        validStates.add("WI"); validStates.add("WY"); validStates.add("DC"); // District of Columbia

        return validStates.contains(state);
    }

    /**
     * Método cleanup - chamado no final do processamento
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("========================================");
        System.out.println("Estatísticas do Mapper:");
        System.out.println("  Total de registros processados: " + recordsProcessed);
        System.out.println("  Cabeçalhos ignorados: " + headerSkipped);
        System.out.println("  Registros válidos: " + validRecords);
        System.out.println("  Registros inválidos: " + invalidRecords);
        System.out.println("  Estados únicos encontrados: " + statesEncountered.size());
        System.out.println("  Transações online: " + onlineTransactions);
        System.out.println("  Estados desconhecidos: " + unknownStates);

        if (recordsProcessed > 0) {
            double successRate = (double) validRecords / recordsProcessed * 100;
            double onlineRate = (double) onlineTransactions / validRecords * 100;
            double unknownRate = (double) unknownStates / validRecords * 100;
            System.out.println("  Taxa de sucesso: " + String.format("%.2f%%", successRate));
            System.out.println("  Taxa de transações online: " + String.format("%.2f%%", onlineRate));
            System.out.println("  Taxa de estados desconhecidos: " + String.format("%.2f%%", unknownRate));
        }

        // Mostrar amostra dos estados encontrados
        System.out.println("  Amostra de estados encontrados: " +
                statesEncountered.toString().replaceAll("[\\[\\]]", ""));

        System.out.println("========================================");
        super.cleanup(context);
    }
}