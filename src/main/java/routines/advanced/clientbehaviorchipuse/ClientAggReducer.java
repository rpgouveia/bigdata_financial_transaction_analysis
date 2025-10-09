package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 1 - Reducer (versão canal online/swipe).
 *
 * Agregar TODOS os eventos (linhas do CSV) de um mesmo client_id e derivar um perfil do cliente:
 *   • onlineRate  = fração de transações do cliente em canal "Online Transaction"
 *   • errorRate   = fração de transações do cliente com campo errors NÃO vazio
 *   • avgCents    = ticket médio (centavos)
 *   • maxCents    = pico (maior valor visto para o cliente)
 * Em seguida, classificar o cliente (LOW/MED/HIGH) por regras parametrizáveis e emitir UMA linha
 * por cliente, atribuída à UF predominante do cliente. Esta linha segue o formato textual:
 *   KEY:   <STATE>
 *   VALUE: "1:low:med:high|CITY=1"
 * (CITY=1 só aparece se o bucket for HIGH e houver cidade predominante).
 *
 *
 * Cálculo:
 *      errorRate = (# transações com 'errors' não vazio) / (total de transações do cliente)
 *
 * • Implicações estatísticas e cuidado com amostra pequena:
 *   - Clientes com POUCAS transações (ex.: tx=1 ou 2) podem inflar artificialmente o errorRate (1 erro => 100%) por puro ruído amostral. Isso pode promover falsos positivos no bucket HIGH.
 *
 * • Interação com canal (onlineRate):
 *   - Por padrão, consideramos mais arriscoso quando errorRate é alto EM CONJUNTO com onlineRate
 *     moderado/alto, pois operações card-not-present tendem a ter erro/fraude mais frequentes.
 *   - Isso está refletido na regra:
 *         (errorRate >= errHigh && onlineRate >= onMed)  => candidato a HIGH
 *
 * • Dados sujos (qualidade do 'errors'):
 *   - Se o campo 'errors' for preenchido de modo inconsistente (ex.: mensagens de warning tratadas
 *     como erro, ou "NULL" textual), considere normalizar já no Mapper (hasError=false para "NULL"/"N/A")
 *     ou aqui antes da contagem (não alterado neste reducer).
 *
 */
public class ClientAggReducer extends Reducer<Text, TransactionMiniWritable, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text clientId, Iterable<TransactionMiniWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long tx = 0L;           // total de transações do cliente
        long onlineCount = 0L;  // transações em canal "Online Transaction"
        long errors = 0L;       // transações cujo campo 'errors' NÃO está vazio
        long sumCents = 0L;     // soma de valores para ticket médio
        long maxCents = 0L;     // maior valor observado (pico)

        // Frequências para determinar UF/cidade predominantes do cliente
        Map<String, Long> stateCount = new HashMap<>();
        Map<String, Long> cityCount  = new HashMap<>();
        Map<String, Long> mccCount   = new HashMap<>();

        for (TransactionMiniWritable v : values) {
            tx++;
            if (v.isOnline())  onlineCount++;
            if (v.isHasError()) errors++;
            long a = v.getAmountCents();
            sumCents += a;
            if (a > maxCents) maxCents = a;

            // normalização defensiva para agrupar UNKNOWN quando faltar dado
            stateCount.merge(nz(v.getState()), 1L, Long::sum);
            cityCount.merge(nz(v.getCity()),   1L, Long::sum);
            mccCount.merge(nz(v.getMcc()),     1L, Long::sum);
        }

        // Nenhuma transação (defensivo): nada a emitir
        if (tx == 0) return;

        String topState = topKey(stateCount);  // UF do cliente
        String topCity  = topKey(cityCount);   // cidade do cliente

        // MÉTRICAS DO CLIENTE
        double onlineRate = onlineCount * 1.0 / tx;  // fração online
        double errorRate  = errors      * 1.0 / tx;  // fração com erro
        long   avgCents   = sumCents / tx;           // ticket médio

        Configuration conf = ctx.getConfiguration();
        float errHigh = conf.getFloat("risk.error.high", 0.05f);          // ≥ 5% de erros => alto (se online moderado+)
        float errMed  = conf.getFloat("risk.error.med",  0.02f);          // ≥ 2% de erros => médio
        float onHigh  = conf.getFloat("risk.online.high", 0.80f);         // ≥ 80% online   => alto
        float onMed   = conf.getFloat("risk.online.med",  0.60f);         // ≥ 60% online   => médio
        long  avgHigh = conf.getLong ("risk.avg_amount.high_cents", 10000L); // ticket médio alto
        long  maxHigh = conf.getLong ("risk.max_amount.high_cents", 50000L); // pico alto

        // CLASSIFICAÇÃO
        // a entrada de erro (ex.: só contar erroRate se tx >= 3), via regra adicional (não implementado).
        String bucket = classifyBucket(
                onlineRate, errorRate, avgCents, maxCents,
                errHigh, errMed, onHigh, onMed, avgHigh, maxHigh
        );

        // EMISSÃO (uma linha por cliente) atribuída à UF predominante
        if (topState != null && !topState.isEmpty()) {
            outKey.set(topState);
            outVal.set(serializeSingleClient(bucket, topCity));
            ctx.write(outKey, outVal);
        }
    }

    /**
     * Serializa o cliente como string compacta para o Job 2.
     * Formato: "1:low:med:high|CITY=1"
     * - '1' à esquerda é o total=1 (um cliente).
     * - Apenas UM dos buckets (low/med/high) vale 1; os demais valem 0.
     * - Se bucket=HIGH e houver cidade predominante, emitimos "CITY=1" após o pipe,
     *   permitindo o Job 2 somar hotspots de ALTO RISCO por cidade.
     */
    private static String serializeSingleClient(String bucket, String city) {
        long low = 0, med = 0, high = 0;
        if ("LOW".equals(bucket))      low  = 1;
        else if ("MED".equals(bucket)) med  = 1;
        else                           high = 1;

        StringBuilder sb = new StringBuilder();
        sb.append(1).append(":").append(low).append(":").append(med).append(":").append(high).append("|");
        if ("HIGH".equals(bucket) && city != null && !city.isEmpty()) {
            sb.append(city).append("=1");
        }
        return sb.toString();
    }

    /**
     * Regras de classificação de risco (parametrizáveis).
     *
     * HIGH quando:
     *   • errorRate >= errHigh E onlineRate >= onMed  (erros relevantes em ambiente majoritariamente online), OU
     *   • onlineRate >= onHigh                        (exposição quase toda online), OU
     *   • avgCents >= avgHigh                         (ticket médio elevado), OU
     *   • maxCents >= maxHigh                         (pico elevado)
     *
     * MED quando (se não for HIGH):
     *   • errorRate >= errMed  OU
     *   • onlineRate >= onMed
     *
     * Caso contrário: LOW
     */
    private static String classifyBucket(double onlineRate, double errorRate, long avgCents, long maxCents,
                                         float errHigh, float errMed, float onHigh, float onMed,
                                         long avgHigh, long maxHigh) {
        boolean highRisk =
                (errorRate >= errHigh && onlineRate >= onMed) ||
                        (onlineRate >= onHigh) ||
                        (avgCents >= avgHigh) ||
                        (maxCents >= maxHigh);

        if (highRisk) return "HIGH";

        boolean medium =
                (errorRate >= errMed) ||
                        (onlineRate >= onMed);

        return medium ? "MED" : "LOW";
    }

    // Retorna a chave com maior frequência em um mapa (empates resolvidos pelo primeiro encontrado)
    private static String topKey(Map<String, Long> map) {
        String best = "";
        long bestV = -1L;
        for (Map.Entry<String, Long> e : map.entrySet()) {
            if (e.getValue() > bestV) {
                bestV = e.getValue();
                best = e.getKey();
            }
        }
        return best;
    }

    // Normaliza strings nulas/vazias para "UNKNOWN" e aplica trim+upper-case
    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }
}
