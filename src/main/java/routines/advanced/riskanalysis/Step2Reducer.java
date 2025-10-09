package routines.advanced.riskanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Step 2 Reducer - Risk Category Classifier
 * Classifica clientes em categorias de risco baseado no perfil comportamental.
 *
 * Categorias de Risco:
 * - LOW (0-30 pontos): Comportamento normal
 * - MEDIUM (31-60 pontos): Alguns sinais de alerta
 * - HIGH (61-85 pontos): Múltiplos indicadores de risco
 * - CRITICAL (86-100+ pontos): Risco extremo
 *
 * Input: client_id -> perfil do Step 1
 * Output: risk_category -> ClientRiskWritable
 */
public class Step2Reducer extends Reducer<Text, Text, Text, ClientRiskWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Processa o perfil do cliente (deve haver apenas 1)
        for (Text value : values) {
            try {
                String[] fields = value.toString().split("\t");

                // Esperamos 14 campos (key + 13 campos do profile, sendo clientId duplicado)
                if (fields.length < 14) continue;

                // Parse do perfil
                // Nota: fields[0] é a key (clientId), fields[1] é clientId duplicado do profile
                // Índices ajustados para pular a duplicação
                String clientId = fields[0].trim();
                int transactionCount = Integer.parseInt(fields[2].trim());  // Era fields[1]
                double totalAmount = Double.parseDouble(fields[3].trim());   // Era fields[2]
                double avgAmount = Double.parseDouble(fields[4].trim());     // Era fields[3]
                int uniqueCities = Integer.parseInt(fields[5].trim());       // Era fields[4]
                int uniqueMccs = Integer.parseInt(fields[6].trim());         // Era fields[5]
                int uniqueCards = Integer.parseInt(fields[7].trim());        // Era fields[6]
                long firstTransaction = Long.parseLong(fields[8].trim());    // Era fields[7]
                long lastTransaction = Long.parseLong(fields[9].trim());     // Era fields[8]
                int onlineCount = Integer.parseInt(fields[10].trim());       // Era fields[9]
                int swipeCount = Integer.parseInt(fields[11].trim());        // Era fields[10]
                int errorCount = Integer.parseInt(fields[12].trim());        // Era fields[11]
                int chargebackCount = Integer.parseInt(fields[13].trim());   // Era fields[12]

                // Calcula risk score
                double riskScore = 0.0;
                List<String> riskFactors = new ArrayList<>();

                // Fator 1: Muitas cidades diferentes (mobilidade suspeita)
                if (uniqueCities > 5) {
                    riskScore += 15;
                    riskFactors.add("HIGH_MOBILITY[" + uniqueCities + "_cities]");
                } else if (uniqueCities > 3) {
                    riskScore += 8;
                    riskFactors.add("MEDIUM_MOBILITY[" + uniqueCities + "_cities]");
                }

                // Fator 2: Muitas categorias MCC (comportamento diversificado)
                if (uniqueMccs > 10) {
                    riskScore += 12;
                    riskFactors.add("DIVERSE_MCC[" + uniqueMccs + "_categories]");
                } else if (uniqueMccs > 6) {
                    riskScore += 6;
                    riskFactors.add("VARIED_MCC[" + uniqueMccs + "_categories]");
                }

                // Fator 3: Múltiplos cartões (pode indicar fraude)
                if (uniqueCards > 3) {
                    riskScore += 20;
                    riskFactors.add("MULTIPLE_CARDS[" + uniqueCards + "_cards]");
                } else if (uniqueCards > 1) {
                    riskScore += 8;
                    riskFactors.add("DUAL_CARDS[" + uniqueCards + "_cards]");
                }

                // Fator 4: Taxa de erros
                if (transactionCount > 0) {
                    double errorRate = (errorCount * 100.0) / transactionCount;
                    if (errorRate > 20) {
                        riskScore += 25;
                        riskFactors.add(String.format("HIGH_ERROR_RATE[%.1f%%]", errorRate));
                    } else if (errorRate > 10) {
                        riskScore += 12;
                        riskFactors.add(String.format("MEDIUM_ERROR_RATE[%.1f%%]", errorRate));
                    }
                }

                // Fator 5: Chargebacks (estornos)
                if (chargebackCount > 3) {
                    riskScore += 25;
                    riskFactors.add("FREQUENT_CHARGEBACKS[" + chargebackCount + "]");
                } else if (chargebackCount > 0) {
                    riskScore += 10;
                    riskFactors.add("CHARGEBACKS[" + chargebackCount + "]");
                }

                // Fator 6: Valor médio alto
                if (avgAmount > 500) {
                    riskScore += 15;
                    riskFactors.add(String.format("HIGH_AVG_AMOUNT[%.2f]", avgAmount));
                } else if (avgAmount > 200) {
                    riskScore += 7;
                    riskFactors.add(String.format("MEDIUM_AVG_AMOUNT[%.2f]", avgAmount));
                }

                // Fator 7: Proporção online vs presencial
                if (transactionCount > 0) {
                    double onlineRate = (onlineCount * 100.0) / transactionCount;
                    if (onlineRate > 80 || onlineRate < 20) {
                        riskScore += 10;
                        riskFactors.add(String.format("UNBALANCED_CHANNELS[%.0f%%_online]", onlineRate));
                    }
                }

                // Determina categoria de risco
                String riskCategory;
                if (riskScore >= 86) {
                    riskCategory = "CRITICAL";
                } else if (riskScore >= 61) {
                    riskCategory = "HIGH";
                } else if (riskScore >= 31) {
                    riskCategory = "MEDIUM";
                } else {
                    riskCategory = "LOW";
                }

                // Cria objeto de risco
                String factorsStr = String.join("; ", riskFactors);
                if (factorsStr.isEmpty()) {
                    factorsStr = "NORMAL_BEHAVIOR";
                }

                ClientRiskWritable risk = new ClientRiskWritable(
                        clientId,
                        riskCategory,
                        riskScore,
                        factorsStr,
                        transactionCount,
                        totalAmount
                );

                // Emite: risk_category -> ClientRiskWritable
                context.write(new Text(riskCategory), risk);
                context.getCounter("Step2", "CLASSIFIED_CLIENTS").increment(1);
                context.getCounter("Step2", "CATEGORY_" + riskCategory).increment(1);

            } catch (Exception e) {
                context.getCounter("Step2", "REDUCER_ERRORS").increment(1);
            }
        }
    }
}