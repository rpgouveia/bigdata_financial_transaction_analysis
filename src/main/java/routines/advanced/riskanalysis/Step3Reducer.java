package routines.advanced.riskanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Step 3 Reducer - Final Risk Report Generator
 * Gera relatório consolidado por categoria de risco com rankings e estatísticas.
 *
 * Input: risk_category -> lista de ClientRiskWritable
 * Output: risk_category -> relatório consolidado
 */
public class Step3Reducer extends Reducer<Text, Text, Text, Text> {

    // Classe auxiliar para ordenação
    private static class ClientRisk {
        String clientId;
        double riskScore;
        String riskFactors;
        int transactionCount;
        double totalAmount;

        ClientRisk(String clientId, double riskScore, String riskFactors,
                   int transactionCount, double totalAmount) {
            this.clientId = clientId;
            this.riskScore = riskScore;
            this.riskFactors = riskFactors;
            this.transactionCount = transactionCount;
            this.totalAmount = totalAmount;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String riskCategory = key.toString();
        List<ClientRisk> clients = new ArrayList<>();

        int totalClients = 0;
        double totalAmount = 0.0;
        int totalTransactions = 0;
        double sumRiskScore = 0.0;

        // Coleta todos os clientes desta categoria
        for (Text value : values) {
            try {
                // Remove caracteres de controle e faz split
                String line = value.toString().replaceAll("\\r\\n|\\r|\\n", "").trim();
                String[] fields = line.split("\t");

                // Limpa cada campo individualmente
                for (int i = 0; i < fields.length; i++) {
                    fields[i] = fields[i].trim();
                }

                // Esperamos 7 campos (key + 6 campos do risk, com duplicações)
                if (fields.length < 7) {
                    context.getCounter("Step3", "INVALID_FIELD_COUNT_" + fields.length).increment(1);
                    continue;
                }

                // Parse dos dados do cliente
                // fields[0] = riskCategory (key)
                // fields[1] = clientId
                // fields[2] = riskCategory (duplicado do toString())
                // fields[3] = riskScore
                // fields[4] = riskFactors
                // fields[5] = transactionCount
                // fields[6] = totalAmount
                String clientId = fields[1];
                double riskScore = Double.parseDouble(fields[3]);
                String riskFactors = fields[4];
                int transactionCount = Integer.parseInt(fields[5]);
                double amount = Double.parseDouble(fields[6]);

                clients.add(new ClientRisk(clientId, riskScore, riskFactors,
                        transactionCount, amount));

                totalClients++;
                totalAmount += amount;
                totalTransactions += transactionCount;
                sumRiskScore += riskScore;

            } catch (NumberFormatException e) {
                context.getCounter("Step3", "NUMBER_FORMAT_ERRORS").increment(1);
                System.err.println("Step3Reducer NumberFormat Error: " + e.getMessage());
            } catch (Exception e) {
                context.getCounter("Step3", "REDUCER_ERRORS").increment(1);
                System.err.println("Step3Reducer Error: " + e.getMessage());
            }
        }

        if (totalClients == 0) return;

        // Ordena clientes por risk score (decrescente)
        Collections.sort(clients, new Comparator<ClientRisk>() {
            @Override
            public int compare(ClientRisk c1, ClientRisk c2) {
                return Double.compare(c2.riskScore, c1.riskScore);
            }
        });

        // Calcula estatísticas
        double avgRiskScore = sumRiskScore / totalClients;
        double avgAmount = totalAmount / totalClients;
        double avgTransactions = (double) totalTransactions / totalClients;

        // Monta relatório consolidado
        StringBuilder report = new StringBuilder();

        // Cabeçalho da categoria
        report.append(String.format("\n========== RISK CATEGORY: %s ==========\n",
                riskCategory));
        report.append(String.format("Total Clients: %d\n", totalClients));
        report.append(String.format("Average Risk Score: %.2f\n", avgRiskScore));
        report.append(String.format("Total Amount: $%.2f\n", totalAmount));
        report.append(String.format("Average Amount per Client: $%.2f\n", avgAmount));
        report.append(String.format("Average Transactions per Client: %.1f\n",
                avgTransactions));
        report.append("\n");

        // Top 10 clientes mais arriscados desta categoria
        report.append("--- TOP 10 HIGHEST RISK CLIENTS ---\n");
        int limit = Math.min(10, clients.size());
        for (int i = 0; i < limit; i++) {
            ClientRisk c = clients.get(i);
            report.append(String.format("%d. Client %s (Score: %.2f, Transactions: %d, " +
                            "Amount: $%.2f)\n   Factors: %s\n",
                    i + 1, c.clientId, c.riskScore, c.transactionCount,
                    c.totalAmount, c.riskFactors));
        }

        report.append("========================================\n");

        // Emite relatório
        context.write(key, new Text(report.toString()));
        context.getCounter("Step3", "REPORTS_GENERATED").increment(1);
        context.getCounter("Step3", "CLIENTS_IN_" + riskCategory).increment(totalClients);
    }
}