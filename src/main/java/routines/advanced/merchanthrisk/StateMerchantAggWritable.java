package routines.advanced.merchanthrisk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Writable;

/**
 * Writable agregado por UF (JOB 2)
 *
 * Contém:
 *  - Contadores de merchants por HEALTH (A/B/C) e RISK (LOW/MED/HIGH)
 *  - Mapa de hotspots: cidade → #merchants HIGH
 *  - Top-K merchants por valor somado (listas id/valor)
 *
 * Projeta merge associativo (para Combiner/Reducer) e método pushTop
 * que mantém apenas os K maiores.
 */
public class StateMerchantAggWritable implements Writable {

    private long totalMerchants;
    private long healthA, healthB, healthC;
    private long riskLow, riskMed, riskHigh;

    private Map<String, Integer> highRiskCityCounts = new HashMap<>();
    private List<String> topIds = new ArrayList<>();
    private List<Long> topVals = new ArrayList<>();
    private int k = 5; // configurável via setK

    public void setK(int k) { this.k = k; }
    public int getK() { return k; }

    public void addOneMerchant(String healthBucket, String riskBucket,
                               String highRiskCityOrNull,
                               String merchId, long sumCents) {
        totalMerchants++;
        if ("A".equals(healthBucket)) healthA++; else if ("B".equals(healthBucket)) healthB++; else healthC++;
        if ("LOW".equals(riskBucket)) riskLow++; else if ("MED".equals(riskBucket)) riskMed++; else riskHigh++;

        if ("HIGH".equals(riskBucket) && highRiskCityOrNull != null && !highRiskCityOrNull.isEmpty()) {
            String c = highRiskCityOrNull.toUpperCase();
            highRiskCityCounts.put(c, highRiskCityCounts.getOrDefault(c, 0) + 1);
        }

        pushTop(merchId, sumCents);
    }

    private void pushTop(String id, long val) {
        topIds.add(id);
        topVals.add(val);
        if (topIds.size() > k) {
            // remove o pior (menor valor)
            int worst = 0;
            long w = topVals.get(0);
            for (int i = 1; i < topVals.size(); i++) {
                if (topVals.get(i) < w) { w = topVals.get(i); worst = i; }
            }
            topIds.remove(worst);
            topVals.remove(worst);
        }
    }

    public void merge(StateMerchantAggWritable other) {
        this.totalMerchants += other.totalMerchants;
        this.healthA += other.healthA; this.healthB += other.healthB; this.healthC += other.healthC;
        this.riskLow += other.riskLow; this.riskMed += other.riskMed; this.riskHigh += other.riskHigh;

        // soma mapas de hotspots
        for (Map.Entry<String,Integer> e : other.highRiskCityCounts.entrySet()) {
            this.highRiskCityCounts.put(e.getKey(),
                    this.highRiskCityCounts.getOrDefault(e.getKey(), 0) + e.getValue());
        }

        // merge top lists e manter K maiores
        for (int i = 0; i < other.topIds.size(); i++) {
            pushTop(other.topIds.get(i), other.topVals.get(i));
        }
        trimToK();
    }

    private void trimToK() {
        if (topIds.size() <= k) return;
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < topIds.size(); i++) idx.add(i);
        idx.sort((i,j) -> Long.compare(topVals.get(j), topVals.get(i)));

        List<String> nIds = new ArrayList<>();
        List<Long> nVals = new ArrayList<>();
        for (int i = 0; i < Math.min(k, idx.size()); i++) {
            int p = idx.get(i);
            nIds.add(topIds.get(p));
            nVals.add(topVals.get(p));
        }
        topIds = nIds; topVals = nVals;
    }

    // Getters (usados no Reducer final)
    public long getTotalMerchants() { return totalMerchants; }
    public long getHealthA() { return healthA; }
    public long getHealthB() { return healthB; }
    public long getHealthC() { return healthC; }
    public long getRiskLow() { return riskLow; }
    public long getRiskMed() { return riskMed; }
    public long getRiskHigh() { return riskHigh; }
    public Map<String,Integer> getHighRiskCityCounts() { return highRiskCityCounts; }
    public List<String> getTopIds() { return topIds; }
    public List<Long> getTopVals() { return topVals; }

    @Override public void write(DataOutput out) throws IOException {
        out.writeLong(totalMerchants);
        out.writeLong(healthA); out.writeLong(healthB); out.writeLong(healthC);
        out.writeLong(riskLow); out.writeLong(riskMed); out.writeLong(riskHigh);

        out.writeInt(highRiskCityCounts.size());
        for (Map.Entry<String,Integer> e : highRiskCityCounts.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeInt(e.getValue());
        }

        out.writeInt(k);
        out.writeInt(topIds.size());
        for (int i = 0; i < topIds.size(); i++) {
            out.writeUTF(topIds.get(i));
            out.writeLong(topVals.get(i));
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        totalMerchants = in.readLong();
        healthA = in.readLong(); healthB = in.readLong(); healthC = in.readLong();
        riskLow = in.readLong(); riskMed = in.readLong(); riskHigh = in.readLong();

        highRiskCityCounts.clear();
        int m = in.readInt();
        for (int i = 0; i < m; i++) {
            String k = in.readUTF();
            int v = in.readInt();
            highRiskCityCounts.put(k, v);
        }

        k = in.readInt();
        topIds.clear(); topVals.clear();
        int t = in.readInt();
        for (int i = 0; i < t; i++) {
            topIds.add(in.readUTF());
            topVals.add(in.readLong());
        }
    }
}
