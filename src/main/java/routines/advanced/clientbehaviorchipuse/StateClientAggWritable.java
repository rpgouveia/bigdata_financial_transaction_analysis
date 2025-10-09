package routines.advanced.clientbehaviorchipuse;

import java.io.*;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.io.*;

/**
 * Agregado por UF (contagem de clientes Low/Med/High + mapa cidade→highRiskClients)
 * Usado como saída do Job 1 e entrada/combinação no Job 2.
 */
public class StateClientAggWritable implements Writable {

    private long totalClients;
    private long lowRiskClients;
    private long medRiskClients;
    private long highRiskClients;

    // Para hotspots: quantos clientes High Risk por cidade
    private MapWritable highRiskCityCounts;

    public StateClientAggWritable() {
        this.highRiskCityCounts = new MapWritable();
    }

    public StateClientAggWritable(long total, long low, long med, long high, Map<String, Long> cityHighRisk) {
        this();
        this.totalClients = total;
        this.lowRiskClients = low;
        this.medRiskClients = med;
        this.highRiskClients = high;
        if (cityHighRisk != null) {
            for (Map.Entry<String, Long> e : cityHighRisk.entrySet()) {
                if (e.getKey() == null) continue;
                this.highRiskCityCounts.put(new Text(e.getKey()), new LongWritable(e.getValue()));
            }
        }
    }

    public void add(StateClientAggWritable other) {
        this.totalClients += other.totalClients;
        this.lowRiskClients += other.lowRiskClients;
        this.medRiskClients += other.medRiskClients;
        this.highRiskClients += other.highRiskClients;

        for (Map.Entry<Writable, Writable> e : other.highRiskCityCounts.entrySet()) {
            Text city = (Text) e.getKey();
            long inc = ((LongWritable) e.getValue()).get();
            LongWritable cur = (LongWritable) this.highRiskCityCounts.get(city);
            if (cur == null) {
                this.highRiskCityCounts.put(new Text(city), new LongWritable(inc));
            } else {
                cur.set(cur.get() + inc);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(totalClients);
        out.writeLong(lowRiskClients);
        out.writeLong(medRiskClients);
        out.writeLong(highRiskClients);
        highRiskCityCounts.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalClients = in.readLong();
        lowRiskClients = in.readLong();
        medRiskClients = in.readLong();
        highRiskClients = in.readLong();
        highRiskCityCounts.readFields(in);
    }

    public long getTotalClients() { return totalClients; }
    public long getLowRiskClients() { return lowRiskClients; }
    public long getMedRiskClients() { return medRiskClients; }
    public long getHighRiskClients() { return highRiskClients; }

    public Map<String, Long> getHighRiskCityCountsAsMap() {
        Map<String, Long> map = new HashMap<>();
        for (Map.Entry<Writable, Writable> e : highRiskCityCounts.entrySet()) {
            map.put(e.getKey().toString(), ((LongWritable)e.getValue()).get());
        }
        return map;
    }

    public static StateClientAggWritable singleClient(String riskBucket, String city) {
        long low=0, med=0, high=0;
        if ("LOW".equals(riskBucket)) low = 1;
        else if ("MED".equals(riskBucket)) med = 1;
        else high = 1;

        Map<String, Long> hrCity = null;
        if ("HIGH".equals(riskBucket) && city != null && !city.isEmpty()) {
            hrCity = new HashMap<>();
            hrCity.put(city, 1L);
        }
        return new StateClientAggWritable(1, low, med, high, hrCity);
    }
}
