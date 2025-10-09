package routines.advanced.clientbehaviorchipuse;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;

public class StateSummaryWritable implements WritableComparable<StateSummaryWritable> {

    private long totalClients;
    private long lowRiskClients;
    private long medRiskClients;
    private long highRiskClients;

    // Top N cidades por clientes High Risk
    private String[] topCities;
    private long[] topCounts;
    private int size;

    public StateSummaryWritable() {
        this.topCities = new String[5];
        this.topCounts = new long[5];
        this.size = 0;
    }

    public StateSummaryWritable(long total, long low, long med, long high,
                                List<Map.Entry<String, Long>> topCityEntries, int topN) {
        this();
        this.totalClients = total;
        this.lowRiskClients = low;
        this.medRiskClients = med;
        this.highRiskClients = high;
        this.size = Math.min(topN, topCityEntries.size());
        for (int i = 0; i < this.size; i++) {
            this.topCities[i] = topCityEntries.get(i).getKey();
            this.topCounts[i] = topCityEntries.get(i).getValue();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(totalClients);
        out.writeLong(lowRiskClients);
        out.writeLong(medRiskClients);
        out.writeLong(highRiskClients);
        out.writeInt(size);
        for (int i=0;i<size;i++) {
            WritableUtils.writeString(out, topCities[i] == null ? "" : topCities[i]);
            out.writeLong(topCounts[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalClients = in.readLong();
        lowRiskClients = in.readLong();
        medRiskClients = in.readLong();
        highRiskClients = in.readLong();
        size = in.readInt();
        topCities = new String[Math.max(size, 5)];
        topCounts = new long[Math.max(size, 5)];
        for (int i=0;i<size;i++) {
            topCities[i] = WritableUtils.readString(in);
            topCounts[i] = in.readLong();
        }
    }

    @Override
    public int compareTo(StateSummaryWritable o) {
        // Ordena por % High Risk desc
        double thisPct = totalClients > 0 ? (highRiskClients * 1.0 / totalClients) : 0.0;
        double otherPct = o.totalClients > 0 ? (o.highRiskClients * 1.0 / o.totalClients) : 0.0;
        return Double.compare(otherPct, thisPct);
    }

    @Override
    public String toString() {
        double lowPct  = totalClients>0 ? (lowRiskClients  *100.0/totalClients) : 0.0;
        double medPct  = totalClients>0 ? (medRiskClients  *100.0/totalClients) : 0.0;
        double highPct = totalClients>0 ? (highRiskClients *100.0/totalClients) : 0.0;

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Clients: %d | Low: %d (%.2f%%) | Med: %d (%.2f%%) | High: %d (%.2f%%)",
                totalClients, lowRiskClients, lowPct, medRiskClients, medPct, highRiskClients, highPct));
        if (size > 0) {
            sb.append(" | Top Cities (High Risk): ");
            for (int i=0;i<size;i++) {
                if (i>0) sb.append(" | ");
                sb.append(topCities[i]).append(": ").append(topCounts[i]);
            }
        }
        return sb.toString();
    }
}
