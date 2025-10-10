package routines.advanced.rfmbyuf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/** Contém total/low/med/high e o mapa de cidades (High Value). */
public class StateClientAggWritable implements Writable {

    private long totalClients;
    private long lowClients;
    private long medClients;
    private long highClients;
    private Map<String, Long> highCities;

    public StateClientAggWritable() {
        highCities = new HashMap<>();
    }

    public void reset(long total, long low, long med, long high) {
        this.totalClients = total;
        this.lowClients = low;
        this.medClients = med;
        this.highClients = high;
        this.highCities.clear();
    }

    public void add(StateClientAggWritable other) {
        this.totalClients += other.totalClients;
        this.lowClients += other.lowClients;
        this.medClients += other.medClients;
        this.highClients += other.highClients;
        for (Map.Entry<String, Long> e : other.highCities.entrySet()) {
            this.highCities.merge(e.getKey(), e.getValue(), Long::sum);
        }
    }

    public void addHighCity(String city, long cnt) {
        highCities.merge(city, cnt, Long::sum);
    }

    public long getTotal() { return totalClients; }
    public long getLow() { return lowClients; }
    public long getMed() { return medClients; }
    public long getHigh() { return highClients; }
    public Map<String, Long> getHighCities() { return highCities; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(totalClients);
        out.writeLong(lowClients);
        out.writeLong(medClients);
        out.writeLong(highClients);
        out.writeInt(highCities.size());
        for (Map.Entry<String, Long> e : highCities.entrySet()) {
            writeS(out, e.getKey());
            out.writeLong(e.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalClients = in.readLong();
        lowClients = in.readLong();
        medClients = in.readLong();
        highClients = in.readLong();
        highCities.clear();
        int n = in.readInt();
        for (int i=0;i<n;i++){
            String city = readS(in);
            long v = in.readLong();
            highCities.put(city, v);
        }
    }

    private static void writeS(DataOutput out, String s) throws IOException {
        if (s == null) { out.writeInt(-1); return; }
        byte[] b = s.getBytes("UTF-8");
        out.writeInt(b.length); out.write(b);
    }
    private static String readS(DataInput in) throws IOException {
        int len = in.readInt(); if (len < 0) return "";
        byte[] b = new byte[len]; in.readFully(b);
        return new String(b, "UTF-8");
    }
}
