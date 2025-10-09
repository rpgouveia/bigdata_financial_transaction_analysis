package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StateAggReducer extends Reducer<Text, StateClientAggWritable, Text, StateSummaryWritable> {

    private static final int TOPN = 5;

    @Override
    protected void reduce(Text state, Iterable<StateClientAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long total=0, low=0, med=0, high=0;
        Map<String, Long> cityHigh = new HashMap<>();

        for (StateClientAggWritable v : values) {
            total += v.getTotalClients();
            low   += v.getLowRiskClients();
            med   += v.getMedRiskClients();
            high  += v.getHighRiskClients();

            Map<String, Long> m = v.getHighRiskCityCountsAsMap();
            for (Map.Entry<String, Long> e : m.entrySet()) {
                cityHigh.merge(e.getKey(), e.getValue(), Long::sum);
            }
        }

        List<Map.Entry<String, Long>> topCities = new ArrayList<>(cityHigh.entrySet());
        topCities.sort((a,b)-> Long.compare(b.getValue(), a.getValue()));
        if (topCities.size() > TOPN) topCities = topCities.subList(0, TOPN);

        StateSummaryWritable out = new StateSummaryWritable(total, low, med, high, topCities, TOPN);
        ctx.write(state, out);
    }
}
