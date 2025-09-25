package routines.amountbyclient;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class para AmountByClient
 * Realiza pré-agregação local dos valores por cliente antes de enviar para o Reducer final
 * Trabalha com centavos (LongWritable) para manter precisão
 */
public class AmountByClientCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    // Objeto reutilizável para o resultado
    private LongWritable result = new LongWritable();

    /**
     * Método reduce do Combiner - agrega valores localmente por cliente
     * @param key ID do cliente
     * @param values Lista de valores (em centavos) para este cliente no nó local
     * @param context Contexto para emitir o resultado pré-agregado
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long clientTotalInCents = 0;

        // Somar todos os valores locais para este cliente
        for (LongWritable value : values) {
            clientTotalInCents += value.get();
        }

        // Emitir o resultado pré-agregado (cliente, total_local_em_centavos)
        result.set(clientTotalInCents);
        context.write(key, result);
    }
}