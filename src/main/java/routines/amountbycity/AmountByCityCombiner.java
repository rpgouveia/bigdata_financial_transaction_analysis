package routines.amountbycity;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class para AmountByCity
 * Realiza pré-agregação local dos valores por cidade antes de enviar para o Reducer final
 * Trabalha com centavos (LongWritable) para manter precisão
 */
public class AmountByCityCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    // Objeto reutilizável para o resultado
    private LongWritable result = new LongWritable();

    /**
     * Método reduce do Combiner - agrega valores localmente por cidade
     * @param key Nome da cidade
     * @param values Lista de valores (em centavos) para esta cidade no nó local
     * @param context Contexto para emitir o resultado pré-agregado
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long cityTotalInCents = 0;

        // Somar todos os valores locais para esta cidade
        for (LongWritable value : values) {
            cityTotalInCents += value.get();
        }

        // Emitir o resultado pré-agregado (cidade, total_local_em_centavos)
        result.set(cityTotalInCents);
        context.write(key, result);
    }
}