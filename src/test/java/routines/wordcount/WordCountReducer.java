package routines.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Classe Reducer para o WordCount
 * Responsável por agregar as contagens de cada palavra e emitir o resultado final
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Objeto reutilizável para o resultado
    private IntWritable result = new IntWritable();

    /**
     * Implementação do método reduce
     * @param key - a palavra
     * @param values - lista de valores (contagens) para esta palavra
     * @param context - contexto para emitir o resultado final
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int occurrences = 0;

        // Iterar sobre todos os valores e somar as ocorrências
        for (IntWritable value : values) {
            occurrences += value.get();
        }

        // Definir o resultado total
        result.set(occurrences);

        // Emitir o par final (palavra, total_ocorrências)
        context.write(key, result);
    }
}