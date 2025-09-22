package routines.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Classe Mapper para o WordCount
 * Responsável por processar cada linha de entrada e emitir pares (palavra, 1)
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objetos reutilizáveis para otimização
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * Implementação do método map
     * @param key - posição da linha no arquivo (offset)
     * @param value - conteúdo da linha
     * @param context - contexto para emitir pares chave-valor
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Converter a linha para string e dividir em palavras
        // Usa espaços em branco como delimitador
        String[] words = value.toString().split("\\s+");

        // Iterar sobre cada palavra
        for (String wordStr : words) {
            // Limpar a palavra: converter para minúscula e remover pontuação
            String cleanedWord = wordStr.toLowerCase()
                    .replaceAll("[^a-zA-Z0-9]", "");

            // Ignorar strings vazias após limpeza
            if (!cleanedWord.isEmpty()) {
                // Definir a palavra limpa
                word.set(cleanedWord);

                // Emitir o par (palavra, 1)
                context.write(word, one);
            }
        }
    }
}