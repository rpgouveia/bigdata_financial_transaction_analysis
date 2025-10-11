package routines.advanced.riskanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator customizado para ordenar categorias de risco por severidade
 * ao invés de ordem alfabética.
 *
 * Ordem desejada: CRITICAL > HIGH > MEDIUM > LOW
 * Ordem alfabética padrão: CRITICAL > HIGH > LOW > MEDIUM (ERRADO!)
 *
 * Este comparator garante que os relatórios do Step 3 apareçam
 * na ordem correta de severidade.
 */
public class RiskCategoryComparator extends WritableComparator {

    /**
     * Construtor - registra este comparator para a classe Text
     */
    public RiskCategoryComparator() {
        super(Text.class, true);
    }

    /**
     * Compara duas categorias de risco por severidade.
     *
     * @param a Primeira categoria
     * @param b Segunda categoria
     * @return valor negativo se a < b, zero se a == b, valor positivo se a > b
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text category1 = (Text) a;
        Text category2 = (Text) b;

        int rank1 = getRiskRank(category1.toString());
        int rank2 = getRiskRank(category2.toString());

        // Ordem decrescente de severidade (maior rank primeiro)
        return Integer.compare(rank2, rank1);
    }

    /**
     * Retorna ranking numérico da categoria de risco.
     * Quanto maior o número, mais severa a categoria.
     *
     * @param category Nome da categoria (CRITICAL, HIGH, MEDIUM, LOW)
     * @return Rank numérico (4=CRITICAL, 3=HIGH, 2=MEDIUM, 1=LOW, 0=unknown)
     */
    private int getRiskRank(String category) {
        switch (category.toUpperCase()) {
            case "CRITICAL":
                return 4;  // Mais severo
            case "HIGH":
                return 3;
            case "MEDIUM":
                return 2;
            case "LOW":
                return 1;  // Menos severo
            default:
                return 0;  // Categoria desconhecida - coloca no final
        }
    }
}