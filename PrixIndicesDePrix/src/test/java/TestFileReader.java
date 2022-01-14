import functions.reader.FileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFileReader {
    @Test
    public void test() {
        /*String input = "C:/Users/Y.DUPERRAY/Documents/ARBORESCENCE/Scolaire/3iL/M2/Exploitation des données de masse/serie_010002078_11012022/valeurs_mensuelles.csv";
        String sample = "C:/Users/Y.DUPERRAY/Documents/ARBORESCENCE/Scolaire/3iL/M2/Exploitation des données de masse/sampleTestFileReader/sample.csv";
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileReader fileReader = new FileReader(input, sparkSession);
        Dataset<Row> expectedOutput = ;
        Dataset<Row> actualOutput = fileReader.get();

        assertThat(actualOutput.collectAsList()).containsExactlyInAnyOrder(expectedOutput);*/
    }
}
