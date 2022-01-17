import Reader.FileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

public class FileReaderTest {
    String inputFile ="src/main/resources/valeurs_test.csv";
    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    FileReader fileReader = new FileReader(inputFile, sparkSession);
    List<String> expected = Arrays.asList("line1", "line2", "line3", "line4", "line5");

    @Test
    public void test(){
        Dataset<Row> actual = fileReader.get();
        assertThat(actual.collectAsList().toString()).contains(expected);
    }
}
