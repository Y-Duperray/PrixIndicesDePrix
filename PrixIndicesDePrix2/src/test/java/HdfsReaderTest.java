import Reader.HDFSReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

@Slf4j
public class HdfsReaderTest {
    String inputPathStr = "file:///C:/Users/Y.DUPERRAY/Documents/ARBORESCENCE/Scolaire/3iL/M2/Exploitation des données de masse/PrixIndicesDePrix2/src/main/resources/valeurs_mensuelles.csv";
    @Test
    public void test(){
        LocatedFileStatus actual = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        String expected = inputPathStr;
        HDFSReader r = new HDFSReader(inputPathStr, sparkSession);
        actual = r.get();
        //assertThat(actual.getPath().toString())isEqualTo(expected);
    }
}
