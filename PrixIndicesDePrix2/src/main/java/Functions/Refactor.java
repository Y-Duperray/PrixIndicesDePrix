package Functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.function.Function;

@Slf4j
public class Refactor implements Function<Dataset<Row>, Dataset<Float>> {
    private SparkSession sparkSession;

    public Refactor(SparkSession session){
        this.sparkSession = session;
    }
    @Override
    public Dataset<Float> apply(Dataset<Row> rowDataset) {
        System.out.println("Refactor.apply()");
        /*float min = 999999999;
        float max = 0;
        float somme = 0;
        List<Float> floatList = rowDataset.select("Prix")
                .map(r -> Float.valueOf(r.getAs("Prix").toString()), Encoders.FLOAT()).collectAsList();
        for(int i = 0; i < floatList.size(); i++){
            if(floatList.get(i) < min){
                min = floatList.get(i);
            }
            if(floatList.get(i) > max){
                max = floatList.get(i);
            }
            somme += floatList.get(i);
        }
        float moyenne = somme / floatList.size();
        float[] retour = {min, max, moyenne};

        return retour;*/

        List<Float> floatList = rowDataset.select("Prix")
                .map(r -> Float.valueOf(r.getAs("Prix").toString()), Encoders.FLOAT()).collectAsList();
        for(int i = 0; i < floatList.size() - 1; i++){
            floatList.set(i, (floatList.get(i) - floatList.get(i++)));
        }
        sparkSession.createDataset(floatList, Encoders.FLOAT());
        return sparkSession.createDataset(floatList, Encoders.FLOAT());
    }
}
