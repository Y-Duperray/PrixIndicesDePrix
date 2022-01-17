package Writer;

import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

public class FileWriter implements Consumer<Dataset<Float>> {
    String outputPathStr = "";
    public FileWriter(String outputPath){
        this.outputPathStr = outputPath;
    }
    @Override
    public void accept(Dataset<Float> floatDataset) {
        System.out.println("FileWriter.accept()");
        floatDataset.coalesce(1).write().format("csv").mode("overwrite").save(outputPathStr+"resultat.csv");
    }
}
