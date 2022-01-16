package functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Year {
    Dataset<Row> valeurs;

    public Year(Dataset<Row> valeursDataset){
        System.out.println("New Year created. ");
        this.valeurs = valeursDataset;
    }

    public String getMin() {
        return null;
    }

    public String getMax() {
        return null;
    }

    public String getEcart() {
        return null;
    }

    public String getAvg() {
        float moyenne;
        float somme;
        float diviseur;
        return null;
    }
}
