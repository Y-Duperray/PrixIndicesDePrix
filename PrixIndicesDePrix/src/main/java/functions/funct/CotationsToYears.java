package functions.funct;

import functions.Cotation;
import functions.Year;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CotationsToYears implements Function<Dataset<Cotation>, Dataset<Year>> {
    private final SparkSession sparkSession;
    public CotationsToYears(SparkSession session){
        System.out.println("New CotationsToYears created. ");
        this.sparkSession = session;
    }
    @Override
    public Dataset<Year> apply(Dataset<Cotation> cotationDataset) {
        System.out.println("CotationsToYears.apply() ");
        List<String> years = new ArrayList();
        List<Dataset<Cotation>> cotationsByYears = new ArrayList();
        List<Year> yearsList = new ArrayList();
        cotationDataset.foreach(cotation -> {
            if(!years.isEmpty()){
                if(!years.contains(cotation.getAnnee())){
                    years.add(cotation.getAnnee());
                }
            }else{
                years.add(cotation.getAnnee());
            }
        });

        years.forEach((year) -> {
            Dataset<Cotation> cotationsByYear = cotationDataset.filter(cotation -> cotation.getAnnee().equals(year));
            cotationsByYears.add(cotationsByYear);
        });

        cotationsByYears.forEach((cotationsByYear) -> {
            Year year = new Year(cotationsByYear.select("valeur"));
            yearsList.add(year);
        });
        return sparkSession.createDataset(yearsList, Encoders.bean(Year.class));
    }
}