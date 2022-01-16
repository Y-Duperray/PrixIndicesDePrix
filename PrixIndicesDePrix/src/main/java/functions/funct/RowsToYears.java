package functions.funct;

import functions.Year;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RowsToYears implements Function<Dataset<Row>, Dataset<Year>> {
    private final SparkSession sparkSession;

    public RowsToYears(SparkSession session){
        System.out.println("New RowsToYears created. ");
        this.sparkSession = session;
    }

    @Override
    public Dataset<Year> apply(Dataset<Row> rowDataset) {
        System.out.println("RowsToYears.apply() ");
        List<String> years = new ArrayList<String>();
        List<Dataset<Row>> rowsByYears = new ArrayList();
        List<Year> yearsList = new ArrayList();

        rowDataset.foreach(row -> {
            if(!years.isEmpty()){
                if(!years.contains(row.getAs("Période").toString().split("-")[0])){
                    System.out.println("Year addition : "+row.getAs("Période").toString().split("-")[0]);//debug
                    years.add(row.getAs("Période").toString().split("-")[0]);
                }
            }else{
                System.out.println("Year addition : "+row.getAs("Période").toString().split("-")[0]);//debug
                years.add(row.getAs("Période").toString().split("-")[0]);
            }
            System.out.println("years.size() : "+years.size());//debug
        });

        System.out.println("years.size() : "+years.size());//debug

        years.forEach((year) -> {
            Dataset<Row> rowsByYear = rowDataset.filter(row -> row.getAs("Période").toString().split("-")[0].equals(year));
            rowsByYears.add(rowsByYear);
        });

        System.out.println("rowsByYears.size() : "+rowsByYears.size());//debug

        rowsByYears.forEach((rowsByYear) -> {
            Year year = new Year(rowsByYear.select("Prix"));
            yearsList.add(year);
        });

        System.out.println("yearsList.size() : "+yearsList.size());//debug

        return sparkSession.createDataset(yearsList, Encoders.bean(Year.class));
    }
}
