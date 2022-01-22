package Writer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Encoders;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class HDFSWriter implements Consumer<Dataset<Row>> {

    public HBaseWriter() {
    }

    @Override
    public void accept(Dataset<Row> ds) {
        String outlines = ds.map( e -> String.format("%d", e.getValue())).collect(Collectors.joining("\n"));
        FSDataOutputStream outstream = hdfs.create(outputPath.suffix(String.format("/variations/%s", locatedFileStatus.getPath().getName())));
        IOUtils.write(outlines, outstream, "UTF-8");
        outstream.close();
        instream.close();
    }
}
