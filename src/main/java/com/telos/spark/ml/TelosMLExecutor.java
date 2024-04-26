package com.telos.spark.ml;

import com.telos.spark.data.ArangoDataframeLoader;
import com.telos.spark.data.MongoDataframeLoader;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TelosMLExecutor {

    private final ArangoDataframeLoader arangoDataframeLoader;

    private final MongoDataframeLoader mongoDataframeLoader;

    public void execute() {

        Dataset<Row> moviesDf = this.arangoDataframeLoader.load();
        moviesDf.printSchema();
        moviesDf.show(5);

        Dataset<Row> peopleDf = this.mongoDataframeLoader.load();
        peopleDf.printSchema();
        peopleDf.show(5);
    }
}
