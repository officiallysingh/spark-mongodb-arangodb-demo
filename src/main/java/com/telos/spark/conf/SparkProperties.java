package com.telos.spark.conf;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@NoArgsConstructor
@ToString
@Valid
@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

    @NotEmpty
    private String appName;

    private String master = "local[*]";

    private Executor executor = new Executor();

    @NotNull
    private Mongo mongo = new Mongo();

    @NotNull
    private Arango arango = new Arango();


    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Executor {

        private int instances = 3;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Mongo {

        @NotNull
        private Connection connection;

        @Getter
        @Setter
        @NoArgsConstructor
        @ToString
        @Valid
        public static class Connection {

            @NotEmpty
            private String url;

            @NotEmpty
            private String database;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Arango {

        @NotEmpty
        private String[] endpoints;

        @NotEmpty
        private String username = "root";

        @NotEmpty
        private String password;

        @NotEmpty
        private String database = "system";

        private boolean sslEnabled;

        private String sslCertValue = "";

        public String endpoints() {
            return String.join(",", this.endpoints);
        }
    }
}
