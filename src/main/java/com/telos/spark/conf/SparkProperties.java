package com.telos.spark.conf;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@NoArgsConstructor
@ToString
@Valid
@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

  @NotEmpty private String appName;

  private String master = "local[*]";

  private Executor executor = new Executor();

  @NotNull private Mongo mongo = new Mongo();

  @NotNull private Arango arango = new Arango();

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

    @NotEmpty private String url;

    @NotNull private Feature feature = new Feature();

    @NotNull private Inference inference = new Inference();

    @NotNull private Label label = new Label();

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Feature {

      @NotEmpty private String database;

      @NotEmpty private String collection;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Inference {

      @NotEmpty private String database;

      @NotEmpty private String collection;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    @Valid
    public static class Label {

      @NotEmpty private String database;

      @NotEmpty private String collection;
    }
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @ToString
  @Valid
  public static class Arango {

    @NotEmpty private String[] endpoints;

    private String username = "root";

    private String password = "";

    @NotEmpty private String database = "_system";

    private boolean sslEnabled;

    private String sslCertValue = "";

    public String endpoints() {
      return String.join(",", this.endpoints);
    }
  }
}
