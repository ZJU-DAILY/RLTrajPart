package utils;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.locationtech.jts.geom.Coordinate;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Params {
    /**
     * Config File
     */
    public final String YAML_CONFIG = "trajectoryflink-conf.yml";
    public final String YAML_PATH = new File(".").getAbsoluteFile().getParent().toString() + File.separator +
            "conf" +  File.separator + YAML_CONFIG;
//    public final String YAML_PATH = "."+ File.separator + YAML_CONFIG;
    /**
     * Parameters
     */
    /* Cluster */
    public boolean clusterMode = false;
    public int parallelism = 1;
    public String kafkaBootStrapServers;

    /* Stream1 Input */
    public String inputTopicName;
    public String inputFormat;
    public String dateFormatStr;
    public List<Double> gridBBox = new ArrayList<>();
    public int numGridCells = 0;
    public String inputDelimiter;
    public String charset;


    /* Query */
    public double queryRadius = Double.MIN_VALUE; // Default 10x10 Grid

    /* Window */
    public int windowInterval = Integer.MIN_VALUE;


    public Params() throws NullPointerException, IllegalArgumentException, NumberFormatException {
        System.out.println(YAML_PATH);
        ConfigType config = getYamlConfig(YAML_PATH);

        /* Cluster */
        clusterMode = config.isClusterMode();
        if ((kafkaBootStrapServers = config.getKafkaBootStrapServers()) == null) {
            throw new NullPointerException("kafkaBootStrapServers is " + config.getKafkaBootStrapServers());
        }
        /* Input Stream1  */
        try {
            if ((inputTopicName = (String)config.getInputStream1().get("topicName")) == null) {
                throw new NullPointerException("inputTopicName1 is " + config.getInputStream1().get("topicName"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputTopicName1 : " + e);
        }
        try {
            if ((inputFormat = (String)config.getInputStream1().get("format")) == null) {
                throw new NullPointerException("format1 is " + config.getInputStream1().get("format"));
            }
            else {
                List<String> validParam = Arrays.asList("GeoJSON", "WKT", "CSV", "TSV");
                if (!validParam.contains(inputFormat)) {
                    throw new IllegalArgumentException(
                            "format1 is " + inputFormat + ". " +
                                    "Valid value is \"GeoJSON\", \"WKT\", \"CSV\" or \"TSV\".");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("format1 : " + e);
        }
        try {
            if ((dateFormatStr = (String)config.getInputStream1().get("dateFormat")) == null) {
                throw new NullPointerException("dateFormat1 is " + config.getInputStream1().get("dateFormat"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("dateFormat1 : " + e);
        }
        try {
            if ((gridBBox = (ArrayList)config.getInputStream1().get("gridBBox")) == null) {
                throw new NullPointerException("gridBBox1 is " + config.getInputStream1().get("gridBBox"));
            }
            if (gridBBox.size() != 4) {
                throw new IllegalArgumentException("gridBBox1 num is " + gridBBox.size());
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("gridBBox1 : " + e);
        }
        try {
            if(config.getInputStream1().get("numGridCells") == null) {
                throw new NullPointerException("numGridCells1 is " + config.getInputStream1().get("numGridCells"));
            }
            else {
                numGridCells = (int)config.getInputStream1().get("numGridCells");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("numGridCells1 : " + e);
        }
        try {
            if((inputDelimiter = (String)config.getInputStream1().get("delimiter")) == null) {
                throw new NullPointerException("inputDelimiter1 is " + config.getQuery().get("delimiter"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputDelimiter1 : " + e);
        }
        try {
            if((charset = (String)config.getInputStream1().get("charset")) == null) {
                throw new NullPointerException("charset1 is " + config.getInputStream1().get("charset"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("charset1 : " + e);
        }

        try {
            if(config.getQuery().get("parallelism") == null) {
                throw new NullPointerException("parallelism is " + config.getQuery().get("parallelism"));
            }
            else {
                parallelism = (int)config.getQuery().get("parallelism");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("parallelism : " + e);
        }


        try {
            if(config.getQuery().get("radius") == null) {
                throw new NullPointerException("radius is " + config.getQuery().get("radius"));
            }
            else {
                queryRadius = (double)config.getQuery().get("radius");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("radius : " + e);
        }



        try {
            if(config.getWindow().get("interval") == null) {
                throw new NullPointerException("interval is " + config.getWindow().get("interval"));
            }
            else {
                windowInterval = (int)config.getWindow().get("interval");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("interval : " + e);
        }

    }

    private ConfigType getYamlConfig(String path) {
        File file = new File(path);
        Yaml yaml = new Yaml();
        FileInputStream input;
        InputStreamReader stream;
        try {
            input = new FileInputStream(file);
            stream = new InputStreamReader(input, "UTF-8");
            return (ConfigType)yaml.load(stream);
        }
        catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String toString() {
        return "clusterMode = " + clusterMode + ", " +
                "kafkaBootStrapServers = " + kafkaBootStrapServers + ", " +
                "\n" +
                "inputTopicName1 = " + inputTopicName + ", " +
                "format1 = " + inputFormat + ", " +
                "dateFormatStr1 = " + dateFormatStr + ", " +
                "gridBBox1 = " + gridBBox + ", " +
                "numGridCells1 = " + numGridCells + ", " +
                "inputDelimiter1 = " + inputDelimiter + ", " +
                "charset1 = " + charset + ", " +
                "\n" +
                "queryRadius = " + queryRadius + ", " +
                "\n" +
                "windowInterval = " + windowInterval + ", ";
    }
}
