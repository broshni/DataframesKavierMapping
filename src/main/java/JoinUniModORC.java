import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.LoggerFactory;

/**
 * Created by roshni on 11/15/16.
 */
public class JoinUniModORC {
    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(ORCfile_uniMod.class);

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        //toplevel folder where wget wrote the data to
        String localDir = "/Users/roshni/Desktop/Dataframes/dataframes.rcsb.org";

        // assumes the human genetic data is available as a parquet file
        // also needs the uniprot-PDB mapping parquet file

        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("available cores: " + cores);

        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("map to PDB");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

        DataFrame uni = sqlContext.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/uniprotPDB");
        uni.registerTempTable("uni");
        System.out.println("size uniprot:" + uni.count());
        uni.show(2);

        DataFrame mod = sqlContext.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/protmod");
        mod.registerTempTable("mod");
        System.out.println("size protmod:" + mod.count());
        mod.show(2);

        // join the ProtMod with Uniprot
        DataFrame uniprotPDBMod= sqlContext.sql("select * from uni left join mod " +
                "where uni.uniProtId = mod.uniProtId_M and uni.uniProtPos=mod.uniProtPos_M");
        uniprotPDBMod.registerTempTable("uniprotPDBMod");
        uniprotPDBMod.show(5);

    }
}
