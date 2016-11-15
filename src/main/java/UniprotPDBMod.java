import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Created by roshni on 11/9/16.
 */
public class UniprotPDBMod {

    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(UniprotPDBMod.class);

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
                .setAppName("map SNP to PDB");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // register the UniProt to PDB mapping
        DataFrame uniprotPDB = sqlContext.read()
                .parquet(localDir + "/parquet/uniprotpdb/20161104")
                .sample(false,0.0000001);

        uniprotPDB.registerTempTable("uniprotPDB");

        System.out.println("size uniprot:" + uniprotPDB.count());

        uniprotPDB = uniprotPDB.repartition(uniprotPDB.col("uniProtId"));

        System.out.println("Example row from PDB to UniProt mapping:");
        uniprotPDB.show(5);

        DataFrame protMod= sqlContext.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/Users/roshni/Desktop/ProtMod.csv").sample(false,0.0001);


         protMod = protMod.repartition(protMod.col("uniProtId_M"));

        protMod.registerTempTable("protMod");

        System.out.println("Size protmod: " + protMod.count());

        System.out.println("Example row from protMod:");
        protMod.show(5);

        // join the ProtMod with Uniprot
        DataFrame uniprotPDBMod= sqlContext.sql("select * from uniprotPDB left join protMod where uniprotPDB.uniProtId = protMod.uniProtId_M and uniprotPDB.uniProtPos=protMod.uniProtPos_M");
        uniprotPDBMod.registerTempTable("uniprotPDBMod");
        uniprotPDBMod.show(5);
        uniprotPDBMod
                .write()
                .mode(SaveMode.Overwrite)
                .save("/Users/roshni/Desktop/dataframes.rcsb.org/parquet/uniprotPDBMod.parquet");
    }
}
