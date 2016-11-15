import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by roshni on 11/8/16.
 */
public class KaviarMapping {
    public static void main(String[] args) {

        //toplevel folder where wget wrote the data to
        String localDir = "/Users/roshni/Desktop/Dataframes/dataframes.rcsb.org";

        // assumes the human genetic data is available as a parquet file
        // also needs the uniprot-PDB mapping parquet file

        int cores = Runtime.getRuntime().availableProcessors();

        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("map SNP to PDB");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // register the UniProt to PDB mapping
        DataFrame uniprotPDB = sqlContext.read().parquet(localDir + "/parquet/uniprotpdb/20161104");

        uniprotPDB.registerTempTable("uniprotPDB");

        System.out.println("Example row from PDB to UniProt mapping:");
        uniprotPDB.show(5);

        DataFrame ProtMod= sqlContext.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/Users/roshni/Desktop/ProtMod.csv");
        ProtMod.registerTempTable("ProtMod");
        System.out.println("Example row from ProtMod:");
        ProtMod.show(5);

        // join the ProtMod with Uniprot
        DataFrame uniprotPDBMod= sqlContext.sql("select * from uniprotPDB left join ProtMod where uniprotPDB.uniProtId = ProtMod.uniProtId and uniprotPDB.uniProtPos=ProtMod.uniProtPos");
        uniprotPDBMod.registerTempTable("uniprotPDBMod");
        uniprotPDBMod.show(5);

        //load the mapping from the human genome (assembly 38) to UniProt and look at a SNP.
        DataFrame chr11 = sqlContext.read().parquet(localDir + "/parquet/humangenome/20161105/hg38/chr11");

        chr11.registerTempTable("chr11");


        DataFrame sickeCellSNP = sqlContext.sql("select * from chr11 where position = 5227002");

        System.out.println("human genome mapping to UniProt for SNP:");
        sickeCellSNP.show();

        // map this position to PDB

        sickeCellSNP.registerTempTable("snp");

        // join genomic info with UniProt to PDB mapping
        DataFrame map2PDB = sqlContext.sql("select * from snp left join uniprotPDBMod where snp.uniProtId = uniprotPDBMod.uniProtId and snp.uniProtPos = uniprotPDBMod.uniProtPos ");

        System.out.println("All PDB entries that map to this SNP");
        map2PDB.show();

    }
}