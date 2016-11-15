import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by roshni on 11/8/16.
 */
public class KaviarDataframeMapping {
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

        //read the kavier table

        DataFrame Kavier1= sqlContext.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/Users/roshni/Desktop/KavierTable_Dataframe.csv");

        Kavier1.registerTempTable("K1");
        Kavier1.show(5);


        // register the UniProt to PDB mapping
        DataFrame uniprotPDB = sqlContext.read().parquet(localDir + "/parquet/uniprotpdb/20161104");

        uniprotPDB.registerTempTable("uniprotPDB");

        System.out.println("Example row from PDB to UniProt mapping:");
        uniprotPDB.show(1);

        //load the mapping from the human genome (assembly 38) to UniProt and look at a SNP.
        DataFrame chr11 = sqlContext.read().parquet(localDir + "/parquet/humangenome/20161105/hg38/chr11");

        chr11.registerTempTable("chr11");

        //Map kaviar to genome

        DataFrame Kaviertogenome = sqlContext.sql("select * from K1 left join chr11 where K1.chromosomeName = chr11.chromosome and K1.position = chr11.position");
        Kaviertogenome.registerTempTable("KtoG");
        Kaviertogenome.show(5);



        // map this position to PDB
        // join genomic info with UniProt to PDB mapping
        DataFrame map2PDB = sqlContext.sql("select * from KtoG inner join uniprotPDB " +
                "where KtoG.uniProtId = uniprotPDB.uniProtId and KtoG.uniProtPos = uniprotPDB.uniProtPos ");
        System.out.println("Printing the whole table..");
        map2PDB.show(5);
        map2PDB.count();

    }
}
