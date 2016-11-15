/**
 * Created by roshni on 11/8/16.
 */
import java.io.*;


public class KaviarDataframe {
    public static void main(String[] args) {


        int  lineno =0;
        try {


            // command line parameter
            FileInputStream fstream = new FileInputStream("/Users/roshni/Desktop/Kaviar-160113-Public/vcfs/Kaviar-160113-Public-hg38.vcf");


            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine; //= "1\t13280035\trs765643056\tA\tG\t.\t.\tAF=0.0000379;AC=1;AN=26378";


            //output file
            File aminoAcid = new File("/Users/roshni/Desktop/KavierTable_Dataframe.csv");




            // if aminoAcid doesnt exists, then create it
            if (!aminoAcid.exists()) {
                aminoAcid.createNewFile();
            }




            FileWriter fw = new FileWriter(aminoAcid.getAbsoluteFile());
            BufferedWriter output = new BufferedWriter(fw);




            //Read File Line By Line
            output.write("chromosomeName,position,REF, ALT\n");
            while ((strLine = br.readLine()) != null) {
                lineno++;


                if(lineno>13) {
                    System.out.println(lineno);




                    // Parse the file to obtain the Chromosome Name and Positions
                    strLine=strLine.replace(",",":");
                    String[] tempStr = strLine.split("\t");
                    String CName = tempStr[0];
                    int CPos = Integer.valueOf(tempStr[1]);
                    String ref= tempStr[3];
                    String alt= tempStr[4];
                    output.write("chr" + CName+ "," + CPos + "," + ref + "," + alt + "\n");
                }


            }
            in.close();
            output.close();
        } catch (IOException e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());


        }


    }

}
