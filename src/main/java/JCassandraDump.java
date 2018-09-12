import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import connection.CassandraConnector;
import java.io.*;
import java.util.StringTokenizer;


/**
 * Created by juliano.biscaia on 03/09/2018.
 */
public class JCassandraDump {

    private Session session;
    private String host = null;
    private Integer port = null;
    private Integer success = 0,
                    fail = 0;
    private String filePath = null;
    private String delimiter = ";";


    public static void main(String[] args) throws IOException {

        JCassandraDump jcd = new JCassandraDump();

        jcd.host = args[0];
        jcd.port = Integer.valueOf(args[1]);
        jcd.filePath = args[2];

        jcd.connectCassandra();
        jcd.loadDataFromFile();


    }

    private void connectCassandra(){

        CassandraConnector connection = null;

        try{
            connection = new CassandraConnector();
            connection.connect(this.host,this.port);
            this.session = connection.getSession();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }


    private void disconnectCassandra(){
        this.session.close();
    }


    private void loadDataFromFile() throws IOException {

        File file = null;
        String fileContent = null;
        String sql = null;
        Integer qtdeToken = 0;


        //Opening file
        file = new File(this.filePath);
        try {
            fileContent = Files.toString(file, Charsets.UTF_8);
        } catch (IOException e) {
            System.out.println("ERROR WHEN TRYING TO OPEN FILE: " + e.getMessage());
        }

        //
        StringTokenizer fileContentLine = new StringTokenizer(fileContent, delimiter);
        System.out.println("TOTAL SQL STATEMENTS: " + fileContentLine.countTokens());


        while (fileContentLine.hasMoreElements()) {
            qtdeToken++;

            try {
                sql = fileContentLine.nextToken();
                this.insertData(sql);
                this.success++;
            } catch (Exception e){
                System.out.println("ERRO WHEN EXECUTE STATEMENT: " + sql + "\nTOKEN: " + qtdeToken + "NEXT TOKEN:  " + fileContentLine.nextToken());
            }
        }

        System.out.println("SUCCESS: " + this.success);
        System.out.println("FAIL: " + this.fail);

        this.disconnectCassandra();


    }

    private void insertData(String sql){
        ResultSet rs = null;

        try {
            rs = this.session.execute(sql);
        } catch (Exception e){
            this.fail++;
            System.out.println(e.getMessage() + "SQL INSTRUCTIION: " + sql);
        }

    }


    private void exportData(String keyspace,String tableName){

    }


}
