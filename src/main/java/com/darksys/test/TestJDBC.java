package com.darksys.test;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by Ben on 2016/9/2.
 */
public class TestJDBC {

    public static void main(String[] args){
        Logger logger = Logger.getLogger(TestJDBC.class.getName());

        System.out.println("Usage: testjdbc.bat [[QA | PROD] [repeat] [interval_in_second]]\nBy default the server is QA, repeat is 2 and interval is 30\n");

        boolean Prod = args != null && args.length > 0 && "PROD".equals(args[0]);
        final String SERVER =  Prod ? "prod_rds" : "qa_rds";
        final String USERNAME = Prod ? "prod_user" :"qa_user";
        final String PASSWORD = Prod ? "prod_passwd" : "qa_passwd";
        final int REPEAT = args != null && args.length > 1 ? Integer.valueOf(args[1]) : 2;
        final int INTERVAL_SEC = args != null && args.length > 2 ? Integer.valueOf(args[2]) : 30;
        final String QUERY = "select * from EQUIPMENT_MASTER_BULKUPLOAD";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        logger.info(String.format("Start, repeat is %d, interval is %d", REPEAT, INTERVAL_SEC));
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver") ;
            String url = String.format("jdbc:sqlserver://%s:1433;DatabaseName=dbname", SERVER);
            logger.info("Connecting to " + url);
            conn = DriverManager.getConnection(url, USERNAME, PASSWORD);
            stmt = conn.createStatement();
            for ( int i = 0; i < REPEAT; i ++ ) {
                try {
                    Thread.sleep(INTERVAL_SEC * 1000);
                    logger.info("Run query: " + QUERY);
                    rs = stmt.executeQuery(QUERY);
                    while (rs.next()) {
                        logger.info("\tEQUNR: " + rs.getString("EQUNR"));
                    }
                }catch(SQLException e) {
                    logger.error("SQL Error", e);
                }catch(Exception e) {
                    logger.error("Exception", e);
                }
            }
        }catch(ClassNotFoundException e) {
            logger.error("Driver Not Found", e);
        }catch(SQLException e) {
            logger.error("SQL Error", e);
        }finally{
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            }catch(Exception ignore){
            }
        }
    }
}
/* --sp
use ...

declare @i int
set @i = 0  
while @i < 120
begin
	SELECT * FROM EQUIPMENT_MASTER_BULKUPLOAD
	OUTPUT select convert(varchar(20), @i)
	Waitfor Delay '00:00:30'
	set @i = @i + 1
end

go
*/