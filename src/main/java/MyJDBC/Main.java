package MyJDBC;

import java.sql.*;
import java.util.Scanner;

public class Main {
    static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/apartmentsdb";//if there is a need one can add a set of params behind ?-mark
    static final String DB_USER = "root";
    static final String DB_PASSWORD = "qwerty";

    static Connection conn;

    public static void main(String[] args) {
        Scanner scn = new Scanner(System.in);
        try{
            conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            initDb();

            while(true){
                System.out.println("Enter price upper limit or 'q' to exit the program:");
                String upperLimit=scn.nextLine();
                if (upperLimit.equals("q")) break;
                System.out.println("Enter agency name or 'q' to exit the program:");
                String agencyName = scn.nextLine();
                if (agencyName.equals("q")) break;
                viewAppartments(upperLimit,agencyName);
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                scn.close();
                if (conn != null) conn.close();
            }catch(SQLException e){
                return;
            }
        }

    }

    private static void initDb() throws SQLException{
        deleteTables();
        createTables();
        fillTables();
        setRelations();
    }

    private static void createTables() throws SQLException{
        Statement st = conn.createStatement();
        try{
            st.execute("DROP TABLE IF EXISTS APARTMENTS");
            st.execute("CREATE TABLE APARTMENTS (APPID INT UNSIGNED NOT NULL AUTO_INCREMENT, " +
                    "CITY VARCHAR(20), DISTRICT VARCHAR(30), STREET VARCHAR(30), " +
                    "POSTALINDEX MEDIUMINT UNSIGNED, BUILDING VARCHAR(6), BLOCK VARCHAR(1), " +
                    "APPNO SMALLINT UNSIGNED, SQUARE DECIMAL(4,1), NUMOFROOM TINYINT UNSIGNED, " +
                    "FLOOR TINYINT UNSIGNED, CONSTRAINT PRIMARY KEY (APPID))");

            st.execute("DROP TABLE IF EXISTS CONTACTS");
            st.execute("CREATE TABLE CONTACTS (CONTACTID INT UNSIGNED NOT NULL AUTO_INCREMENT, " +
                    "CONTACTTYPE ENUM('TEL','EMAIL','SKYPE','WEB'), " +
                    "CONTACTVALUE VARCHAR(128), AGENTID INT UNSIGNED, CONSTRAINT PRIMARY KEY (CONTACTID))");

            st.execute("DROP TABLE IF EXISTS AGENTS");
            st.execute("CREATE TABLE AGENTS (AGENTID INT UNSIGNED NOT NULL AUTO_INCREMENT, " +
                    "AGENTNAME VARCHAR(64), ZKPO INT UNSIGNED, CONTACTREF INT UNSIGNED, CONSTRAINT PRIMARY KEY(AGENTID))");

            st.execute("DROP TABLE IF EXISTS OFFERS");
            st.execute("CREATE TABLE OFFERS(AGENTID INT UNSIGNED NOT NULL, APPID INT UNSIGNED NOT NULL, PRICE DECIMAL(11,2), CONSTRAINT PRIMARY KEY (AGENTID,APPID))");

        }finally {
          st.close();
        }
    }

    private static void deleteTables() throws SQLException{
        Statement st = conn.createStatement();

        try{
            st.execute("DROP TABLE IF EXISTS OFFERS");
            st.execute("DROP TABLE IF EXISTS CONTACTS");
            st.execute("DROP TABLE IF EXISTS AGENTS");
            st.execute("DROP TABLE IF EXISTS APARTMENTS");
       }finally {
            st.close();
        }

    }

    private static void setRelations() throws SQLException{
        Statement st = conn.createStatement();
        try{
            st.execute("ALTER TABLE CONTACTS ADD CONSTRAINT FOREIGN KEY (AGENTID) REFERENCES AGENTS (AGENTID) ON DELETE CASCADE");
            st.execute("ALTER TABLE OFFERS ADD CONSTRAINT FOREIGN KEY (AGENTID) REFERENCES AGENTS (AGENTID) ON UPDATE CASCADE");
            st.execute("ALTER TABLE OFFERS ADD CONSTRAINT FOREIGN KEY (APPID) REFERENCES APARTMENTS (APPID) ON UPDATE CASCADE");

        }finally{
            st.close();
        }
    }

    private static void fillTables() throws SQLException{
        fillApartmentTable();
        fillAgentsTable();
        fillOffersTable();
        fillContactsTable();
    }

    private static void removeDataFromTables() throws SQLException{
        Statement st = conn.createStatement();
        try{
            st.execute("DELETE FROM APARTMENTS");
            st.execute("DELETE FROM AGENTS");
            st.execute("DELETE FROM OFFERS");
            st.execute("DELETE FROM CONTACTS");
        }finally {
            st.close();
        }
    }


    private static void fillAgentsTable() throws SQLException{
        PreparedStatement ps = conn.prepareStatement("INSERT INTO AGENTS " +
                "(AGENTNAME, ZKPO,CONTACTREF)" +
                " VALUES(?, ?, ?)");
        try{
            ps.setString(1,"LUN.UA");
            ps.setString(2,"22060563");
            ps.setString(3,"1");
            ps.executeUpdate();

            ps.setString(1,"Park Lane");
            ps.setString(2,"22060567");
            ps.setString(3,"2");
            ps.executeUpdate();

            ps.setString(1,"Dom plus");
            ps.setString(2,"22060568");
            ps.setString(3,"0");
            ps.executeUpdate();

        }finally {
            ps.close();
        }
    }

    private static void fillContactsTable() throws SQLException{
        PreparedStatement ps = conn.prepareStatement("INSERT INTO CONTACTS " +
                "(CONTACTTYPE, CONTACTVALUE,AGENTID)" +
                " VALUES(?, ?, ?)");
        try{
            ps.setString(1,"web");
            ps.setString(2,"http://parklane.ua/");
            ps.setString(3,"2");
            ps.executeUpdate();

            ps.setString(1,"web");
            ps.setString(2,"http://www.lun.ua/");
            ps.setString(3,"1");
            ps.executeUpdate();

            ps.setString(1,"tel");
            ps.setString(2,"+380 (44) 392-22-22");
            ps.setString(3,"2");
            ps.executeUpdate();

            ps.setString(1,"tel");
            ps.setString(2,"+380 (44) 228-68-68");
            ps.setString(3,"3");
            ps.executeUpdate();

            ps.setString(1,"tel");
            ps.setString(2,"+380 (44) 284-86-79");
            ps.setString(3,"3");
            ps.executeUpdate();

            ps.setString(1,"web");
            ps.setString(2,"http://dom-plus.ua/");
            ps.setString(3,"3");
            ps.executeUpdate();

            ps.setString(1,"skype");
            ps.setString(2,"dom.plus.realty");
            ps.setString(3,"3");
            ps.executeUpdate();

            ps.setString(1,"email");
            ps.setString(2,"info@dom-plus.ua");
            ps.setString(3,"3");
            ps.executeUpdate();

        }finally {
            ps.close();
        }
    }

    private static void fillOffersTable() throws SQLException{
        PreparedStatement ps = conn.prepareStatement("INSERT INTO OFFERS " +
                "(AGENTID, APPID, PRICE)" +
                " VALUES(?, ?, ?)");
        try{
            ps.setString(1,"2");
            ps.setString(2,"1");
            ps.setString(3,"81000");
            ps.executeUpdate();

            ps.setString(1,"3");
            ps.setString(2,"1");
            ps.setString(3,"79000");
            ps.executeUpdate();

            ps.setString(1,"1");
            ps.setString(2,"2");
            ps.setString(3,"71000");
            ps.executeUpdate();

            ps.setString(1,"2");
            ps.setString(2,"2");
            ps.setString(3,"65000");
            ps.executeUpdate();

            ps.setString(1,"1");
            ps.setString(2,"3");
            ps.setString(3,"53000");
            ps.executeUpdate();

            ps.setString(1,"2");
            ps.setString(2,"3");
            ps.setString(3,"54999");
            ps.executeUpdate();

            ps.setString(1,"3");
            ps.setString(2,"3");
            ps.setString(3,"52000");
            ps.executeUpdate();

            ps.setString(1,"1");
            ps.setString(2,"4");
            ps.setString(3,"30000");
            ps.executeUpdate();

            ps.setString(1,"2");
            ps.setString(2,"4");
            ps.setString(3,"27999");
            ps.executeUpdate();

            ps.setString(1,"2");
            ps.setString(2,"5");
            ps.setString(3,"60000");
            ps.executeUpdate();

            ps.setString(1,"3");
            ps.setString(2,"5");
            ps.setString(3,"62000");
            ps.executeUpdate();
        }finally {
            ps.close();
        }
    }


    private static void fillApartmentTable() throws SQLException{
        PreparedStatement ps = conn.prepareStatement("INSERT INTO APARTMENTS " +
                "(CITY, DISTRICT,STREET,POSTALINDEX," +
                "BUILDING,BLOCK,APPNO,SQUARE,NUMOFROOM,FLOOR) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        try {
            ps.setString(1, "Kyiv");
            ps.setString(2, "Svytoshynsky");
            ps.setString(3, "Romena Rolana");
            ps.setString(4, "3194");
            ps.setString(5, "14");
            ps.setString(6, "B");
            ps.setString(7, "51");
            ps.setString(8, "76");
            ps.setString(9, "2");
            ps.setString(10, "9");
            ps.executeUpdate(); // for INSERT, UPDATE & DELETE

            ps.setString(1, "Chernigiv");
            ps.setString(2, "Central");
            ps.setString(3, "Khemlnitsky");
            ps.setString(4, "14010");
            ps.setString(5, "7/3");
            ps.setString(6, null);
            ps.setString(7, "91");
            ps.setString(8, "115");
            ps.setString(9, "4");
            ps.setString(10, "15");
            ps.executeUpdate(); // for INSERT, UPDATE & DELETE

            ps.setString(1, "Odesa");
            ps.setString(2, "Primorsky");
            ps.setString(3, "Seredyofontan");
            ps.setString(4, "65076");
            ps.setString(5, "9");
            ps.setString(6, "A");
            ps.setString(7, "39");
            ps.setString(8, "86");
            ps.setString(9, "3");
            ps.setString(10, "19");
            ps.executeUpdate(); // for INSERT, UPDATE & DELETE

            ps.setString(1, "Lviv");
            ps.setString(2, "Shevchenkosky");
            ps.setString(3, "John Lennon");
            ps.setString(4, "79059");
            ps.setString(5, "35");
            ps.setString(6, null);
            ps.setString(7, "5");
            ps.setString(8, "45");
            ps.setString(9, "1");
            ps.setString(10, "1");
            ps.executeUpdate(); // for INSERT, UPDATE & DELETE

            ps.setString(1, "Dnipro");
            ps.setString(2, "Zhovtenvy");
            ps.setString(3, "Robesp'era");
            ps.setString(4, "49021");
            ps.setString(5, "28");
            ps.setString(6, null);
            ps.setString(7, "19");
            ps.setString(8, "62");
            ps.setString(9, "2");
            ps.setString(10, "3");
            ps.executeUpdate(); // for INSERT, UPDATE & DELETE

        } finally {
            ps.close();
        }
    }

    private static void viewAppartments(String upperPrice, String agencyName) throws SQLException{
        StringBuilder sb = new StringBuilder("SELECT * FROM APARTMENTS T1 INNER JOIN OFFERS T2 INNER JOIN AGENTS T3 WHERE T1.APPID=T2.APPID AND T2.AGENTID=T3.AGENTID");

        StringBuilder sbWhereClause= new StringBuilder();

        if (upperPrice!=null&&!upperPrice.isEmpty()){
            sbWhereClause.append("PRICE<").append(upperPrice);
        }

        if(agencyName!=null&&!agencyName.isEmpty()){
            if (!sbWhereClause.toString().isEmpty()) sbWhereClause.append(" AND ");
            sbWhereClause.append(" AGENTNAME='").append(agencyName).append("'");
        }
        sb.append(" AND (").append(sbWhereClause.toString()).append(")");
        System.out.println(sb.toString());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        try{
            ResultSet rs = ps.executeQuery();
            try{
                ResultSetMetaData md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++)
                    System.out.print(md.getColumnName(i) + "\t\t");
                System.out.println();

                while (rs.next()) {
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        System.out.print(rs.getString(i) + "\t\t");
                    }
                    System.out.println();
                }
            }finally {
                rs.close();
            }
        }finally {
            ps.close();
        }
    }


}
