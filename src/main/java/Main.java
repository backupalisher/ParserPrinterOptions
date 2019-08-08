import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
//    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/zapchasty";
//    private static final String DEFAULT_URL = "jdbc:postgresql://116.203.55.188:5432/zapchasty";
    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/part4_new";
    private static final String DEFAULT_USERNAME = "zapchasty";
    private static final String DEFAULT_PASSWORD = "zapchasty_GfhjkzYtn321";
//    private static final String path = "D:\\part4\\part4_spec\\";
    private static final String path = "E:\\part4\\part4_spec\\";
    private static final String[] brand_list = {"brother","canon","epson","hp","konica minolta","kyocera","lexmark","oki","panasonic","ricoh","riso","samsung","sharp","toshiba","xerox"};

    public static void main(String[] args) {
        String driver = ((args.length > 0) ? args[0] : DEFAULT_DRIVER);
        String url = ((args.length > 1) ? args[1] : DEFAULT_URL);
        String username = ((args.length > 2) ? args[2] : DEFAULT_USERNAME);
        String password = ((args.length > 3) ? args[3] : DEFAULT_PASSWORD);
        Connection connection;

        int brand_id = 0; // ID ПРОИЗВОДИТЕЛЯ
        int detail_id;
        int detail_option_id = 0;
        int parent_id = 0;
        int model_id;

        int spr_detail_option_id = 0;

        int fIndex;
        String detailName;

        String path_list = null;

        String detName = null; //Характеристики
        String detValue; //Значения

        Date startTime = new Date();
        System.out.println("Started " + startTime.toString());



        for (int j = 0; j < brand_list.length; j++) {
            path_list = path + brand_list[j];
            brand_id = j+1;
            System.out.println(brand_id+ ": " + path_list);
            try {
                connection = createConnection(driver, url, username, password);

                File dir = new File(path_list); //path указывает на директорию
                File[] arrFiles = dir.listFiles();
                List<File> lst = Arrays.asList(arrFiles);

                System.out.println(lst.size()/2);
                fIndex = 0;

                for (File file : lst) {
                    String s = file.getName();
//                    System.out.println("file name: " + s);
                    s = s.substring(0, s.length() - 4);
                    if (s.contains("_info")) {
                        detailName = s.replaceAll("_info","").trim();
                        detailName = detailName.replaceAll("[(-)]+","").trim();
                        detailName = detailName.replaceAll("[+]", "plus").trim();
                        detailName = detailName.replaceAll("[/]", "_").trim();
                        System.out.println(++fIndex + ": " + detailName);
                                                                        //("(.*)-", "$1")
                        String nDetailName = detailName.replaceAll("(-)(?!.*-)", "");

                        String editSqlModelId = "SELECT id FROM all_models WHERE name = ?;";
                        List editModelIdParametrs = Arrays.asList(nDetailName);
                        model_id = query(connection, editSqlModelId, editModelIdParametrs);

                        if (model_id == 0) {
                            String sqlModelId = "SELECT id FROM all_models WHERE name = ?;";
                            List modelIdParametrs = Arrays.asList(detailName);
                            model_id = query(connection, sqlModelId, modelIdParametrs);
                        }

                        if (model_id == 0) {
                            String sqlModel = "INSERT INTO all_models (name,brand_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM all_models WHERE name = ? AND brand_id = ?);";
                            List modelParametrs = Arrays.asList(detailName, brand_id, detailName, brand_id);
                            update(connection, sqlModel, modelParametrs);

                            String sqlModelId = "SELECT id FROM all_models WHERE name = ?;";
                            List modelIdParametrs = Arrays.asList(detailName);
                            model_id = query(connection, sqlModelId, modelIdParametrs);
                        }

                        String sqlDetailId = "SELECT id FROM details WHERE name = ?;";
                        List detailIdParametrs = Arrays.asList(detailName);
                        detail_id = query(connection, sqlDetailId, detailIdParametrs);

                        if (detail_id == 0) {
                            String sqlDetails = "INSERT INTO details (name, partcode_id, module_id, all_model_id) SELECT ?, NULL, NULL, ? WHERE NOT EXISTS (SELECT 1 FROM details WHERE name = ?);";
                            List detailsParametrs = Arrays.asList(detailName, model_id, detailName);
                            update(connection, sqlDetails, detailsParametrs);

                            sqlDetailId = "SELECT id FROM details WHERE name = ?;";
                            detailIdParametrs = Arrays.asList(detailName);
                            detail_id = query(connection, sqlDetailId, detailIdParametrs);
                        }

                        try {
                            FileInputStream fileInputStream = new FileInputStream(file.getAbsolutePath());
                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                            String strLine;
                            String[] subStr;

                            while ((strLine = bufferedReader.readLine()) != null) {
                                subStr = strLine.split(";");
                                for (int i = 0; i < subStr.length; i++) {
                                    if (i == 0) {
                                        detName = subStr[i];
                                        String sqlSprDetails = "INSERT INTO spr_detail_options (name) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM spr_detail_options WHERE name=?);";
                                        List sprDetailParametrs = Arrays.asList(detName, detName);
                                        update(connection, sqlSprDetails, sprDetailParametrs);

                                        String sqlSprDetailsId = "SELECT id FROM spr_detail_options WHERE name = ?;";
                                        List sprDetailIdParametrs = Arrays.asList(detName);
                                        spr_detail_option_id = query(connection, sqlSprDetailsId, sprDetailIdParametrs);

                                    }
                                    if (i == 1) {
                                        detValue = subStr[i];

                                        String sqlDetailVal;
                                        List detailValParametrs;

                                        if (detName.equals("Caption")) {
                                            detValue = subStr[i].replaceAll("[(-)]+","").trim();
                                            detValue = detValue.replaceAll("[+]", "plus");
                                            detValue = detValue.replaceAll("[/]", "_");
                                            detValue = detValue.replaceAll(detailName,"").trim();

                                            sqlDetailVal = "INSERT INTO detail_options (spr_detail_option_id, name) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            detailValParametrs = Arrays.asList(spr_detail_option_id, detValue, detValue);
                                            update(connection, sqlDetailVal, detailValParametrs);

                                            List getValName = Arrays.asList(detValue);
                                            parent_id = getId(connection,getValName);
                                        } else if (detName.equals("SubCaption")) {
                                            sqlDetailVal = "INSERT INTO detail_options (spr_detail_option_id, name, parent_id) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            detailValParametrs = Arrays.asList(spr_detail_option_id, detValue, parent_id, detValue);
                                            update(connection, sqlDetailVal, detailValParametrs);

                                            List getValName = Arrays.asList(detValue);
                                            parent_id = getId(connection,getValName);
                                        } else {
                                            String sqlOtherVal = "INSERT INTO detail_options (spr_detail_option_id, name, parent_id) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            List otherVal = Arrays.asList(spr_detail_option_id, detValue, parent_id, detValue);
                                            update(connection,sqlOtherVal,otherVal);

                                            List getValName = Arrays.asList(detValue);
                                            detail_option_id = getId(connection,getValName);
                                        }
                                        String sqlLinkDetailsOptions = "INSERT INTO link_details_options (detail_id, detail_option_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM link_details_options WHERE detail_id=? AND detail_option_id=?);";
                                        List linkDetailsOptions2 = Arrays.asList(detail_id, parent_id, detail_id, parent_id);
                                        update(connection,sqlLinkDetailsOptions,linkDetailsOptions2);
                                        
                                        List linkDetailsOptions = Arrays.asList(detail_id, detail_option_id, detail_id, detail_option_id);
                                        update(connection,sqlLinkDetailsOptions,linkDetailsOptions);
                                    }
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }

            } catch (Exception ex) {
                System.out.println("Connection failed...");
                ex.printStackTrace();
            }
        }

        Date endTime = new Date();
        System.out.println("Finished " + endTime.toString());
        System.out.println("Started: " + startTime + ", Finished: " + endTime.toString());

//        String s = "aaa i-ccccc BB-4564dn";
////        String t = s.replaceAll("(.*)-", "$1");
//        String t = s.replaceAll("(-)(?!.*-)", "");
//        System.out.println(s);
//        System.out.println(t);
    }

    public static String reg(String text){
        text.replaceAll("(-)(?=[^-]*$)", "");
        return text;
    }

    public static int getId(Connection connection,  List<Object> parameters) throws SQLException {
        int results = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
//            ps = connection.prepareStatement("SELECT id FROM detail_options WHERE name = ?;");
            ps = connection.prepareStatement("SELECT id FROM detail_options WHERE name = ?;");
            int i = 0;

            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                results = rs.getInt("id");
            }

//            System.out.println(ps.toString()+ ": "+results);

        } finally {
            close(rs);
            close(ps);
        }
        return results;
    }


    public static int query(Connection connection, String sql, List<Object> parameters) throws SQLException {
        int results = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;

            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                results = rs.getInt("id");
            }

//            System.out.println(ps.toString()+": "+results);
        } finally {
            close(rs);
            close(ps);
        }
        return results;
    }

    public static int update(Connection connection, String sql, List<Object> parameters) throws SQLException {
        int numRowsUpdated = 0;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;
            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }


            numRowsUpdated = ps.executeUpdate();

//            System.out.println(ps.toString()+": "+numRowsUpdated);
        } finally {
            close(ps);
        }
        return numRowsUpdated;
    }

    public static Connection createConnection(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        if ((username == null) || (password == null) || (username.trim().length() == 0) || (password.trim().length() == 0)) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(url, username, password);
        }
    }

    public static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> map(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            if (rs != null) {
                ResultSetMetaData meta = rs.getMetaData();
                int numColumns = meta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<String, Object>();

                    for (int i = 1; i <= numColumns; ++i) {
                        String name = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(name, value);
                    }

                    results.add(row);
                }
            }
        } finally {
            close(rs);
        }

        return results;
    }
}