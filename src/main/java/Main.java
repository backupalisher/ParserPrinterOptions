import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/part4_new";
    private static final String DEFAULT_USERNAME = "zapchasty";
    private static final String DEFAULT_PASSWORD = "zapchasty_GfhjkzYtn321";
    private static final String path = "e:\\part4\\part4_spec\\brother\\";

    public static void main(String[] args) {
        String driver = ((args.length > 0) ? args[0] : DEFAULT_DRIVER);
        String url = ((args.length > 1) ? args[1] : DEFAULT_URL);
        String username = ((args.length > 2) ? args[2] : DEFAULT_USERNAME);
        String password = ((args.length > 3) ? args[3] : DEFAULT_PASSWORD);
        Connection connection = null;

        int brand_id = 1; // ID ПРОИЗВОДИТЕЛЯ
        int model_id = 0;
//        int part_code_id = 0;
        int detail_id = 0;
        int detail_option_id = 0;
        int parent_id = 0;

        int spr_detail_option_id = 0;

        String detailName = null;

        String detName = null; //Характеристики
        String detValue = null; //Значения

        Date date = new Date();
        System.out.println("Запуск " + date.toString());

        try {
            connection = createConnection(driver, url, username, password);

            File dir = new File(path); //path указывает на директорию
            File[] arrFiles = dir.listFiles();
            List<File> lst = Arrays.asList(arrFiles);

            for (File file : lst) {
                try {
                    String s = file.getName();
                    s = s.substring(0, s.length() - 4);
                    if (s.contains("_info")) {
                        detailName = s.substring(0, s.length() - 5);

                        String sqlModel = "INSERT INTO all_models (name,brand_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM all_models WHERE name=? AND brand_id= ?);";
                        List modelParametrs = Arrays.asList(detailName,brand_id,detailName,brand_id);
                        update(connection, sqlModel, modelParametrs);

                        String sqlModelId = "SELECT id FROM all_models WHERE name = ?;";
                        List modelIdParametrs = Arrays.asList(s);
                        model_id = query(connection, sqlModelId, modelIdParametrs);

                        //name, partcode_id, module_id, all_model_id
                        String sqlDetails = "INSERT INTO details (name, partcode_id, module_id, all_model_id) SELECT ?, NULL, NULL, NULL WHERE NOT EXISTS (SELECT 1 FROM details WHERE name=?);";
                        List detailsParametrs = Arrays.asList(detailName,detailName);
                        update(connection,sqlDetails,detailsParametrs);

                        String sqlDetailId = "SELECT id FROM details WHERE name = ?;";
                        List detailIdParametrs = Arrays.asList(detailName);
                        detail_id = query(connection, sqlDetailId, detailIdParametrs);

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

                                        String sqlDetailVal = null;
                                        List detailValParametrs = null;

                                        if (detName.equals("Caption")) {
                                            detValue = subStr[i].replaceAll(detailName, "");
                                            sqlDetailVal = "INSERT INTO detail_options (spr_detail_option_id, name) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            detailValParametrs = Arrays.asList(spr_detail_option_id, detValue, detValue);
                                            update(connection, sqlDetailVal, detailValParametrs);

                                            List getValName = Arrays.asList(detValue);
                                            parent_id = getId(connection,getValName);


                                            detail_option_id = parent_id;
                                        } else if (detName.equals("SubCaption")) {
                                            sqlDetailVal = "INSERT INTO detail_options (spr_detail_option_id, name, parent_id) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            detailValParametrs = Arrays.asList(spr_detail_option_id, detValue, parent_id, detValue);
                                            update(connection, sqlDetailVal, detailValParametrs);

                                            List getValName = Arrays.asList(detValue);
                                            parent_id = getId(connection,getValName);

                                            detail_option_id = parent_id;
                                        } else {
                                            String sqlOtherVal = "INSERT INTO detail_options (spr_detail_option_id, name, parent_id) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM detail_options WHERE name=?);";
                                            List otherVal = Arrays.asList(spr_detail_option_id, detValue, parent_id, detValue);
                                            update(connection,sqlOtherVal,otherVal);

                                            List getValName = Arrays.asList(detValue);
                                            detail_option_id = getId(connection,getValName);

                                        }

                                        String sqlLinkDetailsOptions = "INSERT INTO link_details_options (detail_id, detail_option_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM link_details_options WHERE detail_id=? AND detail_option_id=?);";
                                        List linkDetailsOptions = Arrays.asList(detail_id, detail_option_id, detail_id, detail_option_id);
                                        update(connection,sqlLinkDetailsOptions,linkDetailsOptions);

//                                        System.out.println(detail_id + ", " + detail_option_id);

                                    }
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception ex) {
            System.out.println("Connection failed...");
            ex.printStackTrace();
        }

        System.out.println("Завершение " + date.toString());

    }

    public static int getId(Connection connection,  List<Object> parameters) throws SQLException {
        int results = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement("SELECT id FROM detail_options WHERE name = ?;");
            int i = 0;

            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }

            rs = ps.executeQuery();
            while (rs.next()) {
                results = rs.getInt("id");
            }

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
