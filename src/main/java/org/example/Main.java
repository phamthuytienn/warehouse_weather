package org.example;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Main {
    private static final String SENDER_EMAIL = "20130218@st.hcmuaf.edu.vn";
    private static final String SENDER_PASSWORD = "mymeslmspxoogvoo";
    private static final String SMTP_HOST = "smtp.gmail.com";
    private static final int SMTP_PORT = 465;

    private static final String DB_HOST = "localhost";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123";
    private static final String DB_NAME = "control";

    private static Connection connectionControl;
    private static Connection connectionDW;
    private static Connection connectionST;

    public static void main(String[] args) {
        System.out.println("Start");
        loadConfig();
    }

    private static void loadConfig() {
        Connection conn = connectToControl();
        try {
            Statement statement = conn.createStatement();
            String query = "SELECT * FROM data_file_configs left join data_files on data_file_configs.id = data_files.df_config_id where  DATE(data_files.created_at) <= CURDATE() and DATE(data_files.date_range_to) >= CURDATE() and data_files.status = 'SU' and data_files.is_inserted is not true and data_files.deleted_at is null";
            ResultSet result = statement.executeQuery(query);
            while (result.next()) {
                String filePath = result.getString("file_path") + result.getString("file_name");
                String destinationPath = result.getString("destination_path") + result.getString("file_name");
                String dwLocation = result.getString("dw_location");
                String stLocation = result.getString("st_location");
                connectionDW = createConnection(dwLocation);
                connectionST = createConnection(stLocation);
                checkMoveFile(filePath, destinationPath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnectionControl();
            closeConnectionDW();
        }
    }

    private static Connection connectToControl() {
        if (connectionControl == null) {
            connectionControl = createConnection(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME);
        }
        return connectionControl;
    }

    private static Connection createConnection(String host, String user, String password, String database) {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://" + host + "/" + database;
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            conn = DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private static void closeConnectionControl() {
        if (connectionControl != null) {
            try {
                connectionControl.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closeConnectionDW() {
        if (connectionDW != null) {
            try {
                connectionDW.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closeConnectionST() {
        if (connectionST != null) {
            try {
                connectionST.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void checkMoveFile(String filePath, String destinationPath) {
        File file = new File(filePath);
        if (file.exists()) {
            File destination = new File(destinationPath);
            if (!destination.exists()) {
                destination.mkdirs();
                try {
                    Files.copy(file.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    String fileName = file.getName();
                    if (Integer.parseInt(fileName.split("_")[2]) != 0) {
                        // Perform further operations
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // Handle destination path already exists
            }
        } else {
            // Update data_files table with file not found error
            updateDataFiles(filePath, "File không tồn tại.");
        }
    }

    private static void updateDataFiles(String filePath, String note) {
        Connection conn = connectToControl();
        try {
            String query = "UPDATE data_files SET note = ?, updated_at = ?, updated_by = ?, is_inserted = ?, deleted_at = ? WHERE file_path = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, note);
            statement.setString(2, getCurrentTime());
            statement.setString(3, "admin");
            statement.setBoolean(4, true);
            statement.setString(5, null);
            statement.setString(6, filePath);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnectionControl();
        }
    }

    private static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    private static void sendEmail(String recipientEmail, String subject, String message) {
        Properties props = new Properties();
        props.put("mail.smtp.host", SMTP_HOST);
        props.put("mail.smtp.port", SMTP_PORT);
        props.put("mail.smtp.ssl.enable", "true");

        Session session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(SENDER_EMAIL, SENDER_PASSWORD);
            }
        });

        try {
            MimeMessage mimeMessage = new MimeMessage(session);
            mimeMessage.setFrom(new InternetAddress(SENDER_EMAIL));
            mimeMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipientEmail));
            mimeMessage.setSubject(subject);
            mimeMessage.setText(message);

            Transport.send(mimeMessage);
            System.out.println("Email sent successfully.");
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }
}