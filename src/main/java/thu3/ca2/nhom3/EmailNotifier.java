package thu3.ca2.nhom3;

import javax.mail.*;
import javax.mail.internet.*;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class EmailNotifier {
    private static final String CONFIG_FILE = "config.properties";
    private static Properties properties;

    static {
        loadConfig();
    }

    private static void loadConfig() {
        properties = new Properties();
        try (FileReader reader = new FileReader(CONFIG_FILE)) {
            properties.load(reader);
            validateProperties();
        } catch (IOException e) {
            throw new RuntimeException("Không thể đọc file cấu hình: " + e.getMessage(), e);
        }
    }

    private static void validateProperties() {
        String[] requiredProps = {
                "email.smtp.server",
                "email.port",
                "email.username",
                "email.password",
                "email.to"
        };

        for (String prop : requiredProps) {
            if (!properties.containsKey(prop) || properties.getProperty(prop).trim().isEmpty()) {
                throw new RuntimeException("Thiếu thông tin cấu hình: " + prop);
            }
        }
    }

    public static void sendEmail(String subject, String messageText) {
        try {
            // Thiết lập các thuộc tính cho email
            Properties mailProps = new Properties();
            mailProps.put("mail.smtp.host", properties.getProperty("email.smtp.server"));
            mailProps.put("mail.smtp.port", properties.getProperty("email.port"));
            mailProps.put("mail.smtp.auth", "true");
            mailProps.put("mail.smtp.starttls.enable", "true");

            // Tạo phiên gửi email với xác thực
            Session session = Session.getInstance(mailProps, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(
                            properties.getProperty("email.username"),
                            properties.getProperty("email.password")
                    );
                }
            });

            // Tạo và cấu hình message
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(properties.getProperty("email.username")));
            message.setRecipients(
                    Message.RecipientType.TO,
                    InternetAddress.parse(properties.getProperty("email.to"))
            );
            message.setSubject(subject);
            message.setText(messageText);

            // Gửi email
            Transport.send(message);
            System.out.println("Email đã được gửi thành công!");

        } catch (MessagingException e) {
            throw new RuntimeException("Lỗi khi gửi email: " + e.getMessage(), e);
        }
    }

    // Phương thức kiểm tra xem email đã được cấu hình chưa
    public static boolean isConfigured() {
        return properties != null && !properties.isEmpty();
    }

    // Phương thức để reload config nếu cần
    public static void reloadConfig() {
        loadConfig();
    }
}

