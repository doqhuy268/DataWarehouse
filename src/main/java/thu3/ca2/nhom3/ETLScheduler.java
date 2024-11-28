package thu3.ca2.nhom3;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ETLScheduler {

    public static void main(String[] args) {
        Scheduler scheduler = null;

        try {
            // Đọc file config.properties
            Properties properties = new Properties();
            FileInputStream fis = new FileInputStream("config.properties");
            properties.load(fis);

            // Lấy cron expression từ file config
            String cronExpression = properties.getProperty("scheduler.cron.expression");

            // Kiểm tra tính hợp lệ của cron expression
            if (cronExpression == null || cronExpression.isEmpty()) {
                throw new IllegalArgumentException("Cron expression is not defined in config.properties");
            }

            // Định nghĩa Job
            JobDetail job = JobBuilder.newJob(ETLJob.class)
                    .withIdentity("ETLJob", "ETLGroup")
                    .build();

            // Định nghĩa Trigger dựa vào cron expression
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("ETLTrigger", "ETLGroup")
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                    .build();

            // Khởi tạo Scheduler
            scheduler = StdSchedulerFactory.getDefaultScheduler();

            // Bắt đầu Scheduler
            scheduler.start();

            // Lên lịch Job
            scheduler.scheduleJob(job, trigger);

            System.out.println("ETL Scheduler started with cron: " + cronExpression);

        } catch (IOException e) {
            System.err.println("Error reading config.properties file: " + e.getMessage());
        } catch (SchedulerException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid configuration: " + e.getMessage());
        } finally {
            if (scheduler != null) {
                Scheduler finalScheduler = scheduler;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        if (!finalScheduler.isShutdown()) {
                            finalScheduler.shutdown();
                        }
                    } catch (SchedulerException e) {
                        e.printStackTrace();
                    }
                }));
            }
        }
    }
}
