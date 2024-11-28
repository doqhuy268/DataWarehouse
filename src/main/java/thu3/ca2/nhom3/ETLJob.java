package thu3.ca2.nhom3;

import org.quartz.*;

public class ETLJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            System.out.println("ETL Job started...");
            DataWarehouseETL.main(null); // Gọi quy trình ETL
            EmailNotifier.sendEmail("ETL Job Completed", "ETL Job executed successfully.");
            System.out.println("ETL Job completed successfully.");
        } catch (Exception e) {
            System.err.println("ETL Job failed: " + e.getMessage());
            EmailNotifier.sendEmail("ETL Job Failed", "ETL Job failed with error: " + e.getMessage());

            // Tự động retry nếu gặp lỗi
            try {
                RetryHandler.scheduleRetry(context.getJobDetail());
            } catch (SchedulerException schedulerException) {
                schedulerException.printStackTrace();
            }

            throw new JobExecutionException("ETL Job execution failed", e);
        }
    }
}
