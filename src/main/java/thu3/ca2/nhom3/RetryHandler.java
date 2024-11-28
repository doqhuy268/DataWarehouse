package thu3.ca2.nhom3;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class RetryHandler {

    public static void scheduleRetry(JobDetail jobDetail) throws SchedulerException {
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        Trigger retryTrigger = TriggerBuilder.newTrigger()
                .withIdentity(jobDetail.getKey().getName() + "_retry", jobDetail.getKey().getGroup())
                .startAt(DateBuilder.futureDate(120, DateBuilder.IntervalUnit.SECOND)) // Retry sau 2 phút
                .build();

        scheduler.scheduleJob(jobDetail, retryTrigger);
        System.out.println("Retry scheduled for job: " + jobDetail.getKey().getName());
    }
}

