package lk.icoder.reconbatch.controller;

import lk.icoder.reconbatch.config.BatchConfiguration;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Project recon-batch
 * @Author DILAN on 9/25/2021
 */
@RestController
@RequestMapping("/manage-report")
public class ReportController {

    private JobLauncher jobLauncher;
    private BatchConfiguration batchConfiguration;

    public ReportController(JobLauncher jobLauncher, BatchConfiguration batchConfiguration) {
        this.jobLauncher = jobLauncher;
        this.batchConfiguration = batchConfiguration;
    }

    @GetMapping("/start")
    public void startAdjustmentJob() {
        try {
            jobLauncher.run(
                    batchConfiguration.reconciliationWritingJob(),
                    new JobParametersBuilder().addLong("recon", System.nanoTime()).toJobParameters()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
