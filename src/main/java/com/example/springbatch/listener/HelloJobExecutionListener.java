package com.example.springbatch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HelloJobExecutionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before starting the job - Job Name: " +
                jobExecution.getJobInstance().getJobName());
        System.out.println("Before starting the job" +
                jobExecution.getExecutionContext());

        jobExecution.getExecutionContext().put("Name", "Turxan");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After starting the job - Job Execution Context : " +
                jobExecution.getJobInstance().getJobName());
    }

}
