package com.example.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HelloStepExecutionListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Before step execution: " +
                stepExecution.getJobExecution().getExecutionContext());
        System.out.println("Inside Step - job parameters: " +
                stepExecution.getJobExecution().getJobParameters());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("After step execution: " +
                stepExecution.getJobExecution().getExecutionContext());
        return null;
    }

}
