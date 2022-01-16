package com.example.springbatch.config;

import com.example.springbatch.listener.HelloJobExecutionListener;
import com.example.springbatch.listener.HelloStepExecutionListener;
import com.example.springbatch.processor.InMemoryItemProcessor;
import com.example.springbatch.reader.InMemoryItemReader;
import com.example.springbatch.writer.InMemoryItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private HelloJobExecutionListener helloJobExecutionListener;

    @Autowired
    private HelloStepExecutionListener helloStepExecutionListener;

    @Autowired
    private InMemoryItemReader inMemoryItemReader;

    @Autowired
    private InMemoryItemProcessor inMemoryItemProcessor;

    @Autowired
    private InMemoryItemWriter inMemoryItemWriter;

    // lets create a step. Step does a certain work and jobs will use steps
    @Bean
    public Step step1() {
        return steps.get("step1")
                .listener(helloStepExecutionListener)
                .tasklet(helloBatchTasklet())
                .build();
    }

    @Bean
    public Step step2() {
        return steps.get("step2")
                .<Integer, Integer>chunk(3)
                .reader(inMemoryItemReader)
                .processor(inMemoryItemProcessor)
                .writer(inMemoryItemWriter)
                .build();
    }

    // it is task-based step, not chunk-based
    private Tasklet helloBatchTasklet() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello spring batch");
                return RepeatStatus.FINISHED;
            }
        };
    }

    // lets create a job
    @Bean
    public Job helloBatchJob() {
        return jobs.get("helloBatchJob")
                .listener(helloJobExecutionListener)
                .start(step1())
                .next(step2())
                .build();
    }

}
