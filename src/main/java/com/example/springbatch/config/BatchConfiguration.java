package com.example.springbatch.config;

import com.example.springbatch.listener.HelloJobExecutionListener;
import com.example.springbatch.listener.HelloStepExecutionListener;
import com.example.springbatch.model.Product;
import com.example.springbatch.processor.InMemoryItemProcessor;
import com.example.springbatch.reader.InMemoryItemReader;
import com.example.springbatch.writer.InMemoryItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

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
                .incrementer(new RunIdIncrementer()) //Creates job instance every time with new id
                .listener(helloJobExecutionListener)
                .start(step1())
                .next(step2())
                .build();
    }

    // lets configure a reader for csv file
    // you have to add this to step writer to make it work
    @Bean
    @StepScope
    public FlatFileItemReader flatFileItemReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource("input/product.csv"));

        reader.setLineMapper(
                new DefaultLineMapper<Product>() {
                    {
                        setLineTokenizer(new DelimitedLineTokenizer() {
                            {
                                setNames(new String[]{"productID", "productName", "ProductDesc", "price", "unit"});
                            }
                        });

                        setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                            {
                                setTargetType(Product.class);
                            }
                        });
                    }
                }
        );

        reader.setLinesToSkip(1); // we dont need header part of csv file
        return reader;
    }

    // lets configure a reader for reading xml file
    @Bean
    @StepScope
    public StaxEventItemReader xmlItemReader() {
        StaxEventItemReader reader = new StaxEventItemReader();
        reader.setResource(new FileSystemResource("input/product.csv"));

        // need to let reader to know which tags describe the domain object
        reader.setFragmentRootElementName("product");

        // tell reader how to parse XML and which domain object to be mapped
        reader.setUnmarshaller(new Jaxb2Marshaller() {
            {
                setClassesToBeBound(Product.class);
            }
        });

        return reader;
    }

}