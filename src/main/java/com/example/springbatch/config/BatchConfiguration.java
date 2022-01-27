package com.example.springbatch.config;

import com.example.springbatch.listener.HelloJobExecutionListener;
import com.example.springbatch.listener.HelloStepExecutionListener;
import com.example.springbatch.listener.ProductSkipListener;
import com.example.springbatch.model.Product;
import com.example.springbatch.processor.InMemoryItemProcessor;
import com.example.springbatch.reader.InMemoryItemReader;
import com.example.springbatch.service.adapter.ProductServiceAdapter;
import com.example.springbatch.writer.InMemoryItemWriter;
import com.thoughtworks.xstream.XStream;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.util.HashMap;

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

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ProductServiceAdapter productServiceAdapter;

    // lets create a step. Step does a certain work and jobs will use steps
    @Bean
    public Step step1() {
        return steps.get("step1")
                .listener(helloStepExecutionListener)
                .tasklet(helloBatchTasklet())
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

    @Bean
    public Step step2() {
        return steps.get("step2")
                .<Integer, Integer>chunk(3)
                .reader(inMemoryItemReader)
                .processor(inMemoryItemProcessor)
                .writer(inMemoryItemWriter)
                .build();
    }

    @Bean
    public Step step3() {
        return steps.get("step3")
                .<Integer, Integer>chunk(3)
                .reader(flatFileItemReader())
                .processor(inMemoryItemProcessor)
                .writer(flatFileItemWriter(null))
                .faultTolerant()
                .skip(FlatFileParseException.class) //skip this exception
                .skipLimit(3) //batch will skip only 3 incorrect record
                .skipPolicy(new AlwaysSkipItemSkipPolicy()) //will skip all exceptions
                .listener(new ProductSkipListener()) //skipped records will be written to file
                .build();
    }

    // lets create a job
    @Bean
    public Job helloBatchJob() {
        return jobs.get("helloBatchJob")
                .incrementer(new RunIdIncrementer()) //Creates job instance every time with new id
                .listener(helloJobExecutionListener)
                .start(step1())
                .next(step2())
                .next(step3())
                .build();
    }

    /**
     * Reading and writing flat files
     */

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

    @Bean
    public FlatFileItemWriter flatFileItemWriter(
            @Value("#{jobParameters[fileOutput]}") FileSystemResource outputFile
    ) {
        FlatFileItemWriter writer = new FlatFileItemWriter();
            writer.setResource(outputFile);
        writer.setLineAggregator(new DelimitedLineAggregator(){
            {
                setDelimiter("|");
                setFieldExtractor(new BeanWrapperFieldExtractor(){
                    {
                        setNames(new String[]{"productID", "productName", "ProductDesc", "price", "unit"});
                    }
                });
            }
        });

        return writer;
    }

    /**
     * Reading and writing xml files
     */

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

    @Bean
    @StepScope
    public StaxEventItemWriter xmlItemWriter(
            @Value("#{jobParameters[fileOutput]}") FileSystemResource outputFile
    ) {
        XStreamMarshaller marshaller = new XStreamMarshaller();

        HashMap<String, Class> aliases = new HashMap<>();
        aliases.put("product", Product.class);
        marshaller.setAliases(aliases);
        marshaller.setAutodetectAnnotations(true);

        StaxEventItemWriter writer = new StaxEventItemWriter();
        writer.setResource(outputFile);
        writer.setMarshaller(marshaller);
        writer.setRootTagName("Products");

        return writer;
    }

    // lets configure a reader for reading from DB
    @Bean
    public JdbcCursorItemReader jdbcCursorItemReader() {
        JdbcCursorItemReader reader = new JdbcCursorItemReader();
        reader.setDataSource(dataSource);
        reader.setSql("select product_id, prod_name, prod_desc, unit, price from products");
        reader.setRowMapper(new BeanPropertyRowMapper(Product.class) {
            {
                setMappedClass(Product.class);
            }
        });

        return reader;
    }

    // lets configure a reader for reading json file
    @Bean
    @StepScope
    public JsonItemReader jsonItemReader(@Value("#{jobParameters['fileInput']}")
                                                 FileSystemResource inputFile) {
        var reader = new JsonItemReader(inputFile, new JacksonJsonObjectReader(Product.class));
        return reader;
    }

    // lets configure a reader for reading from rest web service
    @Bean
    public ItemReaderAdapter serviceItemReader() {
        ItemReaderAdapter reader = new ItemReaderAdapter();
        reader.setTargetObject(productServiceAdapter);
        reader.setTargetMethod("nextProduct");

        return reader;
    }

}