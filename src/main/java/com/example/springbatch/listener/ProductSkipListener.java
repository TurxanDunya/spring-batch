package com.example.springbatch.listener;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProductSkipListener {


    @OnSkipInRead
    public void onSkipRead(Throwable t) {
        String readErrorFileName = "error/read_skipped";

        if (t instanceof FlatFileParseException) {
            FlatFileParseException exception = (FlatFileParseException) t;
            onSkip(exception.getInput(), readErrorFileName);
        }
    }

    @OnSkipInProcess
    public void onSkipInProcess(Object item, Throwable t) {
        String processErrorFileName = "error/process_skipped";

        if (t instanceof RuntimeException) {
            onSkip(item, processErrorFileName);
        }
    }

    /**
     * Instead of writing to file, we can write to database or something else.
     */
    public void onSkip(Object object, String fileName) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(fileName, true)) {
            fileOutputStream.write(object.toString().getBytes());
            fileOutputStream.write("\r\n".getBytes());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}