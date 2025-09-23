package com.netcracker.profiler.io;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@Profile("filestorage")
public class ParamReaderFactoryFile extends ParamReaderFactory {

    public ParamReaderFactoryFile(ApplicationContext context) {
        super(context);
    }

    @Override
    public ParamReader getInstance(String rootReference) {
        File root = rootReference == null ? null : new File(rootReference);
        try {
            return new ParamReaderFromMemory_03(root);
        } catch (Throwable t) {
        }
        try {
            return new ParamReaderFromMemory_02(root);
        } catch (Throwable t) {
        }
        try {
            return new ParamReaderFromMemory_01(root);
        } catch (Throwable t) {
        }
        try {
            return new ParamReaderFromMemory(root);
        } catch (Throwable t) {
        }
        return new ParamReaderFile(root);
    }
}
