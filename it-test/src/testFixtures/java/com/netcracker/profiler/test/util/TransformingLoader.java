package com.netcracker.profiler.test.util;

import com.netcracker.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;

/**
 * Class loader that will profile test classes
 */
public class TransformingLoader extends ClassLoader {
    public static final Logger log = LoggerFactory.getLogger(TransformingLoader.class);

    private final ClassFileTransformer transformer;

    public TransformingLoader(ClassFileTransformer transformer) {
        super();
        this.transformer = transformer;
    }

    @Override
    protected synchronized Class<?> loadClass(String className, boolean resolve) throws ClassNotFoundException {
        log.info("TransformingLoader.loadClass({}, {})", className, resolve);
        if (!className.startsWith("com.netcracker.profiler.test.pigs"))
            return super.loadClass(className, resolve);
        InputStream is = null;
        try {
            String jvmName = className.replace('.', '/');
            String fileName = jvmName + ".class";
            log.info("Loading class from {}", fileName);
            is = getResourceAsStream(fileName);
            byte[] bytes = IOHelper.readFully(is);
            byte[] newBytes = transformer.transform(this, jvmName, null, null, bytes);
            if (newBytes != null) bytes = newBytes;
            Class<?> aClass = defineClass(bytes, 0, bytes.length);
            if (resolve)
                resolveClass(aClass);
            return aClass;
        } catch (FileNotFoundException e) {
            log.error("Unable to load class " + className, e);
        } catch (IOException e) {
            log.error("Unable to load class " + className, e);
        } catch (IllegalClassFormatException e) {
            log.error("Unable to load class " + className, e);
        } finally {
            IOHelper.close(is);
        }

        return null;
    }
}
