package com.netcracker.profiler.tools;

import static com.netcracker.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.objectweb.asm.*;
import org.objectweb.asm.util.ASMifier;
import org.objectweb.asm.util.TraceClassVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class GenerateInjector {
    private static final Logger log = LoggerFactory.getLogger(GenerateInjector.class);

    private final List<Path> inputClassDirectories;
    private final Path outputFile;
    private PrintWriter pw;

    int methodId;
    String className;

    static class FilterProfiledEntities extends ClassVisitor {

        private String className;

        public FilterProfiledEntities(ClassVisitor cv) {
            super(OPCODES_VERSION, cv);
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            className = name;
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            if (!name.endsWith("$profiler")) {
                log.debug("ignoring field {}", name);
                return null;
            }
            log.debug("adding field {}", name);
            return super.visitField(access, name, desc, signature, value);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if (name.contains("toCoreSubscriber")) {
                return super.visitMethod(access, name, desc, signature, exceptions);
            }

            if (!name.endsWith("$profiler") && !"<clinit>".equals(name)) {
                log.debug("ignoring method {}", name);
                return null;
            }
            if ("clinit$profiler".equals(name)) {
                log.debug("converting method {} to static initializer", name);
                name = "<clinit>";
            }
            log.debug("adding method {}", name);
            return super.visitMethod(access, name, desc, signature, exceptions);
        }

        @Override
        public void visitSource(String source, String debug) {
        }

        @Override
        public void visitOuterClass(String owner, String name, String desc) {
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {
        }

        @Override
        public void visitEnd() {
        }
    }

    public GenerateInjector(List<Path> inputClassDirectories, Path outputFile) {
        this.inputClassDirectories = inputClassDirectories;
        this.outputFile = outputFile;
    }

    public void run() throws IOException {
        final Path parent = outputFile.getParent();
        if (Files.isRegularFile(parent)) {
            throw new IllegalArgumentException("Can't create class " + outputFile.toAbsolutePath() + " since " + parent.isAbsolute() + " is a regular file");
        }
        if (!Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pw = new PrintWriter(baos, false);
        pw.print("package com.netcracker.profiler.instrument.enhancement;\n");
        pw.print("import java.util.*;\n");
        pw.print("import org.objectweb.asm.*;\n");
        pw.print("import static org.objectweb.asm.Opcodes.*;\n");
        pw.print("import com.netcracker.profiler.instrument.enhancement.ClassEnhancer;\n\n");
        String className = outputFile.getFileName().toString();
        className = className.substring(0, className.lastIndexOf(".java"));
        this.className = className;
        pw.print("public class " + className + " extends HashMap<String, ClassEnhancer> {\n\n");
        for (Path directory : inputClassDirectories) {
            if (!Files.exists(directory)) {
                continue;
            }
            log.info("Processing directory {}", directory.toAbsolutePath());
            try (Stream<Path> files = Files.walk(directory)
                    .filter(Files::isRegularFile)
                    .filter(f -> {
                        String fileName = f.getFileName().toString();
                        return fileName.endsWith(".class") && !fileName.startsWith("EnhancerPlugin_");
                    })) {
                files.forEachOrdered(f -> {
                    try {
                        processClassFile(f);
                    } catch (IOException e) {
                        throw new IllegalStateException("Unable to process file " + f.toAbsolutePath(), e);
                    }
                });
            }
        }
        pw.print("}\n");
        pw.close();
        OutputStream out;
        try {
            out = Files.newOutputStream(outputFile);
            out.write(baos.toByteArray());
            out.close();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write " + outputFile, e);
        }
    }

    private void processClassFile(Path root) throws IOException {
        log.debug("Processing file {}", root.toAbsolutePath());
        ClassReader cr;
        try (InputStream is = Files.newInputStream(root);) {
            cr = new ClassReader(is);
        }

        ASMifier asmifier = new ASMifier();
        TraceClassVisitor printer = new TraceClassVisitor(null, asmifier, null);
        FilterProfiledEntities cv = new FilterProfiledEntities(printer);

        cr.accept(cv, ClassReader.EXPAND_FRAMES);

        if (asmifier.getText().isEmpty()) return;
        pw.print("  {\n");
        pw.print("    put(\"" + cv.className + "\", new ReflectedEnhancerBridge(" + className + ".class, \"e" + methodId + "\"));\n");
        pw.print("  }\n\n");
        pw.print("  public static void e" + methodId + "(ClassVisitor classWriter) {\n");
        pw.print("    FieldVisitor fieldVisitor;\n");
        pw.print("    MethodVisitor methodVisitor;\n");
        asmifier.print(pw);
        pw.print("  }\n");
        methodId++;
    }
}
