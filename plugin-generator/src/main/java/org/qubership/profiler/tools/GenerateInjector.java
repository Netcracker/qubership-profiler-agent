package org.qubership.profiler.tools;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.objectweb.asm.*;
import org.objectweb.asm.util.ASMifier;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.*;

public class GenerateInjector {
    private final File root;
    private final File dst;
    private PrintWriter pw;

    int methodId;
    String className;
    long lastModified;

    private static long getLastModifiedTime(File file) {
        long fileLastModified = file.lastModified();

        if (fileLastModified == 0L) {
            fileLastModified = System.currentTimeMillis();
        }
        return fileLastModified;
    }

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
                System.out.println("ignoring field " + name);
                return null;
            }
            System.out.println("adding field " + name);
            return super.visitField(access, name, desc, signature, value);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if (name.contains("toCoreSubscriber")) {
                return super.visitMethod(access, name, desc, signature, exceptions);
            }

            if (!name.endsWith("$profiler") && !"<clinit>".equals(name)) {
                System.out.println("ignoring method " + name);
                return null;
            }
            if ("clinit$profiler".equals(name)) {
                System.out.println("converting method " + name + " to static initializer" );
                name = "<clinit>";
            }
            System.out.println("adding method " + name);
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

    public GenerateInjector(File root, File dst) {
        this.root = root;
        this.dst = dst;
    }

    public static void main(String[] args) {
        GenerateInjector task = new GenerateInjector(new File(args[0]), new File(args[1]));
        try {
            task.run();
        } catch (FileNotFoundException e) {
            System.err.println("Unable to process " + args[0] + ", " + args[1]);
            e.printStackTrace();
        }
    }

    private void run() throws FileNotFoundException {
        long lastRun = getLastModifiedTime(dst);
        final File parent = dst.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pw = new PrintWriter(baos, false);
        pw.print("package org.qubership.profiler.instrument.enhancement;\n");
        pw.print("import java.util.*;\n");
        pw.print("import org.objectweb.asm.*;\n");
        pw.print("import static org.objectweb.asm.Opcodes.*;\n");
        pw.print("import org.qubership.profiler.instrument.enhancement.ClassEnhancer;\n\n");
        String className = dst.getName();
        className = className.substring(0, className.indexOf('.'));
        this.className = className;
        pw.print("public class " + className + " extends HashMap<String, ClassEnhancer> {\n\n");
        walk(root);
        pw.print("}\n");
        pw.close();
        if (dst.exists() && lastRun > lastModified) {
            System.out.println("Skipped processing " + root + " -> " + dst + " since source files are not modified");
            return;
        }
        OutputStream out;
        try {
            out = new FileOutputStream(dst);
            out.write(baos.toByteArray());
            out.close();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write " + dst, e);
        }
    }

    private void walk(File root) {
        if (root.isDirectory()) {
            for (File file : root.listFiles()) {
                walk(file);
            }
            return;
        }

        if (!root.getName().endsWith(".class")) return;
        if (root.getName().startsWith("EnhancerPlugin_"))
            return;
        try {
            processClassFile(root);
        } catch (IOException e) {
            System.err.println("Unable to process file " + root.getAbsolutePath());
            e.printStackTrace();
        }
    }

    private void processClassFile(File root) throws IOException {
        System.out.println("Processing file " + root.getAbsolutePath());
        lastModified = Math.max(lastModified, getLastModifiedTime(root));
        FileInputStream is = new FileInputStream(root);
        ClassReader cr = new ClassReader(is);
        is.close();

        StringWriter sw = new StringWriter();
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
