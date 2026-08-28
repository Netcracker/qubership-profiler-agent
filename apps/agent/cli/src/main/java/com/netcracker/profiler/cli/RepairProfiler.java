package com.netcracker.profiler.cli;

import com.netcracker.profiler.output.layout.FileAppender;
import com.netcracker.profiler.output.layout.Layout;
import com.netcracker.profiler.output.layout.SinglePageLayout;
import com.netcracker.profiler.output.layout.ZipLayout;
import com.netcracker.profiler.servlet.layout.SimpleLayout;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Exports subset of collected data.
 */
public class RepairProfiler implements Command {
    public static final Logger log = LoggerFactory.getLogger(RepairProfiler.class);
    public static final String DEFAULT_FILE_NAME = "esc_(repaired).zip";
    private String inputFileName;
    private String outputFileName;

    public int accept(Namespace args) {
        inputFileName = args.getString("input_file");
        if(inputFileName == null) {
            log.error("input_file must be filled");
            return -2;
        }
        File inputFile = new File(inputFileName);
        if(!inputFile.exists()) {
            log.error("input_file {} doesn't exist", inputFileName);
            return -2;
        }  else if(!inputFileName.endsWith(".zip")) {
            log.error("input_file mush have .zip extension");
            return -2;
        }
        log.info("Input file is {}", inputFileName);

        outputFileName = args.getString("output_file");
        if (DEFAULT_FILE_NAME.equals(outputFileName)) {
            String fileNameWithoutExt = inputFile.getName();
            int lastIndex = fileNameWithoutExt.lastIndexOf('.');
            if (lastIndex != -1) {
                fileNameWithoutExt = fileNameWithoutExt.substring(0, lastIndex);
            }
            outputFileName = new File(inputFile.getParentFile(), DEFAULT_FILE_NAME.replace("esc", fileNameWithoutExt)).getAbsolutePath();
        } else if(!outputFileName.endsWith(".zip")) {
            log.error("output_file mush have .zip extension");
            return -2;
        }
        log.info("Will save result to {}", new File(outputFileName).getAbsolutePath());

        try {
            return runRepair();
        } catch (IOException e) {
            log.error("Error while repairing profiler", e);
            return -1;
        }
    }

    private int runRepair() throws IOException {
        FileAppender appender = new ResourceFileAppender();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        appender.append("/tree.html", baos);
        String html = baos.toString(StandardCharsets.UTF_8.name());
        SinglePageLayout.Template template = SinglePageLayout.getTemplate(html, StandardCharsets.UTF_8);
        try (ZipInputStream is = new ZipInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(inputFileName))));
             OutputStream os = Files.newOutputStream(Paths.get(outputFileName));
             ZipLayout layout = new ZipLayout(new SimpleLayout(new BufferedOutputStream(os)));
             Layout out = new SinglePageLayout(layout, template)) {
            ZipEntry zipEntry;
            while ((zipEntry = is.getNextEntry()) != null) {
                if (zipEntry.getName().endsWith(".html")) {
                    processHtmlFile(zipEntry, is, out);
                } else {
                    out.putNextEntry(null, zipEntry.getName(), null);
                    copy(is, out.getOutputStream());
                }
            }
        }

        return 0;
    }

    private void processHtmlFile(ZipEntry zipEntry, ZipInputStream is, Layout out) throws IOException {
        out.putNextEntry(SinglePageLayout.JAVASCRIPT, zipEntry.getName(), null);
        BufferedReader inputReader = new BufferedReader(new InputStreamReader(is));
        BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(out.getOutputStream()));

        try {
            String line;
            do {
                line = inputReader.readLine();
                if(line == null) {
                    return;
                }
            } while (!line.startsWith("treedata(0"));

            do {
                outputWriter.write(line+'\n');
                line = inputReader.readLine();
                if(line == null) {
                    return;
                }
            } while (!line.endsWith("</script>"));
            outputWriter.write(line.substring(0, line.length()-9)+'\n');
        } finally {
            outputWriter.flush();
        }
    }

    private static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[8192];
        int length;
        while ((length = is.read(buf)) != -1) {
            os.write(buf, 0, length);
        }
    }

    private static class ResourceFileAppender implements FileAppender {
        @Override
        public void append(String fileName, OutputStream out) throws IOException {
            try (InputStream is = getClass().getResourceAsStream(fileName);) {
                copy(is, out);
            }
        }
    }
}
