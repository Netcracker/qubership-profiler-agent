package org.qubership.profiler.test.dump;

import org.qubership.profiler.Dumper;
import org.qubership.profiler.agent.*;
import org.qubership.profiler.configuration.NetworkExportParamsImpl;
import org.qubership.profiler.dump.DumperThread;
import org.qubership.profiler.metrics.MetricsPluginImpl;
import org.qubership.profiler.transfer.DataSender;

import mockit.Mocked;
import org.testng.Assert;
import org.testng.annotations.*;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

public class DumperTest {
    @Mocked()
    final Profiler unused = null;

    String dumpFolderName;
    File root;

    private void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files)
                    deleteDirectory(file);
            }
        }
        Assert.assertTrue(dir.delete(), "Unable to delete file " + dir.getAbsolutePath());
    }

    @Test(groups = "checkGeneratedFiles", dataProvider = "mustHaveFiles")
    public void checkFile(String path, String description) {
        File file = new File(root, path);
        Assert.assertTrue(file.exists(), description + " file not found: " + file.getAbsolutePath());
    }

    @BeforeSuite
    public void writeSomeData()  throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, SAXException, ParserConfigurationException, InvocationTargetException, InterruptedException {
        BlockingQueue<LocalBuffer> dirtyBuffers = new ArrayBlockingQueue<LocalBuffer>(100);
        BlockingQueue<LocalBuffer> emptyBuffers = new ArrayBlockingQueue<LocalBuffer>(100);
        ArrayList<LocalBuffer> buffers = new ArrayList<LocalBuffer>();
        String dumpFolderName = "test-dump";
        this.dumpFolderName = dumpFolderName;

        deleteDirectory(new File(dumpFolderName));

        Configuration_05 config = createConfiguration();
        final LocalState s = new LocalState();
        final LocalBuffer buffer = new LocalBuffer();
        buffer.state = s;
        s.buffer = buffer;

        s.enter(ProfilerData.resolveTag("void action() () []") | DumperConstants.DATA_ENTER_RECORD);
        s.event("abcd", ProfilerData.resolveTag("void action() () []") | DumperConstants.DATA_TAG_RECORD);
        s.exit();
        dirtyBuffers.put(buffer);

        ConcurrentHashMap<Thread, LocalState> threads = new ConcurrentHashMap<Thread, LocalState>();
        threads.put(s.thread, s);
        Dumper dumper = new Dumper(dirtyBuffers, emptyBuffers, threads, dumpFolderName, new MetricsPluginImpl());
        dumper.configure(config.getParametersInfo(), config.getLogMaxAge(), config.getLogMaxSize(),config.getMetricsConfig(), config.getSystemMetricsConfig());
        final DumperThread thread = new DumperThread(dumper, "Dumper thread");
        List<String> included = new LinkedList<String>();
        List<String> excluded = new LinkedList<String>();
        List<String> systemProperties = new LinkedList<String>();
        included.add("duration");
        NetworkExportParams exportParams = new NetworkExportParamsImpl("localhost", 12345, 0, included, excluded, systemProperties);
        DataSender dataSender = new DataSender(exportParams);
        dataSender.start();
        dumper.getDumperCallsExporter().configureExport(dataSender.getJsonsToSend(), dataSender.getEmptyJsonBuffers(), exportParams);

        Thread.sleep(2000);
        thread.shutdown();
        root = dumper.getCurrentRoot();
    }

    @AfterSuite
    public void cleanup() {
        deleteDirectory(new File(dumpFolderName));
    }

    private Configuration_05 createConfiguration() {
        return new Configuration_05() {
            Map<String, ParameterInfo> map = new HashMap<String, ParameterInfo>();
            {
                getParameterInfo("sql").big(true).deduplicate(true);
            }

            public String getStoreTransformedClassesPath() {
                return null;
            }

            public void setParamType(String param, int type) {
            }

            public EnhancementRegistry getEnhancementRegistry() {
                return new EnhancementRegistry() {
                    public List getEnhancers(String className) {
                        return Collections.EMPTY_LIST;
                    }

                    public void addEnhancer(String className, Object filteredEnhancer) {
                    }

                    public Object getFilter(String name) {
                        return null;
                    }

                    public void addFilter(String name, Object filter) {
                    }
                };
            }

            public Map<String, Integer> getParamTypes() {
                return Collections.emptyMap();
            }

            public Map<String, ParameterInfo> getParametersInfo() {
                return map;
            }

            public ParameterInfo getParameterInfo(String name) {
                ParameterInfo info = map.get(name);
                if (info == null) {
                    info = new ParameterInfo(name);
                    map.put(name, info);
                }
                return info;
            }

            public long getLogMaxAge() {
                return TimeUnit.DAYS.toMillis(7);
            }

            public long getLogMaxSize() {
                return 1024 * 1024 * 1024;
            }

            public boolean isVerifyClassEnabled() {
                return false;
            }

            public List<MetricsConfiguration> getMetricsConfig(){return Collections.EMPTY_LIST;}

            public List<MetricsDescription> getSystemMetricsConfig() {return Collections.EMPTY_LIST;}

            public NetworkExportParams getNetworkExportParams() {
                return null;
            }
        };
    }

    @DataProvider
    public Object[][] mustHaveFiles() {
        return new Object[][]{
            {"suspend" + File.separatorChar + "000001.gz", "Suspend"},
            {"dictionary" + File.separatorChar + "000001.gz", "Dictionary"},
            {"trace" + File.separatorChar + "000001.gz", "Trace"},
            {"calls" + File.separatorChar + "000001.gz", "Calls"},
            {"sql" + File.separatorChar + "000001.gz", "SQL"},
            {"xml" + File.separatorChar + "000001.gz", "XML"}
        };
    }
}
