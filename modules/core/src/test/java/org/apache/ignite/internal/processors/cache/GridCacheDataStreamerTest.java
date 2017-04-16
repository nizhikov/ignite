package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.test.ignite3935.StreamingExampleCacheEntryProcessor;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;

/**
 * @author NIzhikov
 */
public class GridCacheDataStreamerTest extends GridCommonAbstractTest {
    /**
     * jar name contains StreamingExampleCacheEntryProcessor
     */
    public static final String IGNITE_3935_1_0_JAR = "ignite-3935-1.0.jar";

    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setClientMode(false);
        cfg.setPeerClassLoadingEnabled(true);
        return cfg;
    }

    /**
     * @throws Exception
     */
    public void testCacheEntryProcessor() throws Exception {
        //IGNITE-3935 tells about bug in clientMode=true
        //But we can't start grid in separate jvm without local
        //So before starting separate jvm grid we start local
        Ignite local = startGrid(0);
        //StreamingExampleCacheEntryProcessor included from binary dependencies - modules/core/src/test/binaries
        //Have to exclude this dependency from remote grid - see excludeIgnite3935JarFromClasspath
        //So this class should be loaded by peer class loader
        Ignite remote = startGrid(1);
        stopGrid(0);

        Ignition.setClientMode(true);
        try (Ignite client = Ignition.start(getLocalConfiguration())) {
            IgniteCache<String, Long> stmCache = client.getOrCreateCache("mycache");
            try (IgniteDataStreamer<String, Long> stmr = client.dataStreamer(stmCache.getName())) {
                stmr.allowOverwrite(true);
                stmr.receiver(StreamTransformer.from(new StreamingExampleCacheEntryProcessor()));
                stmr.addData("word", 1L);
                System.out.println("Finished");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    @NotNull private IgniteConfiguration getLocalConfiguration() throws Exception {
        final IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(true);
        return cfg;
    }

    @Override protected IgniteProcessProxy igniteProcessProxy(IgniteConfiguration cfg) throws Exception {
        return new IgniteProcessProxy(cfg, log, grid(0)) {
            @NotNull @Override protected Collection<String> filteredJvmArgs(IgniteConfiguration cfg) {
                Collection<String> filteredJvmArgs = new ArrayList<>();

                Marshaller marsh = cfg.getMarshaller();

                if (marsh != null)
                    filteredJvmArgs.add("-D" + IgniteTestResources.MARSH_CLASS_NAME + "=" + marsh.getClass().getName());

                for (String arg : U.jvmArgs()) {
                    if (arg.startsWith("-Xmx") || arg.startsWith("-Xms") || arg.startsWith("-cp") || arg.startsWith("-classpath") ||
                        (marsh != null && arg.startsWith("-D" + IgniteTestResources.MARSH_CLASS_NAME)))
                        filteredJvmArgs.add(arg);
                }

                String classpath = System.getProperty("java.class.path");
                String sfcp = System.getProperty("surefire.test.class.path");
                if (sfcp != null)
                    classpath += System.getProperty("path.separator") + sfcp;

                filteredJvmArgs.add("-cp");
                filteredJvmArgs.add(excludeIgnite3935JarFromClasspath(classpath));
                return filteredJvmArgs;
            }

            /**
             * Excluding ignite-3935-1.0.jar so StreamingExampleCacheEntryProcessor should be loaded by peer class loader
             */
            public String excludeIgnite3935JarFromClasspath(String classpath) {
                final String[] classpathArr = classpath.split(":");
                classpath = "";
                for (int i = 0; i < classpathArr.length; i++) {
                    if (!classpathArr[i].contains(IGNITE_3935_1_0_JAR)) {
                        classpath += (classpath.length() > 0 ? ":" : "") + classpathArr[i];
                    }
                }
                return classpath;
            }
        };
    }
}
