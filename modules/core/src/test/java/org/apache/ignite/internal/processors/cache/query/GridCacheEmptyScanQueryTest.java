package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.test.ignite2190.Employee;
import org.apache.ignite.test.ignite2190.EmployeePredicate;
import org.apache.ignite.test.ignite2190.ObjectPredicate;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 */
public class GridCacheEmptyScanQueryTest extends GridCommonAbstractTest {
    /** jar name contains Employee, ObjectPredicate, EmployeePredicate classes. */
    private static final String IGNITE_2190_1_0_JAR = "ignite-2190-1.0.jar";

    /** {@inheritdoc} */
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
     * Runs empty ScanQuery with OptimizedMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQueryWithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller");
    }

    /**
     * Runs empty ScanQuery with BinaryMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQueryWithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller");
    }

    /**
     * Runs ScanQuery with (Object, Object) predicate and OptimizedMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery2WithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(new ObjectPredicate()), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller");
    }

    /**
     * Runs ScanQuery with (Object, Object) predicate and BinaryMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery2WithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(new ObjectPredicate()), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller");
    }

    /**
     * Runs ScanQuery with (Integer, Employee) predicate and OptimizedMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery3WithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller");
    }

    /**
     * Runs ScanQuery with (Integer, Employee) predicate and BinaryMarshaller.
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery3WithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller");
    }

    /**
     * Runs specified ScanQuery using <code>marsh</code>, check it return <code>expSz</code> record.
     * @throws Exception If failed.
     */
    public void runEmptyScanQuery(ScanQuery<Integer, Employee> qry, int expSz, String marsh) throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, marsh);
        Ignite local = startGrid(0);
        Ignite remote = startGrid(1);
        stopGrid(0);

        Ignition.setClientMode(true);
        try (Ignite client = Ignition.start(getLocalConfiguration())) {
            CacheConfiguration<Integer, Employee> cacheCfg = new CacheConfiguration<>("testCache");

            IgniteCache<Integer, Employee> cache = client.getOrCreateCache(cacheCfg);

            cache.put(1, new Employee(1, "name 1"));
            cache.put(2, new Employee(2, "name 2"));

            assertEquals("Size of result of always true ScanQuery should be 2", expSz,
                cache.query(qry).getAll().size());
        }
        finally {
            stopAllGrids();
        }
    }

    /** Return configuration for grid running in same jvm. */
    private IgniteConfiguration getLocalConfiguration() throws Exception {
        final IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(true);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteProcessProxy igniteProcessProxy(IgniteConfiguration cfg, Ignite locNode,
        boolean resetDiscovery)
        throws Exception {
        return new IgniteProcessProxy(cfg, log, grid(0)) {
            /** {@inheritDoc} */
            @Override protected Collection<String> filteredJvmArgs(IgniteConfiguration cfg) {
                Collection<String> defaultFilteredJvmArgs = super.filteredJvmArgs(cfg);
                List<String> filteredJvmArgs = new ArrayList<>();

                boolean classpathFound = false;
                Iterator<String> iDefaultFilteredJvmArgs = defaultFilteredJvmArgs.iterator();
                while (iDefaultFilteredJvmArgs.hasNext()) {
                    String arg = iDefaultFilteredJvmArgs.next();

                    if ("-cp".equals(arg) || "-classpath".equals(arg)) {
                        filteredJvmArgs.add(arg);
                        filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(iDefaultFilteredJvmArgs.next()));
                        classpathFound = true;
                    } else
                        filteredJvmArgs.add(arg);
                }

                if (!classpathFound) {
                    String classpath = System.getProperty("java.class.path");
                    String sfcp = System.getProperty("surefire.test.class.path");

                    if (sfcp != null)
                        classpath += System.getProperty("path.separator") + sfcp;

                    filteredJvmArgs.add("-cp");
                    filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(classpath));
                }

                return filteredJvmArgs;
            }

            /** Excluding ignite-2190-1.0.jar so Employee class become invisible for a remote node. */
            String excludeIgnite2190JarFromClasspath(String classpath) {
                final String pathSeparator = System.getProperty("path.separator");
                final String[] classpathArr = classpath.split(pathSeparator);
                classpath = "";
                for (String aClasspathArr : classpathArr) {
                    if (!aClasspathArr.contains(IGNITE_2190_1_0_JAR)) {
                        classpath += (classpath.length() > 0 ? pathSeparator : "") + aClasspathArr;
                    }
                }
                return classpath;
            }
        };
    }
}
