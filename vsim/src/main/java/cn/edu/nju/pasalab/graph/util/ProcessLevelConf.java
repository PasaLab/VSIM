package cn.edu.nju.pasalab.graph.util;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This configuration reader will load configurations
 * from the file `pasa.conf.prop` in the working directory.
 *
 * It uses the static class to make sure that the configuration
 * is loaded only once for the current JVM.
 * Created by zk Wang on 12/3/2017.
 */
public class ProcessLevelConf extends Properties {
    // Available Configuration items
    public static final String CONF_FILE_NAME = "pasa.conf.prop";
    private static Logger logger;

    private static Properties properties = null;
    static {
        logger = Logger.getLogger(ProcessLevelConf.class.getName());
        try {
            FileInputStream inputStream = new FileInputStream(CONF_FILE_NAME);
            properties = new Properties();
            properties.load(inputStream);
            inputStream.close();
            logger.info("Load configurations: " + properties);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Can not load the process level configuration file.", e);
        }
    }
    public static Properties getPasaConf() {
        return properties;
    }
}
