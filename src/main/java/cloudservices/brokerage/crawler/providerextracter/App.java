/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.providerextracter;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.commons.utils.url_utils.URLExtracter;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v3.ServiceDescriptionDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v3.ServiceProviderDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v3.ServiceDescription;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v3.ServiceProvider;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class App {

    private final static Logger LOGGER = Logger.getLogger(App.class.getName());
    private static int savedProvidersNum;
    private static int updatedProvidersNum;
    private static int totalNum;
    private final static String TOKEN = ";;;";

    public static void main(String[] args) {
        createLogFile();
//        createNewDB();

        long startTime = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "Extracting Providers Start");

        try {
            Configuration v3Configuration = new Configuration();
            v3Configuration.configure("v3hibernate.cfg.xml");
            ServiceDescriptionDAO serviceDescDAO = new ServiceDescriptionDAO();
            ServiceProviderDAO providerDAO = new ServiceProviderDAO();
            ServiceDescriptionDAO.openSession(v3Configuration);

            fixProviders(providerDAO);

            for (ServiceDescription serviceDesc : serviceDescDAO.getWithoutProvider()) {
                String name = URLExtracter.getDomainName(serviceDesc.getUrl());
                ServiceProvider provider = new ServiceProvider();
                provider.setNumberOfServices(1);
                provider.setName(name);
                serviceDesc.setServiceProvider(addOrUpdateProvider(provider, providerDAO));
                serviceDescDAO.saveOrUpdate(serviceDesc);
            }
        } catch (DAOException | URISyntaxException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            BaseDAO.closeSession();
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            LOGGER.log(Level.SEVERE, "Searching End in {0}ms", totalTime);
            LOGGER.log(Level.SEVERE, "Total Services Found: {0}", totalNum);
            LOGGER.log(Level.SEVERE, "Total Providers Saved: {0}", savedProvidersNum);
            LOGGER.log(Level.SEVERE, "Total Providers Modified: {0}", updatedProvidersNum);
        }
    }

    private static ServiceProvider addOrUpdateProvider(ServiceProvider serviceProvider, ServiceProviderDAO serviceProviderDAO) throws DAOException {
        ServiceProvider inDB = serviceProviderDAO.findByName(serviceProvider.getName());
        if (inDB == null) {
            LOGGER.log(Level.FINE, "There is no service provider in DB with Name = {0}, Saving a new one", serviceProvider.getName());
            serviceProviderDAO.addServiceProvider(serviceProvider);
            savedProvidersNum++;
            return serviceProvider;
        } else {
            LOGGER.log(Level.FINE, "Found the same provider name with ID = {0} in DB, Updating", inDB.getId());
            if (!serviceProvider.getCountry().isEmpty()) {
                inDB.setCountry(inDB.getCountry().concat(TOKEN).concat(serviceProvider.getCountry()));
            }
            if (!serviceProvider.getDescription().isEmpty()) {
                inDB.setDescription(inDB.getDescription().concat(TOKEN).concat(serviceProvider.getDescription()));

            }
            if (!serviceProvider.getExtraInfo().isEmpty()) {
                inDB.setExtraInfo(inDB.getExtraInfo().concat(TOKEN).concat(serviceProvider.getExtraInfo()));

            }
            if (!serviceProvider.getTags().isEmpty()) {
                inDB.setTags(inDB.getTags().concat(TOKEN).concat(serviceProvider.getTags()));

            }

            inDB.setNumberOfServices(inDB.getNumberOfServices() + serviceProvider.getNumberOfServices());
            serviceProviderDAO.saveOrUpdate(inDB);
            updatedProvidersNum++;
            return inDB;
        }
    }

    private static void createNewDB() {
        try {
            Configuration configuration = new Configuration();
            configuration.configure("v3hibernate.cfg.xml");
            BaseDAO.openSession(configuration);
            LOGGER.log(Level.INFO, "Database Creation Successful");
        } finally {
            BaseDAO.closeSession();
        }
    }

    private static boolean createLogFile() {
        try {
            StringBuilder sb = new StringBuilder();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm");
            Calendar cal = Calendar.getInstance();
            sb.append(dateFormat.format(cal.getTime()));
            String filename = sb.toString();
            DirectoryUtil.createDir("logs");
            LoggerSetup.setup("logs/" + filename + ".txt", "logs/" + filename + ".html", Level.FINER);
            return true;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return false;
        }
    }

    private static void fixProviders(ServiceProviderDAO providerDAO) throws DAOException, URISyntaxException {
        List<ServiceProvider> providers = providerDAO.getAll("ServiceProvider");
        for (ServiceProvider provider : providers) {
            if (provider.getName().isEmpty()) {
                String name = URLExtracter.getDomainName(provider.getUrl());
                provider.setName(name);
                providerDAO.saveOrUpdate(provider);
            }
        }
    }
}
