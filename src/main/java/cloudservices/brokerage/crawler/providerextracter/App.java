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

//            fixProviders(providerDAO);
//            fixSameNameProviders(providerDAO);
            for (ServiceDescription serviceDesc : serviceDescDAO.getWithoutProvider()) {
                LOGGER.log(Level.FINE, "Service Description with ID : {0} Does not have provider, Updating", serviceDesc.getId());
                String name = URLExtracter.getDomainName(serviceDesc.getUrl());
                LOGGER.log(Level.FINE, "URL : {0} Name : {1}", new Object[]{serviceDesc.getUrl(), name});
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

    // THIS IS NOT SAFE BECAUSE OF DUPLICATES IN NAMES!!!!
    private static void fixProviders(ServiceProviderDAO providerDAO) throws DAOException, URISyntaxException {
        LOGGER.log(Level.INFO, "Fixing Providers Start");
        List<ServiceProvider> providers = providerDAO.getWithoutName();
        for (ServiceProvider provider : providers) {
            if (provider.getName().isEmpty()) {
                LOGGER.log(Level.FINE, "Provider with ID : {0} Does name have updating name from URL : {1}", new Object[]{provider.getId(), provider.getUrl()});
                String name = URLExtracter.getDomainName(provider.getUrl());
                provider.setName(name);
                providerDAO.saveOrUpdate(provider);
            }
        }
    }

    private static void fixSameNameProviders(ServiceProviderDAO providerDAO) throws DAOException {
        LOGGER.log(Level.INFO, "Fixing Providers with the same name Start");
        List<ServiceProvider> providers = providerDAO.getAll("ServiceProvider");
        for (ServiceProvider provider : providers) {
            try {
                ServiceProvider sp = providerDAO.findByName(provider.getName());
            } catch (DAOException ex) {
                LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            }
//            if (sameNames.isEmpty()) {
//                throw new DAOException("Service Provider with name : " + provider.getName() + " for provider with ID : " + provider.getId() + " is not available");
//            } else if (sameNames.size() == 1) {
//                LOGGER.log(Level.FINE, "Provider with Name : {0} and ID : {1} is fine", new Object[]{provider.getName(), provider.getId()});
//            } else {
//                LOGGER.log(Level.FINE, "There are different providers with Name : {0}", provider.getName());
//                ServiceProvider newName = new ServiceProvider();
//                for (ServiceProvider sameName : sameNames) {
//                    LOGGER.log(Level.FINER, "Provider with ID : {0} has the Name : {0}", new Object[]{provider.getId(),provider.getName()});
//                    
//                }
//            }
//            String name = URLExtracter.getDomainName(provider.getUrl());
//            provider.setName(name);
//            providerDAO.saveOrUpdate(provider);
        }
    }
}
