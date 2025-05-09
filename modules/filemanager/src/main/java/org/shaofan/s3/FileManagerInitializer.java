package org.shaofan.s3;

import java.util.Set;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.WebListener;

@WebListener
public class FileManagerInitializer implements ServletContextListener {
	private static ServletContext servletContext;
	
	
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        servletContext = servletContextEvent.getServletContext();
        System.out.println(servletContext);
        
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
	
}

