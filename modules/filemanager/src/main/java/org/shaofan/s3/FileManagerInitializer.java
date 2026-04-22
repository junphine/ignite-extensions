package org.shaofan.s3;

import java.util.Set;

import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletContainerInitializer;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRegistration;
import jakarta.servlet.annotation.WebListener;

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

