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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.GridKernalContext;



@WebListener
public class FileManagerInitializer implements ServletContextListener {
	private static ServletContext servletContext;
	private static Ignite ignite;
	public static long cpuStartTime = System.currentTimeMillis();
	
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        servletContext = servletContextEvent.getServletContext();
        System.out.println(servletContext);
        
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }	
	
	public static Ignite ignite() {
		if(ignite==null) {
			try {
				onStartup(servletContext);
			} catch (ServletException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ignite;
	}
	
	public static void onStartup(ServletContext context) throws ServletException {
		String instanceName = context.getInitParameter("ignite.igfs.instanceName");
		
		if(ignite!=null && ignite.name().equals(instanceName)) {
			return ;
		}
		
		GridKernalContext ctx = (GridKernalContext) context.getAttribute("gridKernalContext");
        if(ctx==null) {        	
        	if(instanceName !=null && Ignition.allGrids().size()>0) {
        		ignite = Ignition.ignite(instanceName);
        	}
        	else {
        		ignite = Ignition.ignite();
        	}
        }
        else {
        	if(!ctx.grid().fileSystems().isEmpty()) {
        		ignite = ctx.grid();
        	}
        }        
	}
}

