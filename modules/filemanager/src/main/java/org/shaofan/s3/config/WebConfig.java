package org.shaofan.s3.config;


import javax.servlet.MultipartConfigElement;

import org.shaofan.s3.intecept.S3Intecept;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class WebConfig implements WebMvcConfigurer {
    @Autowired
    private S3Intecept s3Intecept;
    
    @Autowired
    private SystemConfig config;
    
    
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 可添加多个
        registry.addInterceptor(s3Intecept).addPathPatterns("/s3/**");
    }

    
    /**
     * @return range bean for region (range request)
     */
    @Bean
    public RangeConverter rangeConverter() {
      return new RangeConverter();
    }
    
    @Bean  
    public ConversionService conversionService() {  
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();  
        // 注册自定义转换器  
        conversionService.addConverter(new RangeConverter());  
        return conversionService;  
    }  

   
}
