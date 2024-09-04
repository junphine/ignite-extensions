package org.shaofan.s3.controller;


import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.shaofan.s3.config.SystemConfig;
import org.shaofan.s3.model.*;
import org.shaofan.s3.service.S3Service;
import org.shaofan.s3.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * api参考地址 https://docs.aws.amazon.com/AmazonS3/latest/API/
 * @author admin
 *
 */
@RestController
@RequestMapping("/s3")
@CrossOrigin
public class S3Controller {
    @Autowired
    private S3Service s3Service;
    
    @Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;    

    // Bucket相关接口
    @PutMapping("/{bucketName}")
    public ResponseEntity<String> createBucket(@PathVariable String bucketName) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service.headBucket(bucketName)) {
            return ResponseEntity.status(409).body("BucketAlreadyExists");
        }
        s3Service.createBucket(bucketName);
        String lacation = systemConfig.getEndpointOverride()+bucketName+"/";
        return ResponseEntity.ok().location(new URI(lacation)).build();
    }

    @GetMapping("/")
    public ResponseEntity<String> listBuckets() throws Exception {
        String xml = "";
        ListBucketsResult result = new ListBucketsResult(s3Service.listBuckets());
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("ListAllMyBucketsResult");
        Element owner = root.addElement("Owner");
        Element id = owner.addElement("ID");
        id.setText(systemConfig.getAccessKey());
        Element displayName = owner.addElement("DisplayName");
        
        Element buckets = root.addElement("Buckets");
        for (Bucket item : result.getBuckets()) {
            Element bucket = buckets.addElement("Bucket");
            Element name = bucket.addElement("Name");
            name.setText(item.getName());
            displayName.setText(item.getAuthor());
            Element creationDate = bucket.addElement("CreationDate");
            creationDate.setText(item.getCreationDate());
        }
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out;
        out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @RequestMapping(value = "/{bucketName}", method = RequestMethod.HEAD)
    public ResponseEntity<Object> headBucket(@PathVariable(value = "bucketName") String bucketName,HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        if (s3Service.headBucket(bucketName)) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{bucketName}")
    public ResponseEntity<String> deleteBucket(@PathVariable String bucketName) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        s3Service.deleteBucket(bucketName);
        return ResponseEntity.noContent().build();
    }

    // List Object相关接口
    @GetMapping("/{bucketName}")
    public void listObjects(@PathVariable String bucketName, HttpServletRequest request,HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");        
        String prefix = ConvertOp.convert2String(request.getParameter("prefix"));
        boolean delimiter = request.getParameter("delimiter")!=null;
        _listObjects(bucketName,prefix,delimiter,response);
    }
    
    private void _listObjects(@PathVariable String bucketName, String prefix,boolean delimiter,HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");        
        
        List<S3Object> s3ObjectList = s3Service.listObjects(bucketName, prefix);
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("ListBucketResult");
        Element name = root.addElement("Name");
        name.setText(bucketName);
        Element prefixElement = root.addElement("Prefix");
        prefixElement.setText(prefix);
        if(delimiter) {
        	Element delimiterElement = root.addElement("Delimiter");
        	delimiterElement.setText("/");     	
        }
        Element isTruncated = root.addElement("IsTruncated");
        isTruncated.setText("false");
        Element maxKeys = root.addElement("MaxKeys");
        maxKeys.setText("5000");
        Element countKeys = root.addElement("KeyCount");
        countKeys.setText(""+s3ObjectList.size());
        Set<String> prefixs = new TreeSet<>();
        for (S3Object s3Object : s3ObjectList) {
        	if(delimiter) {
        		if(prefix==null || prefix.isEmpty()) {
        			if(s3Object.getKey().endsWith("/")) {
        				prefixs.add(s3Object.getKey());
        				continue;
        			}
        		}
        		else if(s3Object.getKey().endsWith("/")) {        			
        			prefixs.add(s3Object.getKey());
        			continue;
        		}
        		else if(s3Object.getKey().startsWith(prefix)) {
        			String[] filePathList = s3Object.getKey().split("/");
        	        StringBuilder result = new StringBuilder();
        	        for (int i = 0; i < filePathList.length - 1; i++) {
        	            result.append(filePathList[i]).append("/");
        	        }
        			prefixs.add(result.toString());   			
        		}
        		
        	}
            Element contents = root.addElement("Contents");
            Element key = contents.addElement("Key");
            key.setText(s3Object.getKey());
            if (!StringUtils.isEmpty(s3Object.getMetadata().getLastModified())) {
                Element lastModified = contents.addElement("LastModified");
                lastModified.setText(DateUtil.getDateIso8601Format(s3Object.getMetadata().getLastModified()));                
            }
            Element size = contents.addElement("Size");
            size.setText(s3Object.getMetadata().getContentLength() + "");
            
            Element ETag = contents.addElement("ETag");
            ETag.setText(s3Object.getMetadata().getETag());
            
            Element StorageClass = contents.addElement("StorageClass");
            StorageClass.setText("STANDARD");
            
        }
        if(delimiter) {
        	Element prefixesElement = root.addElement("CommonPrefixes");
        	for(String p : prefixs) {
	        	Element Prefix = prefixesElement.addElement("Prefix");
	        	Prefix.setText(p);
        	}
        }
        response.setHeader("ContentType", MediaType.APPLICATION_XML.toString());
        response.setCharacterEncoding("UTF-8");
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");        
        XMLWriter writer = new XMLWriter(response.getWriter(), format);
        writer.write(doc);
        writer.close();
    }


    @RequestMapping(value = "/{bucketName}/**", method = RequestMethod.HEAD)
    public void headObject(@PathVariable String bucketName, HttpServletRequest request, HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
        if (metadata==null) {
        	response.setStatus(404);
        } else {
        	HashMap<String, String> headInfo = new HashMap<>();
        	
            if(metadata.getContentLength()>0 && !metadata.getFileName().endsWith("/")) {
            	headInfo.put("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));
            	headInfo.put("Content-Length", ""+metadata.getContentLength());
            }
            headInfo.put("Content-Type", metadata.getContentType());
            headInfo.put("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            headInfo.put("ETag",metadata.getETag());
            
            for (String key : headInfo.keySet()) {
                response.addHeader(key, headInfo.get(key));
            }

        }
    }

    @PutMapping("/{bucketName}/**")
    public ResponseEntity<String> putObject(@PathVariable String bucketName, HttpServletRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        String copySource = request.getHeader("x-amz-copy-source");
        if(copySource!=null) {
        	copySource = URLDecoder.decode(copySource, "utf-8");
        }
        if (StringUtils.isEmpty(copySource)) {
            s3Service.putObject(bucketName, objectKey, request.getInputStream());
            return ResponseEntity.ok().build();
        } else {
            if (copySource.indexOf("\\?") >= 0) {
                copySource = copySource.split("\\?")[0];
            }
            if(copySource.startsWith("/")) {
            	copySource = copySource.substring(1);
            }
            String[] copyList = copySource.split("\\/");
            String sourceBucketName = "";
            for (String item : copyList) {
                if (!StringUtils.isEmpty(item)) {
                    sourceBucketName = item;
                    break;
                }
            }

            StringBuilder result = new StringBuilder();
            for (int i = 1; i < copyList.length; i++) {
                result.append(copyList[i]).append("/");
            }
            String sourceObjectKey = result.toString();
            s3Service.copyObject(sourceBucketName, sourceObjectKey, bucketName, objectKey);

            String xml = "";
            Document doc = DocumentHelper.createDocument();
            Element root = doc.addElement("CopyObjectResult");
            Element lastModified = root.addElement("LastModified");
            lastModified.setText(DateUtil.getUTCDateFormat());
            Element eTag = root.addElement("ETag");
            eTag.setText(EncryptUtil.encryptByMD5(copySource));
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("utf-8");
            StringWriter out;
            out = new StringWriter();
            XMLWriter writer = new XMLWriter(out, format);
            writer.write(doc);
            writer.close();
            xml = out.toString();
            out.close();
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
        }
    }

    @GetMapping("/{bucketName}/**")
    public void getOrListObject(@PathVariable String bucketName, HttpServletRequest request, HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        ObjectMetadata metadata = s3Service.headObject(bucketName, objectKey);
        if (metadata == null ) {
        	response.setStatus(404);
        } 
        else if(metadata.getContentLength()==0 && metadata.getFileName().endsWith("/")) {
        	boolean delimiter = request.getParameter("delimiter")!=null;
        	_listObjects(bucketName,objectKey,delimiter,response);
        }
        else {
        	S3ObjectInputStream objectStream = s3Service.getObject(bucketName, objectKey);        	
        	
        	response.addHeader("Content-Disposition", "filename=" + URLEncoder.encode(metadata.getFileName(), "utf-8"));            
            response.addHeader("Last-Modified", DateUtil.getDateGMTFormat(metadata.getLastModified()));                
            response.addHeader("ETag",metadata.getETag());
            response.setContentType(metadata.getContentType());          
            response.setContentLengthLong(metadata.getContentLength());
            response.setCharacterEncoding(metadata.getContentEncoding());
            
            byte[] buff = new byte[1024*16];
            BufferedInputStream bufferedInputStream = null;
            OutputStream outputStream = null;
            try {
                outputStream = response.getOutputStream();
                bufferedInputStream = new BufferedInputStream(objectStream);
                int i = 0;
                while ((i = bufferedInputStream.read(buff)) != -1) {
                    outputStream.write(buff, 0, i);                    
                }
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
                response.setStatus(404);
            } finally {
                try {
                    bufferedInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }        
        
    }

    @DeleteMapping("/{bucketName}/**")
    public ResponseEntity<String> deleteObject(@PathVariable String bucketName, HttpServletRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        s3Service.deleteObject(bucketName, objectKey);
        return ResponseEntity.noContent().build();
    }


    // 分片上传
    @RequestMapping(value = "/{bucketName}/**", method = RequestMethod.POST, params = "uploads")
    public ResponseEntity<Object> createMultipartUpload(@PathVariable String bucketName, HttpServletRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        InitiateMultipartUploadResult result = s3Service.initiateMultipartUpload(bucketName, objectKey);

        String xml = "";
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("InitiateMultipartUploadResult");
        Element bucket = root.addElement("Bucket");
        bucket.setText(bucketName);
        Element key = root.addElement("Key");
        key.setText(objectKey);
        Element uploadId = root.addElement("UploadId");
        uploadId.setText(result.getUploadId());
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out;
        out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }

    @RequestMapping(value = "/{bucketName}/**", method = RequestMethod.PUT, params = {"partNumber", "uploadId"})
    public ResponseEntity<String> uploadPart(@PathVariable String bucketName, HttpServletRequest request, HttpServletResponse response) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        int partNumber = ConvertOp.convert2Int(request.getParameter("partNumber"));
        String uploadId = request.getParameter("uploadId");
        PartETag eTag = s3Service.uploadPart(bucketName, objectKey, partNumber, uploadId, request.getInputStream());
        response.addHeader("ETag", eTag.geteTag());
        return ResponseEntity.ok().build();
    }

    @RequestMapping(value = "/{bucketName}/**", method = RequestMethod.POST, params = "uploadId")
    public ResponseEntity<String> completeMultipartUpload(@PathVariable String bucketName, HttpServletRequest request) throws Exception {
        bucketName = URLDecoder.decode(bucketName, "utf-8");
        String pageUrl = URLDecoder.decode(request.getRequestURI(), "utf-8");
        if (pageUrl.indexOf("\\?") >= 0) {
            pageUrl = pageUrl.split("\\?")[0];
        }
        String objectKey = pageUrl.replace(request.getContextPath() + "/s3/" + bucketName + "/", "");
        String uploadId = request.getParameter("uploadId");
        List<PartETag> partETags = new ArrayList<>();

        SAXReader reader = new SAXReader();
        Document bodyDoc = reader.read(request.getInputStream());
        Element bodyRoot = bodyDoc.getRootElement();
        List<Element> elementList = bodyRoot.elements("Part");
        for (Element element : elementList) {
            int partNumber = ConvertOp.convert2Int(element.element("PartNumber").getText());
            String eTag = element.element("ETag").getText();
            PartETag partETag = new PartETag(partNumber, eTag);
            partETags.add(partETag);
        }
        CompleteMultipartUploadResult result = s3Service.completeMultipartUpload(bucketName, objectKey, uploadId, new CompleteMultipartUpload(partETags));
        String xml = "";
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("CompleteMultipartUploadResult");
        Element location = root.addElement("Location");
        location.setText(CommonUtil.getApiPath() + "s3/" + bucketName + "/" + objectKey);
        Element bucket = root.addElement("Bucket");
        bucket.setText(bucketName);
        Element key = root.addElement("Key");
        key.setText(objectKey);
        Element etag = root.addElement("ETag");
        etag.setText(result.geteTag());

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        StringWriter out;
        out = new StringWriter();
        XMLWriter writer = new XMLWriter(out, format);
        writer.write(doc);
        writer.close();
        xml = out.toString();
        out.close();
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xml);
    }
}
