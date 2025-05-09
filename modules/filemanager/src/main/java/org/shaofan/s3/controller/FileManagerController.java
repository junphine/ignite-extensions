package org.shaofan.s3.controller;

import static org.shaofan.utils.RarUtils.unRarFile;
import static org.shaofan.utils.TargzUtils.unTargzFile;
import static org.shaofan.utils.ZipUtils.unZipFiles;
import static org.shaofan.utils.ZipUtils.zipFiles;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import javax.activation.MimetypesFileTypeMap;
import javax.mail.internet.MimeUtility;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.shaofan.s3.util.S3Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Type;
import software.amazon.awssdk.utils.StringInputStream;
import software.amazon.awssdk.utils.StringUtils;

/**
 * @author shaofan
 */
@RestController
@RequestMapping(value = "/s3-rest")
@CrossOrigin
public class FileManagerController  {  	
    
    @Autowired
    private S3Util s3Util;
    
    @Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;
    
    /**
    *
    * @param webjarsResourceURI
    * @return
    */
   private String[] getFileToken(String webjarsResourceURI) {
	   webjarsResourceURI = webjarsResourceURI.replaceAll("//", "/");
	   if(webjarsResourceURI.startsWith("/")) {
		   webjarsResourceURI = webjarsResourceURI.substring(1);
	   }
	   
       String[] tokens = webjarsResourceURI.split("/",2);
       if(tokens.length==1) {
    	   return new String[] { tokens[0], "" };
       }
       tokens[1] = tokens[1];
       return tokens;
   }

    /**
     * 展示文件列表
     */
    @PostMapping("list")
    public Object list(@RequestBody JSONObject json) throws ServletException {

        try {
            // 需要显示的目录路径            

            // 返回的结果集
            List<JSONObject> fileItems = new ArrayList<>();
            String[] tokens = getFileToken(json.getString("path"));
            String bucketName = tokens[0];
            String path = tokens[1];
            
            if(StringUtils.isBlank(bucketName)) {
            	List<Bucket> list = s3Util.getBucketList();

                String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
                for (Bucket pathObj : list) {
                	String fname = pathObj.name();            	             

                    // 封装返回JSON数据
                    JSONObject fileItem = new JSONObject();
                    fileItem.put("name", fname);
                    fileItem.put("date", dt.format(new Date(pathObj.creationDate().getEpochSecond()*1000)));
                    fileItem.put("size", 0);
                    fileItem.put("etag", null);
                    fileItem.put("rights", getPermissions(fname, null));
                    fileItem.put("type", "dir");
                    fileItems.add(fileItem);
                }
                
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("result", fileItems);
                return jsonObject;
            }

            List<S3Object> list = s3Util.getObjectAndPrefixList(bucketName, path);

            String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
            for (S3Object pathObj : list) {
            	String[] fnames = pathObj.key().split("/");            	             
            	String fname = fnames[fnames.length-1];
                // 封装返回JSON数据
                JSONObject fileItem = new JSONObject();
                fileItem.put("name", fname);
                fileItem.put("date", dt.format(new Date(pathObj.lastModified().toEpochMilli())));
                fileItem.put("size", pathObj.size());
                fileItem.put("etag", pathObj.eTag());
                fileItem.put("rights", getPermissions(bucketName,pathObj));
                fileItem.put("type", pathObj.key().endsWith("/")?"dir":"file");
                fileItems.add(fileItem);
            }
            
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("result", fileItems);
            return jsonObject;
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 文件上传
     */
    @PostMapping("upload")
    public Object upload(@RequestParam("destination") String destination, MultipartHttpServletRequest request) {
    	
        try {
            // Servlet3.0方式上传文件
            MultiValueMap<String, MultipartFile> files = request.getMultiFileMap();

            for (List<MultipartFile> file : files.values()) {
            	for(MultipartFile part: file)
	                if (part.getContentType() != null) {  // 忽略路径字段,只处理文件类型
	                   
	                    String[] tokens = getFileToken(destination);
	                    String bucketName = tokens[0];
	                    String path = tokens[1];	                    
	
	                    String filename = path+"/"+part.getOriginalFilename();
	                    if(path.isBlank() || path.equals("/")) {
	                    	filename = part.getOriginalFilename();
	                    }
	                    s3Util.upload(bucketName, filename, part.getInputStream());
	                    
	                    part.getInputStream().close();
	                }
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 文件下载/预览
     */
    @GetMapping("preview")
    public void preview(HttpServletResponse response, String path) throws IOException {

    	String[] tokens = getFileToken(path);
        String bucketName = tokens[0];
        path = tokens[1];   

        /*
         * 获取mimeType
         */
        String mimeType = new MimetypesFileTypeMap().getContentType(path);
        if (mimeType == null) {
            mimeType = "application/octet-stream";            
        }

        response.setContentType(mimeType);  
        response.setHeader("Content-Disposition", "inline; filename=\"" + MimeUtility.encodeWord(FilenameUtils.getName(path)) + "\"");

        try (
        	InputStream in = s3Util.getFileInputStream(bucketName, path);
        	InputStream inputStream = new BufferedInputStream(in)) {
            FileCopyUtils.copy(inputStream, response.getOutputStream());
        }
        catch(Exception e) {
        	error(e.getMessage());
        }
    }
    
    /**
     * 文件下载/预览
     */
    @GetMapping("view/**")
    public void view(HttpServletRequest request,HttpServletResponse response) throws IOException {
    	String base = request.getContextPath();
    	String uri = request.getRequestURI().replaceFirst(base+"/s3-rest/view/", "");
    	preview(response,uri);
    }


    /**
     * 创建目录
     */
    @PostMapping("createFolder")
    public Object createFolder(@RequestBody JSONObject json) {
        try {
            String newPath = json.getString("newPath");
            String[] tokens = getFileToken(newPath);
            String bucketName = tokens[0];
            String path = tokens[1];  
            if(!StringUtils.isBlank(path)) {
            	path = path+"/";
            }
            String jsonString = "{}";
            StringInputStream in = new StringInputStream(jsonString);   
            
			s3Util.upload(bucketName, path+"metadata.json", in);
			return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }
    
    private String getPermissions(String bucketName,S3Object file) throws IOException {
    	
    	if(file!=null) {    		
    		//List<Grant> grants = s3Util.getObjectACL(bucketName, file.key());
    		//return grants.toString();
    	}
    	// default perms
        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_WRITE);
        permissions.add(PosixFilePermission.OTHERS_READ);
        return PosixFilePermissions.toString(permissions);
    }

    /**
     * 修改文件或目录权限
     */
    @PostMapping("changePermissions")
    public Object changePermissions(@RequestBody JSONObject json) {
        try {

            String perms = json.getString("perms"); // 权限
            boolean recursive = json.getBoolean("recursive"); // 子目录是否生效
            Set<PosixFilePermission> posixPerms = PosixFilePermissions.fromString(perms);
            List<Grant> grants = new ArrayList<>();
            // admin用户完全控制
            Grant grant = Grant.builder().grantee(
    				Grantee.builder()
    				.displayName("admin")
    				.type(Type.CANONICAL_USER)
    				.id("admin")
    				.build())
    				.permission(Permission.FULL_CONTROL)
    			.build();
            grants.add(grant);
            
            ObjectCannedACL acl;
            if(posixPerms.contains(PosixFilePermission.OTHERS_WRITE)) {
        		acl = ObjectCannedACL.PUBLIC_READ_WRITE;
        	}
            else if(posixPerms.contains(PosixFilePermission.OTHERS_READ)) {
        		acl = ObjectCannedACL.PUBLIC_READ;
        	}
            else if(posixPerms.contains(PosixFilePermission.GROUP_WRITE)) {
        		acl = ObjectCannedACL.AUTHENTICATED_READ;
        	}
            else if(posixPerms.contains(PosixFilePermission.GROUP_READ)) {
            	acl = ObjectCannedACL.AUTHENTICATED_READ;
        	}
            else if(!posixPerms.contains(PosixFilePermission.OWNER_WRITE)) {
            	acl = ObjectCannedACL.BUCKET_OWNER_READ;
        	}
            else {
            	acl = ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL;
            }
            
            AccessControlPolicy policy = AccessControlPolicy.builder().grants(grants)
            	.build();
            
            JSONArray items = json.getJSONArray("items");
            for (int i = 0; i < items.size(); i++) {
                String key = items.getString(i);
                // 设置权限
                String[] tokens = getFileToken(key);
                String bucketName = tokens[0];
                String path = tokens[1]; 
    			s3Util.putObjectACL(bucketName, path, acl.toString(), policy);
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 复制文件或目录
     */
    @PostMapping("copy")
    public Object copy(@RequestBody JSONObject json, HttpServletRequest request) {
        try {
            String newpath = json.getString("newPath");
            JSONArray items = json.getJSONArray("items");
            
            String[] destTokens = getFileToken(newpath);
            String destBucketName = destTokens[0];
            String destKey = destTokens[1];
            String newFileName = json.getString("singleFilename");
            for (int i = 0; i < items.size(); i++) {
                String path = items.getString(i);
                String[] tokens = getFileToken(path);
                String bucketName = tokens[0];
                String key = tokens[1];

                String tofile = newFileName == null ? FilenameUtils.getName(key) : newFileName;
                boolean noExist = s3Util.headObject(destBucketName, destKey+"/"+tofile).containsKey("noExist");
                if(!noExist)
                	return error(destKey+"/"+tofile + " already exits!");
                
                try (
                	InputStream in = s3Util.getFileInputStream(bucketName, key);
                	InputStream inputStream = new BufferedInputStream(in); ) {
                	
                	s3Util.upload(destBucketName, destKey+"/"+tofile, inputStream);
                }
                catch(Exception e) {
                	 return error(e.getMessage());
                }
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 移动文件或目录
     */
    @PostMapping("move")
    public Object move(@RequestBody JSONObject json) {
        try {
            String newpath = json.getString("newPath");
            JSONArray items = json.getJSONArray("items");
            
            String[] destTokens = getFileToken(newpath);
            String destBucketName = destTokens[0];
            String destKey = destTokens[1];

            for (int i = 0; i < items.size(); i++) {                
                String path = items.getString(i);
                String[] tokens = getFileToken(path);
                String bucketName = tokens[0];
                String key = tokens[1];

                String tofile = FilenameUtils.getName(key);
                boolean noExist = s3Util.headObject(destBucketName, destKey+"/"+tofile).containsKey("noExist");
                if(!noExist)
                	return error(destKey+"/"+tofile + " already exits!");
                
                try (
                	InputStream in = s3Util.getFileInputStream(bucketName, key);
                	InputStream inputStream = new BufferedInputStream(in); ) {
                	
                	s3Util.upload(destBucketName, destKey+"/"+tofile, inputStream);
                	
                	s3Util.delete(bucketName, key);
                }
                catch(Exception e) {
                	 return error(e.getMessage());
                }
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 删除文件或目录
     */
    @PostMapping("remove")
    public Object remove(@RequestBody JSONObject json) {
        try {
            JSONArray items = json.getJSONArray("items");
            for (int i = 0; i < items.size(); i++) {
                String path = items.getString(i);
                String[] tokens = getFileToken(path);
                String bucketName = tokens[0];
                String key = tokens[1];
                s3Util.delete(bucketName, key);
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 重命名文件或目录
     */
    @PostMapping("rename")
    public Object rename(@RequestBody JSONObject json) {
        try {
            String path = json.getString("item");
            String newPath = json.getString("newItemPath");
            
            String[] destTokens = getFileToken(newPath);
            String destBucketName = destTokens[0];
            String destKey = destTokens[1];

            String[] tokens = getFileToken(path);
            String bucketName = tokens[0];
            String key = tokens[1];
            
            
            try (
            	InputStream in = s3Util.getFileInputStream(bucketName, key);
            	InputStream inputStream = new BufferedInputStream(in); ) {
            	
            	s3Util.upload(destBucketName, destKey, inputStream);
            	
            	s3Util.delete(bucketName, key);
            }
            catch(Exception e) {
            	 return error(e.getMessage());
            }           
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 查看文件内容,针对html、txt等可编辑文件
     */
    @PostMapping("getContent")
    public Object getContent(@RequestBody JSONObject json) {
        try {
            String path = json.getString("item");
            String[] tokens = getFileToken(path);
            String bucketName = tokens[0];
            String key = tokens[1];

            String content = new String(s3Util.getFileByte(bucketName, key),"UTF-8");

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("result", content);
            return jsonObject;
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 修改文件内容,针对html、txt等可编辑文件
     */
    @PostMapping("edit")
    public Object edit(@RequestBody JSONObject json) {
        try {
            String path = json.getString("item");
            String content = json.getString("content");

            String[] tokens = getFileToken(path);
            String bucketName = tokens[0];
            String key = tokens[1];
            
            StringInputStream in = new StringInputStream(content);    
            
			s3Util.upload(bucketName, key, in);

            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 文件压缩
     */
    @PostMapping("compress")
    public Object compress(@RequestBody JSONObject json) {
    	String root = systemConfig.getDataPath();
        try {
            String destination = json.getString("destination");
            String compressedFilename = json.getString("compressedFilename");
            JSONArray items = json.getJSONArray("items");
            List<File> files = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                File f = new File(root, items.getString(i));
                files.add(f);
            }

            File zip = new File(root + destination, compressedFilename);

            try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip))) {
                zipFiles(out, "", files.toArray(new File[files.size()]));
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }

    /**
     * 文件解压
     */
    @PostMapping("extract")
    public Object extract(@RequestBody JSONObject json) {
    	String root = systemConfig.getDataPath();
        try {
            String destination = json.getString("destination");
            String zipName = json.getString("item");
            String folderName = json.getString("folderName");
            if(folderName!=null) {
            	destination = destination+"/"+folderName;
            }
            
            File file = new File(root, zipName);

            String extension = org.shaofan.utils.FileUtils.getExtension(zipName);
            switch (extension) {
                case ".zip":
                    unZipFiles(file, root + destination);
                    break;
                case ".gz":
                    unTargzFile(file, root + destination);
                    break;
                case ".rar":
                    unRarFile(file, root + destination);
            }
            return success();
        } catch (Exception e) {
            return error(e.getMessage());
        }
    }


    private JSONObject error(String msg) {
        // { "result": { "success": false, "error": "msg" } }
        JSONObject result = new JSONObject();
        result.put("success", false);
        result.put("error", msg);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("result", result);
        return jsonObject;

    }

    private JSONObject success() {
        // { "result": { "success": true, "error": null } }
        JSONObject result = new JSONObject();
        result.put("success", true);
        result.put("error", null);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("result", result);
        return jsonObject;
    }

}
