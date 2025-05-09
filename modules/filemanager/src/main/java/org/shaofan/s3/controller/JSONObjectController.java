package org.shaofan.s3.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import software.amazon.awssdk.utils.StringInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.ignite.internal.processors.rest.igfs.model.S3Object;
import org.apache.ignite.internal.processors.rest.igfs.model.S3ObjectInputStream;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.mail.internet.MimeUtility;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * for amis
 */
@RestController
@RequestMapping(value = "/docs")
@CrossOrigin
public class JSONObjectController {
	private static final String ACCEPT_JSON = "Accept=application/json";
	private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";

	@Autowired
	private S3Service s3Service;

	private String bucketName = "json_datasets";

	private String key(String coll, String docId) {
		coll = StringUtils.trimLeadingCharacter(coll, '/');
		coll = StringUtils.trimTrailingCharacter(coll, '/');
		docId = StringUtils.trimLeadingCharacter(docId, '/');
		docId = StringUtils.trimTrailingCharacter(docId, '/');

		return coll + "/" + docId + ".json";
	}
	
	private Map<String, String> getUserMetadata(final HttpServletRequest request) {
        return Collections.list(request.getHeaderNames()).stream()
            .filter(header -> header.startsWith(HEADER_X_AMZ_META_PREFIX))
            .collect(Collectors.toMap(
                header -> header.substring(HEADER_X_AMZ_META_PREFIX.length()),
                request::getHeader
            ));
      }

	/**
	 * 展示JSON collection列表
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public JSONObject list(@RequestParam(value = "name", required = false) String name) {
		JSONObject jsonObject = new JSONObject();
		try {
			// 需要显示的目录路径
			// 返回的结果集
			List<JSONObject> fileItems = new ArrayList<>();

			List<S3Object> list = s3Service.listObjects(bucketName, null);

			String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
			SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
			for (S3Object pathObj : list) {
				String fname = pathObj.getKey();

				if (name != null && !name.isEmpty()) {
					if (name.charAt(0) == '/' && !fname.startsWith(name)) {
						continue;
					}
					if (name.charAt(0) != '/' && fname.indexOf(name) < 0) {
						continue;
					}
				}

				// 封装返回JSON数据
				JSONObject fileItem = new JSONObject();
				fileItem.put("name", fname);
				fileItem.put("date", dt.format(pathObj.getMetadata().getLastModified()));
				fileItem.put("size", pathObj.getMetadata().getContentLength());
				fileItem.put("etag", pathObj.getMetadata().getETag());
				fileItem.put("type", fname.endsWith("/") ? "dir" : "file");
				fileItems.add(fileItem);
			}

			jsonObject.put("data", fileItems);
			jsonObject.put("status", 0);
			return jsonObject;
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 展示JSON对象列表
	 */
	@RequestMapping(value = "/{collection}", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public JSONObject all_collection_docs(@PathVariable("collection") String collection,
			@RequestParam(value = "name", required = false) String name) {
		JSONObject jsonObject = new JSONObject();
		try {
			// 需要显示的目录路径
			// 返回的结果集

			List<S3Object> list = s3Service.listObjects(bucketName, collection);

			List<JSONObject> fileItems = list.parallelStream().filter((S3Object pathObj) -> {
				String fname = pathObj.getKey().substring(collection.length());

				if (name != null && !name.isEmpty()) {
					if (name.charAt(0) == '/' && !fname.startsWith(name)) {
						return false;
					}
					if (name.charAt(0) != '/' && fname.indexOf(name) < 0) {
						return false;
					}
				}
				return true;
			}).map((S3Object pathObj) -> {
				String fname = pathObj.getKey();

				S3ObjectInputStream objectStream = s3Service.getObject(bucketName, fname);

				JSONObject json;
				try {
					json = JSONObject.parseObject(objectStream, StandardCharsets.UTF_8, JSONObject.class);
					if (fname.endsWith(".json")) {
						fname = fname.substring(0, fname.length() - 5);
					}
					json.put("_id", fname);
					return json;

				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}

			}).collect(Collectors.toList());

			jsonObject.put("data", fileItems);
			jsonObject.put("status", 0);
			return jsonObject;
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 文档创建
	 */
	@RequestMapping(value = "/{collection}/{path}", method = RequestMethod.POST, headers = ACCEPT_JSON)
	public JSONObject upload(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			HttpServletRequest request,
			@RequestBody JSONObject json) {

		try {
			Map<String,String> userMeta = getUserMetadata(request);
			StringInputStream in = new StringInputStream(json.toJSONString());
			s3Service.putObject(bucketName, key(collection, destination), in, userMeta);
			return success(key(collection, destination));
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	/**
	 * 文档下载/预览
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "/{collection}/{path}", method = RequestMethod.GET, headers = ACCEPT_JSON)
	public void preview(HttpServletResponse response, 
			@PathVariable("collection") String collection,
			@PathVariable("path") String path) throws IOException {

		response.setContentType("application/json");
		response.setHeader("Content-Disposition",
				"inline; filename=\"" + MimeUtility.encodeWord(FilenameUtils.getName(path)) + "\"");

		try (S3ObjectInputStream objectStream = s3Service.getObject(bucketName, key(collection, path))) {

			FileCopyUtils.copy(objectStream, response.getOutputStream());
		} catch (Exception e) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Resource Not Found");
		}
	}

	/**
	 * 文件全量更新，返回新版本
	 */
	@RequestMapping(value = "/{collection}/{path}", method = RequestMethod.PUT, headers = ACCEPT_JSON)
	@ResponseBody
	public JSONObject put(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			HttpServletRequest request,
			@RequestBody JSONObject updates) {

		try {
			Map<String,String> userMeta = getUserMetadata(request);
			StringInputStream in = new StringInputStream(updates.toJSONString());
			s3Service.putObject(bucketName, key(collection, destination), in, userMeta);
			return success(key(collection, destination));
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}

	}

	/**
	 * 删除文档中的内容keys
	 */
	@RequestMapping(value = "/{collection}/{path}", method = RequestMethod.DELETE, headers = ACCEPT_JSON)
	@ResponseBody
	public JSONObject remove(@PathVariable("collection") String collection, 
			@PathVariable("path") String destination,
			@RequestBody JSONObject deletes) {
		try {
			String path = key(collection, destination);
			if (deletes == null) {
				s3Service.deleteObject(bucketName, path);
				return success(path);
			}

			S3ObjectInputStream objectStream = s3Service.getObject(bucketName, path);

			JSONObject json;
			json = JSONObject.parseObject(objectStream, StandardCharsets.UTF_8, JSONObject.class);

			deleteMap(json, deletes);

			StringInputStream in = new StringInputStream(json.toJSONString());

			s3Service.putObject(bucketName, path, in, null);
			return success(deletes.keySet());

		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	private void deleteMap(Map<String, Object> json, Map<String, Object> deletes) {
		for (Map.Entry<String, Object> ent : deletes.entrySet()) {
			String key = ent.getKey();
			if (ent.getValue() instanceof Map) {
				Object value = json.get(key);
				if (value instanceof Map) {
					deleteMap((Map) value, (Map) ent.getValue());
				}
			} else if (ent.getValue() instanceof List) {
				Object value = json.get(key);
				if (value instanceof Map) {
					Map<String, Object> jsonValue = (Map) value;
					List keys = (List) ent.getValue();
					for (Object id : keys) {
						jsonValue.remove(id);
					}
				}
			} else {
				json.remove(key);
			}
		}
	}

	/**
	 * 查看文件元信息
	 */
	@RequestMapping("/{collection}/{path}/meta")
	public JSONObject getContent(@PathVariable("collection") String collection,
			@PathVariable("path") String destination) {
		try {
			JSONObject jsonObject = new JSONObject();
			List<S3Object> list = s3Service.listObjects(bucketName, key(collection, destination));
			String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
			SimpleDateFormat dt = new SimpleDateFormat(DATE_FORMAT);
			for (S3Object pathObj : list) {
				String fname = pathObj.getKey();

				// 封装返回JSON数据
				JSONObject fileItem = new JSONObject();
				fileItem.put("name", fname);
				fileItem.put("date", dt.format(pathObj.getMetadata().getLastModified()));
				fileItem.put("size", pathObj.getMetadata().getContentLength());
				fileItem.put("etag", pathObj.getMetadata().getETag());
				fileItem.put("type", fname.endsWith("/") ? "dir" : "file");

				jsonObject.put("data", fileItem);
				jsonObject.put("status", 0);
			}

			if (list.size() == 0) {
				jsonObject.put("status", 400);
			}

			return jsonObject;

		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}
	}

	@RequestMapping(value = "/{collection}/{path}", method = RequestMethod.PATCH, headers = ACCEPT_JSON)
	public JSONObject patch(@PathVariable("collection") String collection, 
			@PathVariable("path") String path,
			@RequestBody JSONObject updates) {

		try {
			S3ObjectInputStream objectStream = s3Service.getObject(bucketName, path);

			JSONObject json = JSONObject.parseObject(objectStream, StandardCharsets.UTF_8, JSONObject.class);
			json.putAll(updates);

			StringInputStream in = new StringInputStream(json.toJSONString());

			s3Service.putObject(bucketName, key(collection, path), in, null);
			return success("");
		} catch (Exception e) {
			return error(e.getMessage(), 500);
		}

	}

	private JSONObject error(String msg, int status) {
		JSONObject result = new JSONObject();
		result.put("message", msg);
		result.put("status", status);
		result.put("error", msg);
		return result;

	}

	private JSONObject success(Object data) {
		JSONObject result = new JSONObject();
		result.put("message", "success");
		result.put("status", 0);
		result.put("data", data);
		return result;
	}

}
