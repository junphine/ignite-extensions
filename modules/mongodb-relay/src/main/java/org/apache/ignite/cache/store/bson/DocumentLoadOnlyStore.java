/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.bson;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.cache.integration.CacheLoaderException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.store.CacheLoadOnlyStoreAdapter;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeDefaultHasher;
import org.apache.ignite.cache.store.jdbc.JdbcTypeHasher;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformer;
import org.apache.ignite.cache.store.jdbc.JdbcTypesTransformer;
import org.apache.ignite.configuration.CacheConfiguration;

import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil;
import de.bwaldvogel.mongo.bson.Document;

/**
 * Implementation of {@link CacheStore} backed by CSV File and POJO via reflection.
 *
 * This implementation stores objects in underlying database using java beans mapping description via reflection. <p>
 * Use {@link DocumentLoadOnlyStoreFactory} factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 */
    
public	class DocumentLoadOnlyStore<K> extends CacheLoadOnlyStoreAdapter<K, BinaryObject, Document> implements Serializable {
    
	private static final long serialVersionUID = 1L;
	/** Csv file name. */
    final String filePath;
    final String idField;
    final List<Document> collection = new ArrayList<>();
    boolean streamerEnabled = false;
    
    /** Hash calculator.  */
    protected JdbcTypeHasher hasher = JdbcTypeDefaultHasher.INSTANCE;

    /** Types transformer. */
    protected JdbcTypesTransformer transformer = JdbcTypesDefaultTransformer.INSTANCE;

    /** Types that store could process. */
    private JdbcType[] types;
    
    /** Auto injected ignite instance. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** Constructor. */
    public DocumentLoadOnlyStore(String inputFile,String idField) {
        this.filePath = inputFile;
        this.idField = idField;
    }    
   

    public void processZipFile(File zipFile) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
                    String collectionName = getCollectionNameFromZipEntry(entry);
                    processCsvStream(zipIn, collectionName);
                }
                zipIn.closeEntry();
            }
        }
    }

    public void processCsvFile(File csvFile) throws IOException {
        String collectionName = getCollectionNameFromFile(csvFile);
        try (FileInputStream fis = new FileInputStream(csvFile)) {
            processCsvStream(fis, collectionName);
        }
    }

    private String getCollectionNameFromZipEntry(ZipEntry entry) {
        String fileName = entry.getName();
        // Remove directory path and file extension
        int lastSlash = fileName.lastIndexOf('/');
        if (lastSlash != -1) {
            fileName = fileName.substring(lastSlash + 1);
        }
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private String getCollectionNameFromFile(File file) {
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private void processCsvStream(InputStream inputStream, String collectionName) {
       
        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            CSVParser parser = CSVFormat.DEFAULT.builder()
            		.setSkipHeaderRecord(true)
                    .setIgnoreEmptyLines(true)
                    .setIgnoreHeaderCase(true)
                    .setTrim(true)
                    .build().parse(reader);

            List<Document> batch = new ArrayList<>(this.getBatchSize());
            for (CSVRecord record : parser) {
                Document doc = new Document();
                doc.append("_class",collectionName);
                parser.getHeaderNames().forEach(header -> 
                    doc.append(header, record.get(header))
                );
                batch.add(doc);

                if (batch.size() >= this.getBatchSize()) {
                    collection.addAll(batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                collection.addAll(batch);
            }
            System.out.println("Imported " + collectionName + " (" + parser.getRecordNumber() + " records)");
        } catch (IOException e) {
            System.err.println("Error processing collection " + collectionName + ": " + e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override protected Iterator<Document> inputIterator(@Nullable Object... args) throws CacheLoaderException {       

        try {
        	File inputFile = new File(filePath);

            if (filePath.toLowerCase().endsWith(".zip")) {
                processZipFile(inputFile);
            } else if (filePath.toLowerCase().endsWith(".csv")) {
                processCsvFile(inputFile);
            } else {
                System.err.println("Unsupported file format. Only .csv and .zip are supported.");
            }
        }
        catch (IOException e) {
            throw new CacheLoaderException("Failed to open the source file " + filePath, e);
        }
        
        Iterator<Document> scanner = collection.iterator();

        /**
         * Iterator for text input. The scanner is implicitly closed when there's nothing to scan.
         */
        return new Iterator<Document>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (!scanner.hasNext()) {
                    return false;
                }
                return true;
            }

            /** {@inheritDoc} */
            @Override public Document next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return scanner.next();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @Nullable @Override protected IgniteBiTuple<K, BinaryObject> parse(Document doc, @Nullable Object... args) {
    	Object key = DocumentUtil.toBinaryKey(doc.get(idField));
    	BinaryObject value = DocumentUtil.documentToBinaryObject(this.ignite.binary(), "", doc, idField);
    	return new T2<>((K)key,value);
    }


	public JdbcType[] getTypes() {
		java.sql.Types t;
		return types;
	}


	public void setTypes(JdbcType[] types) {
		this.types = types;
	}
}
