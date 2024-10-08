package de.kp.works.ignite.graph;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

public class IgniteVertexEntry {    

    public String id;
    public String idType;
    public String label;
    public Long createdAt;
    public Long updatedAt;
    public String propKey;
    public String propType;
    public Object propValue;

    public IgniteVertexEntry(           
            String id,
            String idType,
            String label,
            Long createdAt,
            Long updatedAt,
            String propKey,
            String propType,
            Object propValue) {
    	
        this.id = id;
        this.idType = idType;

        this.label = label;

        this.createdAt = createdAt;
        this.updatedAt = updatedAt;

        this.propKey   = propKey;
        this.propType  = propType;
        this.propValue = propValue;
    }    
    
    public final String getCacheKey() {
    	// String cacheKey = UUID.randomUUID().toString();
    	return id + ':' + propKey;
    }

}

