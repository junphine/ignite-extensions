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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

import static de.kp.works.ignite.ValueUtils.convertToBigDecimal;

public class IgniteVertexEntry {

    public String id; // vertex id VERTEX_ID_COL_NAME

    public String label;

    public String propKey;
    public Object propValue;

    public short idType;
    public short propType;

    public long createdAt;
    public long updatedAt;

    public IgniteVertexEntry(           
            String id,
            short idType,
            String label,
            long createdAt,
            long updatedAt,
            String propKey,
            short propType,
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

    public final BigDecimal getDecimalValue() {
        return convertToBigDecimal(propValue);
    }

}

