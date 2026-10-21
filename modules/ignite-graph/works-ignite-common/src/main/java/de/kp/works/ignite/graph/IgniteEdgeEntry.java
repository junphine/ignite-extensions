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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;

import static de.kp.works.ignite.ValueUtils.convertToBigDecimal;

public class IgniteEdgeEntry {
    
    public String id; // edge_id

    public String label;
    public String toId;

    public String toIdType; // to node label

    public String fromId;

    public String fromIdType; // from node label
    public String propKey;
    public Object propValue;
    public short propType;
    public float weight = 1.0f;

    public long createdAt;
    public long updatedAt;

    public IgniteEdgeEntry(           
            String id,
            String label,
            String toId,
            String toIdType,
            String fromId,
            String fromIdType,
            long createdAt,
            long updatedAt,
            String propKey,
            short propType,
            Object propValue) {    	
      
        this.id = id;
        this.label = label;
        this.toId = toId;
        this.toIdType = toIdType;
        this.fromId = fromId;
        this.fromIdType = fromIdType;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.propKey   = propKey;
        this.propType  = propType;
        this.propValue = propValue;

    }
    
    public final String getCacheKey() {
    	return id + ':' + propKey;
    }

    public final BigDecimal getDecimalValue() {
        return convertToBigDecimal(propValue);
    }
}
