package de.kp.works.janus;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
 * 
 * 
 */

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class AbstractEntryBuilder {

	public static String KEY = "KEY";

	public static String HASH_KEY_START = "HASH_KEY_START";
	public static String HASH_KEY_END   = "HASH_KEY_END";
	
	public static String HASH_KEY = "HASH_KEY";
	public static String DEFAULT_HASH_KEY = "hk";
	
	public static String RANGE_KEY = "RANGE_KEY";
	public static String DEFAULT_RANGE_KEY = "rk";
	
	public static String RANGE_KEY_START = "RANGE_KEY_START";
	public static String RANGE_KEY_END = "RANGE_KEY_END";
	
	public static String DATA_TYPE = "DATA_TYPE";
	public static String DATA_VALUE = "DATA_VALUE";
	
	public static String BYTE_BUFFER = "BYTE_BUFFER";
	
	public static String EMPTY_VALUE = "null";

	
	/********** KEY SUPPORT *********/

	public static IgniteValue encodeRowKeyAsIgniteValue(final StaticBuffer input) {
        return new IgniteValue(encodeKeyBufferAsHexString(input, DEFAULT_HASH_KEY));
    }
    
    public static IgniteValue encodeRangeKeyAsIgniteValue(final StaticBuffer input) {
        return new IgniteValue(encodeKeyBufferAsHexString(input, DEFAULT_RANGE_KEY));
    }

    public static String encodeKeyBufferAsHexString(final StaticBuffer input, String defaultKey) {
		if (input == null || input.length() == 0) {
			return defaultKey;
	    }
	
	    final ByteBuffer buf = input.asByteBuffer();
	    final char[] chats = Hex.encodeHex(buf);	    
	    return new String(chats);	
	}    
   

    public static String _encodeKeyBufferAsString(final StaticBuffer input, String defaultKey) {
    	if (input == null || input.length() == 0) {
            return defaultKey;
        }
        final ByteBuffer buf = input.asByteBuffer();
        IgniteValue value = new IgniteValue(buf);
        byte[] data = value.data();
		return new String(data,StandardCharsets.UTF_8);
    }
 
    /********** VALUE SUPPORT **********/
 
    public static IgniteValue encodeValueAsIgniteValue(final StaticBuffer value) {
        return new IgniteValue(value.asByteBuffer());
    }

    public static StaticBuffer decodeValue(final ByteBuffer val) {
		if (null == val) return null;
		return StaticArrayBuffer.of(val);
    	
    }
    
    public static StaticBuffer decodeValue(final byte[] val) {
		if (null == val) return null;
		return StaticArrayBuffer.of(val);    	
    }
    
    public static StaticBuffer decodeValue(final IgniteValue val) {
    	if (null == val) return null;
    	return StaticArrayBuffer.of(val.data());
    
    }

    public static StaticBuffer decodeHashKey(IgniteValue key) { 
        final String value = key.getS();
        //not modfiy@byron
        return decodeKeyFromHexString(value);
    }

    public static StaticBuffer decodeKeyFromHexString(final String name) {
        try {
            return new StaticArrayBuffer(Hex.decodeHex(name));  
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }    
    }	
    
    public static StaticBuffer _decodeKeyFromString(final String name) {
    	return new StaticArrayBuffer(name.getBytes(StandardCharsets.UTF_8));    
    }	

    public static StaticBuffer decodeRangeKey(IgniteValue key) { 
        final String value = key.getS();
        //not modfiy@byron
        return decodeKeyFromHexString(value);       
    }
  
    public static StaticBuffer decodeRangeKey(String value) {
    	//not modfiy@byron
        return decodeKeyFromHexString(value);        
  
    }
   
}
