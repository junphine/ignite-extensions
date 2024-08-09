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

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.util.Iterator;
import java.util.List;

public class IgniteKeyIterator implements KeyIterator {

	/* HASH_KEY (0), RANGE_KEY (1), BYTE_BUFFER (2) */
	final Iterator<List<?>> iterator;
	private FieldsQueryCursor<List<?>> curser;

	public IgniteKeyIterator(List<List<?>> entries) {
		this.curser = null;
		this.iterator = entries.iterator();
	}

	public IgniteKeyIterator(FieldsQueryCursor<List<?>> curser) {
		this.curser = curser;
		this.iterator = curser.iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public StaticBuffer next() {
		/*
		 * The key iterator provides hash keys that
		 * are from a column slice request
		 */
		List<?> result = iterator.next();
		String rowKey = (String) result.get(0);
		StaticBuffer hashKey = AbstractEntryBuilder.decodeKeyFromHexString(rowKey);

		byte[] byteBuffer = (byte[]) result.get(2);
		StaticBuffer value = AbstractEntryBuilder.decodeValue(byteBuffer);

		if (value == null)
			return StaticArrayEntry.of(hashKey).getColumn();
		else
			return StaticArrayEntry.of(hashKey, value).getColumn();
		
	}

	@Override
	public void close() {
		if(this.curser!=null) {
			this.curser.close();
			this.curser = null;
		}
	}

	@Override
	public RecordIterator<Entry> getEntries() {

		return new RecordIterator<Entry>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry next() {
				List<?> result = iterator.next();
				String rowKey = (String) result.get(0);
				StaticBuffer hashKey = AbstractEntryBuilder.decodeKeyFromHexString(rowKey);

				byte[] byteBuffer = (byte[]) result.get(2);
				StaticBuffer value = AbstractEntryBuilder.decodeValue(byteBuffer);

				if (value == null)
					return StaticArrayEntry.of(hashKey);
				else
					return StaticArrayEntry.of(hashKey, value);
			}

			@Override
			public void close() {
				IgniteKeyIterator.this.close();
			}
		};
	}

}
