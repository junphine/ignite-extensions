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

import org.apache.ignite.transactions.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
/*
 * This class is introduced for future (additional) transaction support
 */
public class IgniteStoreTransaction extends AbstractStoreTransaction {
	
	final Transaction t;
	final boolean topLeverl;

	public IgniteStoreTransaction(Transaction t, boolean isCreate, BaseTransactionConfig config) {
		super(config);
		this.t = t;
		this.topLeverl = isCreate;
	}

	@Override
	public void commit() throws BackendException {
		if(topLeverl) {
			t.commit();
		}		
	}

	@Override
	public void rollback() throws BackendException {
		if(topLeverl) {
			t.rollback();
		}
		else {
			t.setRollbackOnly();
		}
	}

}
