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

package org.apache.ignite.internal.processors.hadoop.impl;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import java.io.DataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;


import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Hadoop utility methods.
 */
public class HadoopUtils {
    /** Staging constant. */
    private static final String STAGING_CONSTANT = ".staging";
    /**
     * Constructor.
     */
    private HadoopUtils() {
        // No-op.
    }


    /**
     * Internal comparison routine.
     *
     * @param buf1 Bytes 1.
     * @param len1 Length 1.
     * @param ptr2 Pointer 2.
     * @param len2 Length 2.
     * @return Result.
     */
    @SuppressWarnings("SuspiciousNameCombination")
    public static int compareBytes(byte[] buf1, int len1, long ptr2, int len2) {
        int minLength = Math.min(len1, len2);

        int minWords = minLength / Longs.BYTES;

        for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
            long lw = GridUnsafe.getLong(buf1, GridUnsafe.BYTE_ARR_OFF + i);
            long rw = GridUnsafe.getLong(ptr2 + i);

            long diff = lw ^ rw;

            if (diff != 0) {
                if (GridUnsafe.BIG_ENDIAN)
                    return (lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE) ? -1 : 1;

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;

                if (x == 0) {
                    x = (int) (diff >>> 32);

                    n = 32;
                }

                y = x << 16;

                if (y == 0)
                    n += 16;
                else
                    x = y;

                y = x << 8;

                if (y == 0)
                    n += 8;

                return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * Longs.BYTES; i < minLength; i++) {
            int res = UnsignedBytes.compare(buf1[i], GridUnsafe.getByte(ptr2 + i));

            if (res != 0)
                return res;
        }

        return len1 - len2;
    }

    /**
     * Deserialization of Hadoop Writable object.
     *
     * @param writable Writable object to deserialize to.
     * @param bytes byte array to deserialize.
     */
    public static void deserialize(Writable writable, byte[] bytes) throws IOException {
        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes));

        writable.readFields(dataIn);

        dataIn.close();
    }

    /**
     * Create UserGroupInformation for specified user and credentials.
     *
     * @param user User.
     * @param credentialsBytes Credentials byte array.
     */
    public static UserGroupInformation createUGI(String user, byte[] credentialsBytes) throws IOException {
        Credentials credentials = new Credentials();

        HadoopUtils.deserialize(credentials, credentialsBytes);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);

        ugi.addCredentials(credentials);

        if (credentials.numberOfTokens() > 0)
            ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);

        return ugi;
    }
}