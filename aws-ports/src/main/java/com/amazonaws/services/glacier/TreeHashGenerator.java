/*
 * Copyright 2012-2022 Amazon Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.glacier;

import com.amazonaws.services.glacier.internal.TreeHashInputStream;
import software.amazon.awssdk.utils.BinaryUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for calculating tree hashes.
 * <p>
 * For more information on tree hashing, see
 * <a href="http://en.wikipedia.org/wiki/Hash_tree">http://en.wikipedia.org/wiki/Hash_tree</a>.
 */
public class TreeHashGenerator {

    /**
     * Calculates a hex encoded binary hash using a tree hashing algorithm for
     * the data in the specified file.
     *
     * @param file
     *            The file containing the data to hash.
     *
     * @return The hex encoded binary tree hash for the data in the specified
     *         file.
     */
    public static String calculateTreeHash(File file) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            return calculateTreeHash(in);
        }
    }

    /**
     * Calculates a hex encoded binary hash using a tree hashing algorithm for
     * the data in the specified input stream. The method will consume all the
     * inputStream and close it when returned.
     *
     * @param input
     *            The input stream containing the data to hash.
     *
     * @return The hex encoded binary tree hash for the data in the specified
     *         input stream.
     */
    public static String calculateTreeHash(InputStream input) throws IOException {
        byte[] buffer = new byte[1024];
        TreeHashInputStream treeHashInputStream =
            new TreeHashInputStream(input);
        try {
            while (treeHashInputStream.read(buffer, 0, buffer.length) != -1) ;
        } finally {
            // closing is currently required to compute the checksum
            treeHashInputStream.close();
        }
        return calculateTreeHash(treeHashInputStream.getChecksums());
    }

    /**
     * Returns the hex encoded binary tree hash for the individual checksums
     * given. The sums are assumed to have been generated from sequential 1MB
     * portions of a larger file, with the possible exception of the last part,
     * which may be less than a full MB.
     *
     * @return The combined hex encoded binary tree hash for the individual
     *         checksums specified.
     */
    public static String calculateTreeHash(List<byte[]> checksums) {

        /*
         * The tree hash algorithm involves concatenating adjacent pairs of
         * individual checksums, then taking the checksum of the resulting bytes
         * and storing it, then recursing on this new list until there is only
         * one element. Any final odd-numbered parts at each step are carried
         * over to the next iteration as-is.
         */
        List<byte[]> hashes = new ArrayList<byte[]>();
        hashes.addAll(checksums);
        while ( hashes.size() > 1 ) {
            List<byte[]> treeHashes = new ArrayList<byte[]>();
            for ( int i = 0; i < hashes.size() / 2; i++ ) {
                byte[] firstPart = hashes.get(2 * i);
                byte[] secondPart = hashes.get(2 * i + 1);
                byte[] concatenation = new byte[firstPart.length + secondPart.length];
                System.arraycopy(firstPart, 0, concatenation, 0, firstPart.length);
                System.arraycopy(secondPart, 0, concatenation, firstPart.length, secondPart.length);
                treeHashes.add(computeSHA256Hash(concatenation));
            }
            if ( hashes.size() % 2 == 1 ) {
                treeHashes.add(hashes.get(hashes.size() - 1));
            }
            hashes = treeHashes;
        }

        return BinaryUtils.toHex(hashes.get(0));
    }

    public static MessageDigest newSHA256DigestInstance() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not support SHA-256");
        }
    }

    private static byte[] computeSHA256Hash(byte[] data) {
        MessageDigest messageDigest = newSHA256DigestInstance();
        messageDigest.update(data);
        return messageDigest.digest();
    }
}
