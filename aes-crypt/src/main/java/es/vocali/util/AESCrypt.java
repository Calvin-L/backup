/*
 * Copyright 2008 Vócali Sistemas Inteligentes
 * Copyright 2019 Calvin Loncaric
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.vocali.util;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Enumeration;

/**
 * This class provides methods to encrypt and decrypt files using
 * <a href="http://www.aescrypt.com/aes_file_format.html">aescrypt file format</a>,
 * version 1 or 2.
 * <p>
 * Requires Java 6 and <a href="http://java.sun.com/javase/downloads/index.jsp">Java
 * Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files</a>.
 * <p>
 * Thread-safety and sharing: this class is not thread-safe.<br>
 * <code>AESCrypt</code> objects can be used as Commands (create, use once and dispose),
 * or reused to perform multiple operations (not concurrently though).
 *
 * <p>
 * Calvin Loncaric has made modifications to this file since its original distribution:
 * <ul>
 *   <li>Fixed misuse of {@link InputStream#read(byte[])} and
 *       {@link InputStream#skip(long)}, which may not read or skip exactly the number
 *       of bytes requested</li>
 *   <li>Allow encryption and decryption of streams where the total size is not known
 *       in advance</li>
 *   <li>Verify that the reserved byte is 0</li>
 *   <li>Documentation tweaks and fixes</li>
 * </ul>
 *
 * @author Vócali Sistemas Inteligentes
 * @author Calvin Loncaric
 */
public class AESCrypt {
	private static final String JCE_EXCEPTION_MESSAGE = "Please make sure "
		+ "\"Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files\" "
		+ "(http://java.sun.com/javase/downloads/index.jsp) is installed on your JRE.";
	private static final String RANDOM_ALG = "SHA1PRNG";
	private static final String DIGEST_ALG = "SHA-256";
	private static final String HMAC_ALG = "HmacSHA256";
	private static final String CRYPT_ALG = "AES";
	private static final String CRYPT_TRANS = "AES/CBC/NoPadding";
	private static final byte[] DEFAULT_MAC =
		{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef};
	private static final int KEY_SIZE = 32;
	private static final int BLOCK_SIZE = 16;
	private static final int SHA_SIZE = 32;

	private final boolean DEBUG;
	private byte[] password;
	private Cipher cipher;
	private Mac hmac;
	private SecureRandom random;
	private MessageDigest digest;
	private IvParameterSpec ivSpec1;
	private SecretKeySpec aesKey1;
	private IvParameterSpec ivSpec2;
	private SecretKeySpec aesKey2;


	// ------------------------------------------------------------------------
	// PRIVATE METHODS


	/**
	 * Prints a debug message on standard output if DEBUG mode is turned on.
	 */
	protected void debug(String message) {
		if (DEBUG) {
			System.out.println("[DEBUG] " + message);
		}
	}


	/**
	 * Prints a debug message on standard output if DEBUG mode is turned on.
	 */
	protected void debug(String message, byte[] bytes) {
		if (DEBUG) {
			StringBuilder buffer = new StringBuilder("[DEBUG] ");
			buffer.append(message);
			buffer.append("[");
			for (int i = 0; i < bytes.length; i++) {
				buffer.append(bytes[i]);
				buffer.append(i < bytes.length - 1 ? ", " : "]");
			}
			System.out.println(buffer.toString());
		}
	}


	/**
	 * Generates a pseudo-random byte array.
	 * @return pseudo-random byte array of <code>len</code> bytes.
	 */
	protected byte[] generateRandomBytes(int len) {
		byte[] bytes = new byte[len];
		random.nextBytes(bytes);
		return bytes;
	}


	/**
	 * SHA256 digest over given byte array and random bytes.<br>
	 * <code>bytes.length * num</code> random bytes are added to the digest.
	 * <p>
	 * The generated hash is saved back to the original byte array.<br>
	 * Maximum array size is {@link #SHA_SIZE} bytes.
	 */
	protected void digestRandomBytes(byte[] bytes, int num) {
		assert bytes.length <= SHA_SIZE;

		digest.reset();
		digest.update(bytes);
		for (int i = 0; i < num; i++) {
			random.nextBytes(bytes);
			digest.update(bytes);
		}
		System.arraycopy(digest.digest(), 0, bytes, 0, bytes.length);
	}


	/**
	 * Generates a pseudo-random IV based on time and this computer's MAC.
	 * <p>
	 * This IV is used to crypt IV 2 and AES key 2 in the file.
	 * @return IV.
	 */
	protected byte[] generateIv1() {
		byte[] iv = new byte[BLOCK_SIZE];
		long time = System.currentTimeMillis();
		byte[] mac = null;
		try {
			Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
			while (mac == null && ifaces.hasMoreElements()) {
				mac = ifaces.nextElement().getHardwareAddress();
			}
		} catch (Exception e) {
			// Ignore.
		}
		if (mac == null) {
			mac = DEFAULT_MAC;
		}

		for (int i = 0; i < 8; i++) {
			iv[i] = (byte) (time >> (i * 8));
		}
		System.arraycopy(mac, 0, iv, 8, mac.length);
		digestRandomBytes(iv, 256);
		return iv;
	}


	/**
	 * Generates an AES key starting with an IV and applying the supplied user password.
	 * <p>
	 * This AES key is used to crypt IV 2 and AES key 2.
	 * @return AES key of {@link #KEY_SIZE} bytes.
	 */
	protected byte[] generateAESKey1(byte[] iv, byte[] password) {
		byte[] aesKey = new byte[KEY_SIZE];
		System.arraycopy(iv, 0, aesKey, 0, iv.length);
		for (int i = 0; i < 8192; i++) {
			digest.reset();
			digest.update(aesKey);
			digest.update(password);
			aesKey = digest.digest();
		}
		return aesKey;
	}


	/**
	 * Generates the random IV used to crypt file contents.
	 * @return IV 2.
	 */
	protected byte[] generateIV2() {
		byte[] iv = generateRandomBytes(BLOCK_SIZE);
		digestRandomBytes(iv, 256);
		return iv;
	}


	/**
	 * Generates the random AES key used to crypt file contents.
	 * @return AES key of {@link #KEY_SIZE} bytes.
	 */
	protected byte[] generateAESKey2() {
		byte[] aesKey = generateRandomBytes(KEY_SIZE);
		digestRandomBytes(aesKey, 32);
		return aesKey;
	}

	/**
	 * Utility method to read bytes from a stream until the given array is fully filled.
	 * Returns the number of bytes actually read. If the number actually read is fewer than
	 * the length desired, then the end of the stream has been reached.
	 */
	protected static int tryRead(InputStream in, byte[] bytes) throws IOException {
		int off = 0;
		int numRead;
		while (off < bytes.length && (numRead = in.read(bytes, off, bytes.length - off)) != -1) {
			off += numRead;
		}
		return off;
	}

	/**
	 * Utility method to read bytes from a stream until the given array is fully filled.
	 * @throws IOException if the array can't be filled.
	 */
	protected static void readBytes(InputStream in, byte[] bytes) throws IOException {
		if (tryRead(in, bytes) < bytes.length) {
			throw new IOException("Unexpected end of file");
		}
	}


	// ------------------------------------------------------------------------
	// PUBLIC API


	/**
	 * Builds an object to encrypt or decrypt files with the given password.
	 * @throws GeneralSecurityException if the platform does not support the required cryptographic methods.
	 */
	public AESCrypt(String password) throws GeneralSecurityException {
		this(false, password);
	}


	/**
	 * Builds an object to encrypt or decrypt files with the given password.
	 * @throws GeneralSecurityException if the platform does not support the required cryptographic methods.
	 */
	public AESCrypt(boolean debug, String password) throws GeneralSecurityException {
		try {
			DEBUG = debug;
			setPassword(password);
			random = SecureRandom.getInstance(RANDOM_ALG);
			digest = MessageDigest.getInstance(DIGEST_ALG);
			cipher = Cipher.getInstance(CRYPT_TRANS);
			hmac = Mac.getInstance(HMAC_ALG);
		} catch (GeneralSecurityException e) {
			throw new GeneralSecurityException(JCE_EXCEPTION_MESSAGE, e);
		}
	}


	/**
	 * Changes the password this object uses to encrypt and decrypt.
	 */
	public void setPassword(String password) {
		this.password = password.getBytes(StandardCharsets.UTF_16LE);
		debug("Using password: ", this.password);
	}

	/**
	 * The input stream is encrypted and saved to the output stream.
	 * <p>
	 * <code>version</code> can be either 1 or 2.<br>
	 * None of the streams are closed.
	 * @throws IOException when there are I/O errors.
	 * @throws GeneralSecurityException if the platform does not support the required cryptographic methods.
	 */
	public void encrypt(int version, InputStream in, OutputStream out)
	throws IOException, GeneralSecurityException {
		try {
			byte[] text;

			ivSpec1 = new IvParameterSpec(generateIv1());
			aesKey1 = new SecretKeySpec(generateAESKey1(ivSpec1.getIV(), password), CRYPT_ALG);
			ivSpec2 = new IvParameterSpec(generateIV2());
			aesKey2 = new SecretKeySpec(generateAESKey2(), CRYPT_ALG);
			debug("IV1: ", ivSpec1.getIV());
			debug("AES1: ", aesKey1.getEncoded());
			debug("IV2: ", ivSpec2.getIV());
			debug("AES2: ", aesKey2.getEncoded());

			out.write("AES".getBytes(StandardCharsets.UTF_8));	// Heading.
			out.write(version);	// Version.
			out.write(0);	// Reserved.
			if (version == 2) {	// No extensions.
				out.write(0);
				out.write(0);
			}
			out.write(ivSpec1.getIV());	// Initialization Vector.

			text = new byte[BLOCK_SIZE + KEY_SIZE];
			cipher.init(Cipher.ENCRYPT_MODE, aesKey1, ivSpec1);
			cipher.update(ivSpec2.getIV(), 0, BLOCK_SIZE, text);
			cipher.doFinal(aesKey2.getEncoded(), 0, KEY_SIZE, text, BLOCK_SIZE);
			out.write(text);	// Crypted IV and key.
			debug("IV2 + AES2 ciphertext: ", text);

			hmac.init(new SecretKeySpec(aesKey1.getEncoded(), HMAC_ALG));
			text = hmac.doFinal(text);
			out.write(text);	// HMAC from previous cyphertext.
			debug("HMAC1: ", text);

			cipher.init(Cipher.ENCRYPT_MODE, aesKey2, ivSpec2);
			hmac.init(new SecretKeySpec(aesKey2.getEncoded(), HMAC_ALG));
			text = new byte[BLOCK_SIZE];
			int len, last = 0;
			while ((len = tryRead(in, text)) > 0) {
				cipher.update(text, 0, BLOCK_SIZE, text);
				hmac.update(text);
				out.write(text);	// Crypted file data block.
				last = len;
			}
			last &= 0x0f;
			out.write(last);	// Last block size mod 16.
			debug("Last block size mod 16: " + last);

			text = hmac.doFinal();
			out.write(text);	// HMAC from previous cyphertext.
			debug("HMAC2: ", text);
		} catch (InvalidKeyException e) {
			throw new GeneralSecurityException(JCE_EXCEPTION_MESSAGE, e);
		}
	}

	/**
	 * @author Calvin Loncaric
	 */
	private static class BlockWalker {
		private final PushbackInputStream in;
		private final int blockLen;
		private final int maxFooterLen;
		private final byte[] footerBuffer;
		private int footerLen;

		public BlockWalker(InputStream in, int blockLen, int maxFooterLen) throws IOException {
			this.in = new PushbackInputStream(in);
			this.blockLen = blockLen;
			this.maxFooterLen = maxFooterLen;
			footerBuffer = new byte[maxFooterLen];
			footerLen = tryRead(in, footerBuffer);
		}

		boolean nextBlock(byte[] buf) throws IOException {
			assert buf.length >= blockLen;

			// any more?
			int popped = in.read();
			if (popped < 0) {
				return false;
			}
			in.unread(popped);

			// read next block
			System.arraycopy(footerBuffer, 0, buf, 0, blockLen);
			System.arraycopy(footerBuffer, blockLen, footerBuffer, 0, footerLen - blockLen);
			footerLen -= blockLen;
			int nread;
			while (footerLen < maxFooterLen && (nread = in.read(footerBuffer, footerLen, maxFooterLen - footerLen)) >= 0) {
				footerLen += nread;
			}
			return true;
		}

		byte[] footer() {
			return footerBuffer;
		}

		int footerLen() {
			return footerLen;
		}
	}

	/**
	 * The input stream is decrypted and saved to the output stream.
	 * <p>
	 * The input file size is needed in advance.<br>
	 * The input stream can be encrypted using version 1 or 2 of aescrypt.<br>
	 * None of the streams are closed.
	 * @throws IOException when there are I/O errors.
	 * @throws GeneralSecurityException if the platform does not support the required cryptographic methods.
	 */
	public void decrypt(InputStream in, OutputStream out)
	throws IOException, GeneralSecurityException {
		try {
			byte[] text, backup;
			int version;

			text = new byte[3];
			readBytes(in, text);	// Heading.
			if (!new String(text, StandardCharsets.UTF_8).equals("AES")) {
				throw new IOException("Invalid file header");
			}

			version = in.read();	// Version.
			if (version < 1 || version > 2) {
				throw new IOException("Unsupported version number: " + version);
			}
			debug("Version: " + version);

			int reservedByte = in.read();	// Reserved.
			if (reservedByte != 0) {
				throw new IOException("Invalid reserved byte (expected 0, got " + reservedByte + ")");
			}

			if (version == 2) {	// Extensions.
				text = new byte[2];
				int len;
				do {
					readBytes(in, text);
					len = ((0xff & (int) text[0]) << 8) | (0xff & (int) text[1]);
					in.skipNBytes(len);
					debug("Skipped extension sized: " + len);
				} while (len != 0);
			}

			text = new byte[BLOCK_SIZE];
			readBytes(in, text);	// Initialization Vector.
			ivSpec1 = new IvParameterSpec(text);
			aesKey1 = new SecretKeySpec(generateAESKey1(ivSpec1.getIV(), password), CRYPT_ALG);
			debug("IV1: ", ivSpec1.getIV());
			debug("AES1: ", aesKey1.getEncoded());

			cipher.init(Cipher.DECRYPT_MODE, aesKey1, ivSpec1);
			backup = new byte[BLOCK_SIZE + KEY_SIZE];
			readBytes(in, backup);	// IV and key to decrypt file contents.
			debug("IV2 + AES2 ciphertext: ", backup);
			text = cipher.doFinal(backup);
			ivSpec2 = new IvParameterSpec(text, 0, BLOCK_SIZE);
			aesKey2 = new SecretKeySpec(text, BLOCK_SIZE, KEY_SIZE, CRYPT_ALG);
			debug("IV2: ", ivSpec2.getIV());
			debug("AES2: ", aesKey2.getEncoded());

			hmac.init(new SecretKeySpec(aesKey1.getEncoded(), HMAC_ALG));
			backup = hmac.doFinal(backup);
			text = new byte[SHA_SIZE];
			readBytes(in, text);	// HMAC and authenticity test.
			if (!Arrays.equals(backup, text)) {
				throw new IOException("Message has been altered or password incorrect");
			}
			debug("HMAC1: ", text);

			cipher.init(Cipher.DECRYPT_MODE, aesKey2, ivSpec2);
			hmac.init(new SecretKeySpec(aesKey2.getEncoded(), HMAC_ALG));
			backup = new byte[BLOCK_SIZE];
			text = new byte[BLOCK_SIZE];

			BlockWalker walker = new BlockWalker(in, BLOCK_SIZE, SHA_SIZE + 1);
			byte[] prev = null;
			while (walker.nextBlock(backup)) {
				if (prev != null) {
					cipher.update(prev, 0, BLOCK_SIZE, text);
					hmac.update(prev, 0, BLOCK_SIZE);
					out.write(text);
				}
				byte[] tmp = prev;
				prev = backup;
				backup = tmp;
				if (backup == null) {
					backup = new byte[BLOCK_SIZE];
				}
			}
			byte[] footer = walker.footer();
			int len = footer[0] > 0 ? footer[0] : BLOCK_SIZE;
			if (prev != null) {
				debug("Last block size mod 16: " + footer[0]);
				cipher.update(prev, 0, BLOCK_SIZE, text);
				hmac.update(prev, 0, BLOCK_SIZE);
				out.write(text, 0, len);
			}
			out.write(cipher.doFinal());
			backup = hmac.doFinal();
			if (walker.footerLen() != SHA_SIZE + 1) {
				throw new IOException("Unexpected end of file");
			}
			for (int i = 0; i < SHA_SIZE; ++i) {
				if (footer[i+1] != backup[i]) {
					throw new IOException("Message has been altered or password incorrect");
				}
			}
		} catch (InvalidKeyException e) {
			throw new GeneralSecurityException(JCE_EXCEPTION_MESSAGE, e);
		}
	}

}
