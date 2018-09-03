package com.sf.misc.hive.udfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAesDecrypt;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;

public class UDFAESDecrypt extends GenericUDFAesDecrypt {

    public static final Log LOGGER = LogFactory.getLog(UDFAESDecrypt.class);

    protected int default_mysql_encrypt_key_length = 16;

    protected SecretKey getSecretKey(byte[] key, int keyLength) {
        key = fixKeyLengh(key, 0, keyLength, default_mysql_encrypt_key_length);

        return super.getSecretKey(key, key.length);
    }

    /**
     * walk aroud for jce restriction
     *
     * @param cipher the ciper to fix
     * @return the fixed cipher
     * @throws Exception any exception
     */
    protected Cipher fixCiper(Cipher cipher) throws Exception {
        if (Cipher.getMaxAllowedKeyLength("AES") >= 256) {
            return cipher;
        }

        Class c = Class.forName("javax.crypto.CryptoAllPermissionCollection");
        Constructor con = c.getDeclaredConstructor();
        con.setAccessible(true);
        Object allPermissionCollection = con.newInstance();
        Field f = c.getDeclaredField("all_allowed");
        f.setAccessible(true);
        f.setBoolean(allPermissionCollection, true);

        c = Class.forName("javax.crypto.CryptoPermissions");
        con = c.getDeclaredConstructor();
        con.setAccessible(true);
        Object allPermissions = con.newInstance();
        f = c.getDeclaredField("perms");
        f.setAccessible(true);
        ((Map) f.get(allPermissions)).put("*", allPermissionCollection);

        c = Class.forName("javax.crypto.JceSecurityManager");
        f = c.getDeclaredField("defaultPolicy");
        f.setAccessible(true);
        Field mf = Field.class.getDeclaredField("modifiers");
        mf.setAccessible(true);
        mf.setInt(f, f.getModifiers() & ~Modifier.FINAL);
        f.set(null, allPermissions);

        return cipher;
    }

    protected byte[] fixKeyLengh(byte[] bytes, int start, int end, int encrypt_key_length) {
        int raw_key_length = end - start;

        byte[] fixed = new byte[encrypt_key_length];
        Arrays.fill(fixed, (byte) 0x0);

        if (raw_key_length <= encrypt_key_length) {
            System.arraycopy(bytes, start, fixed, 0, raw_key_length);
            return fixed;
        }

        // raw key longer than encrypt key length.
        // cyclic xor ,see mysql-server/mysys_ssl/my_aes.cc#my_aes_create_key
        for (int read_index = 0, write_index = 0; read_index < raw_key_length; read_index++, write_index++) {
            if (write_index == encrypt_key_length) {
                write_index = 0;
            }

            fixed[write_index] ^= bytes[read_index];
        }
        return fixed;
    }
}

