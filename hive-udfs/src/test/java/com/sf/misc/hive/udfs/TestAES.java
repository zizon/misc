package com.sf.misc.hive.udfs;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class TestAES {
    Cipher cipher;
    UDFAESDecrypt decrypt;

    @Before
    public void init() throws Exception {
        cipher = Cipher.getInstance("AES");
        decrypt = new UDFAESDecrypt();
        cipher = decrypt.fixCiper(cipher);
    }

    public void commonTest(String encryt_key, String message, String encrypted_hex_string) throws Exception {
        byte[] key = encryt_key.getBytes();
        key = decrypt.fixKeyLengh(key, 0, key.length, cipher.getBlockSize());
        //key = decrypt.pad(key, 0, key.length, cipher.getBlockSize());
        SecretKey encrytp_key = new SecretKeySpec(key, 0, key.length, "AES");
        Assert.assertNotNull(encrytp_key);

        // test encrypt
        byte[] input = message.getBytes();
        String expected = encrypted_hex_string.toLowerCase();
        cipher.init(Cipher.ENCRYPT_MODE, encrytp_key);
        byte[] encrypetd = cipher.doFinal(input);
        Assert.assertEquals(expected, Hex.encodeHexString(encrypetd).toLowerCase());

        // test descrypt
        cipher.init(Cipher.DECRYPT_MODE, encrytp_key);
        byte[] padded = encrypetd;
        byte[] decrypted = cipher.doFinal(padded);
        Assert.assertEquals(new String(input), new String(decrypted));
    }

    @Test
    public void test() throws Exception {
        commonTest("1234567890123456", "ABC", "cba4acfb309839ba426e07d67f23564f");
        System.out.println("16 byte keys ok");
        commonTest("4eddda27-84cc-4b3e-862a-6d7750518401", "ABC", "4BD995E4B31BA7CECC03CE1838D42C3B");
        System.out.println("non 16 byte keys ok");
        commonTest("4eddda27-84cc-4b3e-862a-6d7750518401", "18825162921", "8e64d98c499bf181e90c138a58752d5f");
        System.out.println("non 16 byte keys and message ok");
        /*
        byte[] encryted = cipher.doFinal(to_encrypt_bytes);
        System.out.println(Hex.encodeHexString(encryted).toLowerCase());
        String expected_hex_encrytped = "8e64d98c499bf181e90c138a58752d5f";

        cipher.init(Cipher.DECRYPT_MODE, encrytp_key);

        int padding = cipher.getBlockSize() - input.length % cipher.getBlockSize();
        byte[] padded_input = new byte[input.length + padding];
        System.arraycopy(input, 0, padded_input, 0, input.length);
        Arrays.fill(padded_input, input.length, padded_input.length, (byte) (padding & 0xff));

        System.out.println(cipher.getBlockSize());
        System.out.println(input.length);
        byte[] decrypted = cipher.doFinal(padded_input, 0, input.length);
        System.out.println(decrypted);
        //byte[] decryped = cipher.doFinal(byte_key);
        */
        //System.out.println(new String(decryped));
    }
}
