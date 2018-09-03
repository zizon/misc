package com.sf.misc.hadoop.sasl;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SaslTokenIdentifier extends TokenIdentifier {

    public static final Text TOKEN_KIND = new Text("sasl token");

    protected static final String SIGNATURE = "_signature_";
    protected static final Charset UTF8 = Charset.forName("UTF8");
    protected static final MessageDigest DIGESTOR;

    static {
        MessageDigest selected = null;
        try {
            selected = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("no digest found", e);
        } finally {
            DIGESTOR = selected;
        }
    }

    protected Map<String, String> context;

    public SaslTokenIdentifier() {
        context = Maps.newConcurrentMap();
    }

    public SaslTokenIdentifier(Map<String, String> context) {
        this();
        this.context.putAll(context);
    }

    public boolean integrityCheck() {
        String provided = this.context.getOrDefault(SIGNATURE, null);
        if (provided == null) {
            return false;
        }

        // compute
        String digested = digest();

        // compare
        return provided.equals(digested);
    }

    public SaslTokenIdentifier sign() {
        this.context.put(SIGNATURE, digest());
        return this;
    }

    protected String digest() {
        String digest_source = this.context.entrySet().parallelStream()
                // exclude signature
                .filter((entry) -> !entry.getKey().equals(SIGNATURE))
                // sort by key
                .sorted(Map.Entry.comparingByKey())
                .map((entry) -> {
                    return entry.getKey() + "=" + entry.getValue();
                })
                .collect(Collectors.joining("\n"));

        return new String(DIGESTOR.digest(digest_source.getBytes(UTF8)), UTF8);
    }

    @Override
    public Text getKind() {
        return TOKEN_KIND;
    }

    @Override
    public UserGroupInformation getUser() {
        return UserGroupInformation.createProxyUser(this.context.get("user"), UserGroupInformation.createRemoteUser(this.context.get("real")));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = new Gson().toJson( //
                this.context
        ).getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int lenght = in.readInt();
        byte[] raw = new byte[lenght];
        in.readFully(raw);
        this.context = new Gson().fromJson(new String(raw), Map.class);
    }
}
