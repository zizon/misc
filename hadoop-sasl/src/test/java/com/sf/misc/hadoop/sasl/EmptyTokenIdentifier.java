package com.sf.misc.hadoop.sasl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmptyTokenIdentifier extends TokenIdentifier {

    public static final Text TOKEN_KIND = new Text("empty token");

    @Override
    public Text getKind() {
        return TOKEN_KIND;
    }

    @Override
    public UserGroupInformation getUser() {
        return UserGroupInformation.createRemoteUser("empty token user");
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
}
