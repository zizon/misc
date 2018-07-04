package com.sf.misc.yarn;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public interface YarnRMProtocol extends ApplicationMasterProtocol, ApplicationClientProtocol {
}
