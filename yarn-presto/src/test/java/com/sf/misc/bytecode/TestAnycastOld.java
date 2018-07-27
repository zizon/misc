package com.sf.misc.bytecode;

import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.yarn.ConfigurationAware;
import com.sf.misc.yarn.rpc.UGIAware;
import com.sf.misc.yarn.rpc.YarnRMProtocol;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TestAnycastOld {

    public static final Logger LOGGER = Logger.get(TestAnycastOld.class);

    public void printClass(byte[] clazz) throws Throwable {
        ClassReader reader = new ClassReader(clazz);
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    public void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(ClassResolver.locate(clazz).get().openStream());
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    public static final class Template implements YarnRMProtocol {

        protected ConfigurationAware<YarnRMProtocolConfig> delegate;

        @Override
        public YarnRMProtocolConfig config() {
            return delegate.config();
        }

        @Override
        public UserGroupInformation ugi() {
            return null;
        }

        @Override
        public GetNewApplicationResponse getNewApplication(GetNewApplicationRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public SubmitApplicationResponse submitApplication(SubmitApplicationRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public KillApplicationResponse forceKillApplication(KillApplicationRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetQueueUserAclsInfoResponse getQueueUserAcls(GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationUpdateResponse updateReservation(ReservationUpdateRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetNodesToLabelsResponse getNodeToLabels(GetNodesToLabelsRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetLabelsToNodesResponse getLabelsToNodes(GetLabelsToNodesRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterNodeLabelsResponse getClusterNodeLabels(GetClusterNodeLabelsRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationReportResponse getApplicationReport(GetApplicationReportRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationsResponse getApplications(GetApplicationsRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationAttemptReportResponse getApplicationAttemptReport(GetApplicationAttemptReportRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationAttemptsResponse getApplicationAttempts(GetApplicationAttemptsRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetContainerReportResponse getContainerReport(GetContainerReportRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetContainersResponse getContainers(GetContainersRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetDelegationTokenResponse getDelegationToken(GetDelegationTokenRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public RenewDelegationTokenResponse renewDelegationToken(RenewDelegationTokenRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public CancelDelegationTokenResponse cancelDelegationToken(CancelDelegationTokenRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public RegisterApplicationMasterResponse registerApplicationMaster(RegisterApplicationMasterRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public FinishApplicationMasterResponse finishApplicationMaster(FinishApplicationMasterRequest request) throws YarnException, IOException {
            return null;
        }

        @Override
        public AllocateResponse allocate(AllocateRequest request) throws YarnException, IOException {
            return null;
        }
    }

    public static interface TestInterface {
        public String method(String input);
    }

    public static class GenTestInterface implements TestInterface {
        protected final TestInterface delegate = null;

        @Override
        public String method(String input) {
            if (delegate != null) {
                delegate.method(input);
            }
            return "hello";
        }
    }

    //@Test
    public void test2() throws Throwable {
        AnycastOld anycastOld = new AnycastOld();
        Object[] parameters = new Object[]{
                new GenTestInterface()
        };

        Arrays.stream(parameters).forEach(anycastOld::adopt);

        AnycastOld.GeneratedInfoBundle codes = anycastOld.bytecode(TestInterface.class, null);
        //printClass(Template.class);
        printClass(codes.bytecode);
        printClass(parameters[0].getClass());

        //LOGGER.info( BytecodeDescriptor.unparse(Template.class));

        Class<?> clazz = AnycastOld.CODEGEN_CLASS_LOADER.defineClass(codes.name, codes.bytecode);

        TestInterface protocol = TestInterface.class.cast(clazz.getConstructor(
                Arrays.stream(parameters) //
                        .map((instance) -> instance.getClass()) //
                        .sorted((left, right) -> left.getName().compareTo(right.getName()))
                        .collect(Collectors.toList()) //
                        .toArray(new Class[0]) //
        ).newInstance(parameters));
        LOGGER.info(protocol.method("yes"));
    }

    @Test
    public void test3() throws Throwable {
        LOGGER.info("" + DynamicCallSite.Static.TRACER);
    }

    //@Test
    public void test() throws Throwable {
        //Arrays.stream(YarnRMProtocol.class.getMethods()).forEach(System.out::println);
        LOGGER.info(Type.getType(TestAnycastOld.class).getInternalName());
        AnycastOld anycastOld = new AnycastOld();

        Object[] parameters = new Object[]{
                new UGIAware() {
                    @Override
                    public UserGroupInformation ugi() {
                        return (UserGroupInformation) null;
                    }
                },
                new ConfigurationAware<YarnRMProtocolConfig>() {
                    @Override
                    public YarnRMProtocolConfig config() {
                        return new YarnRMProtocolConfig();
                    }
                }
        };

        printClass(parameters[0].getClass());

        Arrays.stream(parameters).forEach(anycastOld::adopt);

        AnycastOld.GeneratedInfoBundle codes = anycastOld.bytecode(YarnRMProtocol.class, null);
        //printClass(Template.class);
        printClass(codes.bytecode);
        //printClass(parameters[1].getClass());

        //LOGGER.info( BytecodeDescriptor.unparse(Template.class));

        Class<?> clazz = AnycastOld.CODEGEN_CLASS_LOADER.defineClass(codes.name, codes.bytecode);
        YarnRMProtocol protocol = YarnRMProtocol.class.cast(clazz.getConstructor(
                Arrays.stream(parameters) //
                        .map((instance) -> instance.getClass()) //
                        .sorted((left, right) -> left.getName().compareTo(right.getName()))
                        .collect(Collectors.toList()) //
                        .toArray(new Class[0]) //
        ).newInstance(parameters[0], parameters[1]));

        LOGGER.info("reesult:" + protocol.doAS(() -> "adf"));
    }
}
