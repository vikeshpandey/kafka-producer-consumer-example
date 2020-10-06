package io.github.vikeshpandey;

import com.amazonaws.services.kafka.AWSKafkaAsync;
import com.amazonaws.services.kafka.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MSKCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSKCluster.class);

    private final AWSKafkaAsync awsKafkaAsyncClient;

    public MSKCluster(AWSKafkaAsync awsKafkaAsyncClient) {
        this.awsKafkaAsyncClient = awsKafkaAsyncClient;
    }

    public String createMSKCluster() throws ExecutionException, InterruptedException {
        final CreateClusterRequest createClusterRequest = createClusterRequest();
        LOGGER.info("Submitting create clusterAsync request..");
        Future<CreateClusterResult> clusterAsync = awsKafkaAsyncClient.createClusterAsync(createClusterRequest);

        CreateClusterResult createClusterResult = clusterAsync.get();
        LOGGER.info("Create Cluster request submitted successfully, current state of cluster is : {}", createClusterResult.getState());

        System.out.print("Waiting for the cluster to become ACTIVE..");

        String clusterARN = createClusterResult.getClusterArn();
        DescribeClusterResult describeClusterResult = getDescribeClusterResult(clusterARN);


        while (!getDescribeClusterResult(clusterARN).getClusterInfo().getState().equalsIgnoreCase(ClusterState.ACTIVE.name())) {
            System.out.print(".");
            Thread.sleep(60000);
            LOGGER.info("now state...{}", createClusterResult.getState());
            LOGGER.info("enum is :{}", ClusterState.ACTIVE.name());
        }
        LOGGER.info("Cluster ACTIVE now, ARN is : {}", clusterARN);
        return clusterARN;
    }

    private CreateClusterRequest createClusterRequest() {
        CreateClusterRequest createClusterRequest = new CreateClusterRequest();
        createClusterRequest.setClusterName("demo-msk-cluster");
        BrokerNodeGroupInfo brokerNodeGroupInfo = getBrokerNodeGroupInfo();
        createClusterRequest.setBrokerNodeGroupInfo(brokerNodeGroupInfo);

        EncryptionInfo encryptionInfo = getEncryptionInfo();
        createClusterRequest.setEncryptionInfo(encryptionInfo);

        createClusterRequest.setEnhancedMonitoring(EnhancedMonitoring.PER_TOPIC_PER_BROKER.toString());

        createClusterRequest.setKafkaVersion("2.2.1");
        createClusterRequest.setNumberOfBrokerNodes(3);
        return createClusterRequest;
    }

    private static BrokerNodeGroupInfo getBrokerNodeGroupInfo() {
        final BrokerNodeGroupInfo brokerNodeGroupInfo = new BrokerNodeGroupInfo();
        brokerNodeGroupInfo.setBrokerAZDistribution("DEFAULT");
        brokerNodeGroupInfo.setClientSubnets(Stream.of("subnet-066c575f838456b50", "subnet-0c20542e23ec4727b", "subnet-032c1c8f852d07988").collect(Collectors.toList()));
        brokerNodeGroupInfo.setInstanceType("kafka.m5.large");
        brokerNodeGroupInfo.setSecurityGroups(Collections.singletonList("sg-0d213fad443bc8a12"));
        return brokerNodeGroupInfo;
    }

    private static EncryptionInfo getEncryptionInfo() {
        final EncryptionInTransit encryptionInTransit = new EncryptionInTransit();
        encryptionInTransit.setInCluster(true);
        encryptionInTransit.setClientBroker("TLS_PLAINTEXT");

        final EncryptionInfo encryptionInfo = new EncryptionInfo();
        encryptionInfo.setEncryptionInTransit(encryptionInTransit);

        return encryptionInfo;
    }

    public DescribeClusterResult getDescribeClusterResult(String clusterARN) throws InterruptedException, ExecutionException {
        DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest();
        describeClusterRequest.setClusterArn(clusterARN);
        Future<DescribeClusterResult> describeClusterResultFuture = awsKafkaAsyncClient.describeClusterAsync(describeClusterRequest);
        return describeClusterResultFuture.get();
    }

    public AWSKafkaAsync getAwsKafkaAsyncClient() {
        return awsKafkaAsyncClient;
    }
}
