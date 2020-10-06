package io.github.vikeshpandey;

import com.amazonaws.services.kafka.*;
import com.amazonaws.services.kafka.model.*;
import com.amazonaws.services.kafka.model.DescribeClusterResult;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Run {

    private static final Logger LOGGER = LoggerFactory.getLogger(Run.class);


    private static Properties loadProperties(final String propertiesFileName) throws IOException {
        String rootPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).getPath();
        Properties properties = new Properties();
        properties.load(new FileInputStream(rootPath + propertiesFileName));
        return properties;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
//        if (args.length == 0) {
//            LOGGER.info("please provide the name of the topic");
//            System.exit(-1);
//        } else if (args.length > 1) {
//            LOGGER.info("incorrect number of command-line arguments supplied, please only supply the {topic-name}");
//            System.exit(-1);
//        }
//        final String topicName = args[0];
        final MSKCluster mskCluster = new MSKCluster(AWSKafkaAsyncClientBuilder.standard().build());
       // final String clusterARN = mskCluster.createMSKCluster();
        final AWSKafkaAsync awsKafkaAsyncClient = mskCluster.getAwsKafkaAsyncClient();


//        String zookeeperConnectionString = getZookeeperConnectionString(clusterARN, awsKafkaAsyncClient);


        final String bootstrapBrokerString = getBootstrapBrokerString("arn:aws:kafka:us-east-1:018632230441:cluster/demo-msk-cluster/4261f4c3-bf94-4475-b10e-ad38257039ff-4", awsKafkaAsyncClient);


        System.out.println("bootstrapBrokerString......."+bootstrapBrokerString);
//        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-2.demo-msk-cluster.wltt32.c4.kafka.us-east-1.amazonaws.com:9092");
//        AdminClient kafkaAdminClient = KafkaAdminClient.create(properties);
//
//        CreateTopicsResult result = kafkaAdminClient.createTopics(
//                Stream.of("foo", "bar", "baz").map(
//                        name -> new NewTopic(name, 3, (short) 1)
//                ).collect(Collectors.toList())
//        );
//        result.all().get();
 //       System.out.println("done.......");






        final Properties producerProperties = loadProperties("producer.properties");

        final MSKProducer mskProducer = new MSKProducer(producerProperties);



        //send a sample message to the topic
        LOGGER.info("test loggerrrrrrrrr");
        mskProducer.sendMessage("AWSKafkaTutorialTopic");

        //consumer consumes the message
        final Properties consumerProperties = loadProperties("consumer.properties");
        final MSKConsumer mskConsumer = new MSKConsumer(consumerProperties);
        mskConsumer.subscribeToTopic("AWSKafkaTutorialTopic");
        mskConsumer.consumeMessage("AWSKafkaTutorialTopic");


    }

    private static String getBootstrapBrokerString(String clusterARN, AWSKafkaAsync awsKafkaAsyncClient) throws InterruptedException, ExecutionException {
        GetBootstrapBrokersRequest getBootstrapBrokersRequest = new GetBootstrapBrokersRequest();
        getBootstrapBrokersRequest.setClusterArn(clusterARN);
        Future<GetBootstrapBrokersResult> bootstrapBrokersAsync = awsKafkaAsyncClient.getBootstrapBrokersAsync(getBootstrapBrokersRequest);
        GetBootstrapBrokersResult getBootstrapBrokersResult = bootstrapBrokersAsync.get();
        return getBootstrapBrokersResult.getBootstrapBrokerString();
    }

//    private static String getZookeeperConnectionString(String clusterARN, MSKCluster mskCluster) throws InterruptedException, ExecutionException {
//        DescribeClusterResult describeClusterResult = mskCluster.getDescribeClusterResult(clusterARN);
//        return describeClusterResult.getClusterInfo().getZookeeperConnectString();
//    }




}
