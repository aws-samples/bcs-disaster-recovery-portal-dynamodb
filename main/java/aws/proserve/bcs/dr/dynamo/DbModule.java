// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.secret.Credential;
import com.amazonaws.jmespath.ObjectMapperSingleton;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;

import javax.annotation.Nullable;
import javax.inject.Singleton;

@Singleton
@Module
class DbModule {

    @Provides
    @Singleton
    @Source
    static AmazonDynamoDB sourceDynamoDB(
            @Nullable @Source String region,
            @Nullable @Source Credential credential) {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(region)
                .withCredentials(Credential.toProvider(credential))
                .build();
    }

    @Provides
    @Singleton
    @Target
    static AmazonDynamoDB targetDynamoDB(
            @Nullable @Target String region) {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(region)
                .build();
    }

    @Provides
    @Singleton
    ObjectMapper objectMapper() {
        return ObjectMapperSingleton.getObjectMapper();
    }

    @Provides
    @Singleton
    AWSSecretsManager secretsManager() {
        return AWSSecretsManagerClientBuilder.defaultClient();
    }
}
