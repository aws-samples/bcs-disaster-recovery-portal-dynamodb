// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.VoidHandler;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.lambda.util.Assure;
import aws.proserve.bcs.dr.util.Preconditions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

public class ConfigureStream implements VoidHandler<ConfigureStream.Request> {

    @Override
    public void handleRequest(Request request, Context context) {
        DbComponent.build(request.getProjectId(), request.getTable().getRegion(), null)
                .configureStream()
                .configure(request.getTable().getName());
    }

    static class Request {
        private Resource table;
        private String projectId;

        public Resource getTable() {
            return table;
        }

        public void setTable(Resource table) {
            this.table = table;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }
    }

    @Singleton
    static class Worker {
        private final Logger log = LoggerFactory.getLogger(getClass());

        private final AmazonDynamoDB dynamoDB;

        @Inject
        Worker(@Source AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
        }

        boolean configure(String tableName) {
            try {
                final var stream = dynamoDB.describeTable(tableName).getTable().getStreamSpecification();
                if (stream != null && stream.isStreamEnabled()) {
                    dynamoDB.updateTable(new UpdateTableRequest()
                            .withTableName(tableName)
                            .withStreamSpecification(new StreamSpecification()
                                    .withStreamEnabled(false)));

                    Assure.assure(() -> {
                        final var s = dynamoDB.describeTable(tableName).getTable().getStreamSpecification();
                        Preconditions.checkState(s == null || !s.isStreamEnabled(), "Stream is not disabled.");
                    });

                    Assure.assure(() -> {
                        final var status = dynamoDB.describeTable(tableName).getTable().getTableStatus();
                        Preconditions.checkState(status.equals("ACTIVE"), "Table status is not ACTIVE");
                    });
                }

                dynamoDB.updateTable(new UpdateTableRequest()
                        .withTableName(tableName)
                        .withStreamSpecification(new StreamSpecification()
                                .withStreamEnabled(true)
                                .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)));
                Assure.assure(() -> {
                    final var s = dynamoDB.describeTable(tableName).getTable().getStreamSpecification();
                    Preconditions.checkState(s != null && s.isStreamEnabled()
                                    && s.getStreamViewType().equals("NEW_AND_OLD_IMAGES"),
                            "Stream is not disabled.");
                });
                return true;
            } catch (Exception e) {
                log.warn("Unable to configure table.", e);
                return false;
            }
        }
    }
}
