// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.Context;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Checks if the table has enabled stream properly.
 */
public class CheckStream implements BoolHandler<CheckStream.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return DbComponent.build(request.getProjectId(), request.getTable().getRegion(), null)
                .checkStream()
                .check(request.getTable().getName());
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
        private final AmazonDynamoDB dynamoDB;

        @Inject
        Worker(@Source AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
        }

        boolean check(String tableName) {
            final var stream = dynamoDB.describeTable(tableName).getTable().getStreamSpecification();
            if (stream == null) {
                return false;
            }

            final var viewType = stream.getStreamViewType();
            return stream.isStreamEnabled()
                    && (viewType.equals("NEW_IMAGE") || viewType.equals("NEW_AND_OLD_IMAGES"));
        }
    }
}
