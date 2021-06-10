// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Checks if the table exists.
 */
public class CheckSourceTable implements BoolHandler<CheckSourceTable.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        final var table = request.getTable();
        return DbComponent.build(request.getProjectId(), table.getRegion(), null)
                .checkSourceTable()
                .check(table.getName());
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

        boolean check(String tableName) {
            try {
                dynamoDB.describeTable(tableName);
                return true;
            } catch (Exception e) {
                log.warn("Unable to describe table.", e);
                return false;
            }
        }
    }
}
