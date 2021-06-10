// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Target;
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
public class CheckTargetTable implements BoolHandler<CheckTargetTable.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        final var table = request.getTable();
        return DbComponent.build(null, null, table.getRegion())
                .checkTargetTable()
                .check(table.getName());
    }

    static class Request {
        private Resource table;

        public Resource getTable() {
            return table;
        }

        public void setTable(Resource table) {
            this.table = table;
        }
    }

    @Singleton
    static class Worker {
        private final Logger log = LoggerFactory.getLogger(getClass());

        private final AmazonDynamoDB dynamoDB;

        @Inject
        Worker(@Target AmazonDynamoDB dynamoDB) {
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
