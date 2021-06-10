// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.secret.Credential;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Checks if the schema of two tables match.
 */
public class CheckSchema implements BoolHandler<CheckSchema.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return DbComponent.build(
                request.getProjectId(),
                request.getSource().getRegion(),
                request.getTarget().getRegion())
                .checkSchema()
                .check(request.getSource().getName(), request.getTarget().getName());
    }

    static class Request {
        private Resource source;
        private Resource target;
        private String projectId;

        public Resource getSource() {
            return source;
        }

        public void setSource(Resource source) {
            this.source = source;
        }

        public Resource getTarget() {
            return target;
        }

        public void setTarget(Resource target) {
            this.target = target;
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

        private final AmazonDynamoDB sourceDynamo;
        private final AmazonDynamoDB targetDynamo;

        @Inject
        Worker(@Source AmazonDynamoDB sourceDynamo,
               @Target AmazonDynamoDB targetDynamo) {
            this.sourceDynamo = sourceDynamo;
            this.targetDynamo = targetDynamo;
        }

        boolean check(String sourceTable, String targetTable) {
            final var source = sourceDynamo.describeTable(sourceTable).getTable();
            final var target = targetDynamo.describeTable(targetTable).getTable();

            final var sKey = source.getKeySchema();
            final var tKey = target.getKeySchema();
            if (sKey.size() != tKey.size()) {
                log.info("Size of key schema list mismatches");
                return false;
            }
            for (int i = 0; i < sKey.size(); i++) {
                if (!sKey.get(i).getAttributeName().equals(tKey.get(i).getAttributeName())
                        || !sKey.get(i).getKeyType().equals(tKey.get(i).getKeyType())) {
                    log.info("{} does not match {}", sKey.get(i), tKey.get(i));
                    return false;
                }
            }
            final var sAttr = source.getAttributeDefinitions();
            final var tAttr = target.getAttributeDefinitions();
            for (int i = 0; i < sAttr.size(); i++) {
                if (!sAttr.get(i).getAttributeName().equals(tAttr.get(i).getAttributeName())
                        || !sAttr.get(i).getAttributeType().equals(tAttr.get(i).getAttributeType())) {
                    log.info("{} does not match {}", sAttr.get(i), tAttr.get(i));
                    return false;
                }
            }

            return true;
        }
    }
}
