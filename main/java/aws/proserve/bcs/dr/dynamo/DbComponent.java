// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.secret.Credential;
import aws.proserve.bcs.dr.secret.SecretManager;
import dagger.BindsInstance;
import dagger.Component;

import javax.annotation.Nullable;
import javax.inject.Singleton;

@Singleton
@Component(modules = DbModule.class)
interface DbComponent {

    static DbComponent build(String projectId, String source, String target) {
        return DaggerDbComponent.builder()
                .sourceRegion(source)
                .targetRegion(target)
                .sourceCredential(projectId == null ? null :
                        DaggerDbComponent.builder()
                                .build()
                                .secretManager()
                                .getCredentialByProject(projectId))
                .build();
    }

    SecretManager secretManager();

    CheckSourceTable.Worker checkSourceTable();

    CheckTargetTable.Worker checkTargetTable();

    CheckSchema.Worker checkSchema();

    CheckStream.Worker checkStream();

    ConfigureStream.Worker configureStream();

    @Component.Builder
    interface Builder {

        @BindsInstance
        Builder sourceRegion(@Nullable @Source String region);

        @BindsInstance
        Builder targetRegion(@Nullable @Target String region);

        @BindsInstance
        Builder sourceCredential(@Nullable @Source Credential credential);

        DbComponent build();
    }
}
