// Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//  AWSDynamoDBKeysProjection.swift
//  SmokeDynamoDB
//

import Foundation
import Logging
import DynamoDBClient
import DynamoDBModel
import SmokeAWSCore
import SmokeHTTPClient
import AsyncHTTPClient

@available(swift, deprecated: 2.0, renamed: "AWSDynamoDBCompositePrimaryKeysProjection")
public class AWSDynamoDBKeysProjection<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBKeysProjection {
    internal let dynamodb: AWSDynamoDBClient<InvocationReportingType>
    internal let targetTableName: String
    internal let logger: Logger

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }

    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)

        self.logger = reporting.logger
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: staticCredentials,
                                          awsRegion: region, reporting: reporting,
                                          endpointHostName: endpointHostName,
                                          endpointPort: endpointPort, requiresTLS: requiresTLS,
                                          eventLoopProvider: eventLoopProvider)
        self.targetTableName = tableName

        self.logger.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }

    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew) {
        self.logger = reporting.logger
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                          awsRegion: region, reporting: reporting,
                                          endpointHostName: endpointHostName,
                                          endpointPort: endpointPort, requiresTLS: requiresTLS,
                                          eventLoopProvider: eventLoopProvider)
        self.targetTableName = tableName

        self.logger.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    internal init(dynamodb: AWSDynamoDBClient<InvocationReportingType>,
                  targetTableName: String,
                  logger: Logger) {
        self.dynamodb = dynamodb
        self.targetTableName = targetTableName
        self.logger = logger
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times.
     */
    public func close() throws {
        try dynamodb.close()
    }
}
