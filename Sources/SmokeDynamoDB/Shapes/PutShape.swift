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
//  PutShape.swift
//  SmokeDynamoDB
//

import DynamoDBModel

internal protocol PutShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         item: PutItemInputAttributeMap,
         tableName: TableName)
}

extension PutShape {
    
    static func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                            targetTableName: TableName) throws -> Self {
        let attributes = try DynamoDBModel.AttributeValue.getAttributes(forItem: item)

        let expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
        let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

        return Self(conditionExpression: conditionExpression,
                    expressionAttributeNames: expressionAttributeNames,
                    expressionAttributeValues: nil,
                    item: attributes,
                    tableName: targetTableName)
    }
    
    static func getInputForUpdateItem<AttributesType, ItemType>(
            newItem: TypedDatabaseItem<AttributesType, ItemType>,
            existingItem: TypedDatabaseItem<AttributesType, ItemType>,
            targetTableName: TableName) throws -> Self {
        let attributes = try DynamoDBModel.AttributeValue.getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBModel.AttributeValue(N: String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBModel.AttributeValue(S: existingItem.createDate.iso8601)]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return Self(conditionExpression: conditionExpression,
                    expressionAttributeNames: expressionAttributeNames,
                    expressionAttributeValues: expressionAttributeValues,
                    item: attributes,
                    tableName: targetTableName)
    }
}

extension DynamoDBModel.PutItemInput: PutShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         item: PutItemInputAttributeMap,
         tableName: TableName) {
        self.init(conditionExpression: conditionExpression,
                  conditionalOperator: nil,
                  expected: nil,
                  expressionAttributeNames: expressionAttributeNames,
                  expressionAttributeValues: expressionAttributeValues,
                  item: item,
                  returnConsumedCapacity: nil,
                  returnItemCollectionMetrics: nil,
                  returnValues: nil,
                  tableName: tableName)
    }
}

extension DynamoDBModel.Put: PutShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         item: PutItemInputAttributeMap,
         tableName: TableName) {
        self.init(conditionExpression: conditionExpression,
                  expressionAttributeNames: expressionAttributeNames,
                  expressionAttributeValues: expressionAttributeValues,
                  item: item,
                  returnValuesOnConditionCheckFailure: nil,
                  tableName: tableName)
    }
}
