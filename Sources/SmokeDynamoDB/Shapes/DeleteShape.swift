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
//  DeleteShape.swift
//  SmokeDynamoDB
//

import DynamoDBModel

internal protocol DeleteShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         key: Key,
         tableName: TableName)
}

extension DeleteShape {
    
    static func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                      targetTableName: TableName) throws -> Self {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if let keyAttributes = attributeValue.M {
            return Self(conditionExpression: nil,
                        expressionAttributeNames: nil,
                        expressionAttributeValues: nil,
                        key: keyAttributes,
                        tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
    }
    
    static func getInputForDeleteItem<AttributesType, ItemType>(
            existingItem: TypedDatabaseItem<AttributesType, ItemType>,
            targetTableName: TableName) throws -> Self {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)
        
        guard let keyAttributes = attributeValue.M else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }

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
                    key: keyAttributes,
                    tableName: targetTableName)
    }
}

extension DynamoDBModel.DeleteItemInput: DeleteShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         key: Key,
         tableName: TableName) {
        self.init(conditionExpression: conditionExpression,
                  conditionalOperator: nil,
                  expected: nil,
                  expressionAttributeNames: expressionAttributeNames,
                  expressionAttributeValues: expressionAttributeValues,
                  key: key,
                  returnConsumedCapacity: nil,
                  returnItemCollectionMetrics:  nil,
                  returnValues: nil,
                  tableName: tableName)
    }
}

extension DynamoDBModel.Delete: DeleteShape {
    init(conditionExpression: ConditionExpression?,
         expressionAttributeNames: ExpressionAttributeNameMap?,
         expressionAttributeValues: ExpressionAttributeValueMap?,
         key: Key,
         tableName: TableName) {
        self.init(conditionExpression: conditionExpression,
                  expressionAttributeNames: expressionAttributeNames,
                  expressionAttributeValues: expressionAttributeValues,
                  key: key,
                  returnValuesOnConditionCheckFailure: nil,
                  tableName: tableName)
    }
}
