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
//  AWSDynamoDBWriteTransaction.swift
//  SmokeDynamoDB
//

import DynamoDBModel
import NIO

public class AWSDynamoDBWriteTransaction: DynamoDBWriteTransaction {
    private var transactItems: TransactWriteItemList
    private let targetTableName: TableName
    private let executeHandler: (TransactWriteItemList) -> EventLoopFuture<Void>
    
    internal init(targetTableName: TableName, executeHandler: @escaping (TransactWriteItemList) -> EventLoopFuture<Void>) {
        self.transactItems = []
        self.targetTableName = targetTableName
        self.executeHandler = executeHandler
    }
    
    public var count: Int {
        return self.transactItems.count
    }
    
    public func execute() -> EventLoopFuture<Void> {
        return self.executeHandler(self.transactItems)
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        let put = try DynamoDBModel.Put.getInputForInsert(item, targetTableName: self.targetTableName)
        
        let transactWriteItem = TransactWriteItem(conditionCheck: nil, delete: nil, put: put, update: nil)
        
        transactItems.append(transactWriteItem)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        let attributes = try DynamoDBModel.AttributeValue.getAttributes(forItem: item)
        
        let put = DynamoDBModel.Put(item: attributes,
                                    tableName: targetTableName)
        
        let transactWriteItem = TransactWriteItem(conditionCheck: nil, delete: nil, put: put, update: nil)
        
        transactItems.append(transactWriteItem)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        let put = try DynamoDBModel.Put.getInputForUpdateItem(newItem: newItem,
                                                              existingItem: existingItem,
                                                              targetTableName: self.targetTableName)
        
        let transactWriteItem = TransactWriteItem(conditionCheck: nil, delete: nil, put: put, update: nil)
        
        transactItems.append(transactWriteItem)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws
            where AttributesType: PrimaryKeyAttributes {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        let delete = try DynamoDBModel.Delete.getInputForDeleteItem(forKey: key,
                                                                    targetTableName: self.targetTableName)
        
        let transactWriteItem = TransactWriteItem(conditionCheck: nil, delete: delete, put: nil, update: nil)
        
        transactItems.append(transactWriteItem)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        let delete = try DynamoDBModel.Delete.getInputForDeleteItem(existingItem: existingItem,
                                                                    targetTableName: self.targetTableName)
        
        let transactWriteItem = TransactWriteItem(conditionCheck: nil, delete: delete, put: nil, update: nil)
        
        transactItems.append(transactWriteItem)
    }
}
