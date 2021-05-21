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
//  InMemoryDynamoDBWriteTransaction.swift
//  SmokeDynamoDB
//

import NIO

internal enum TransactionRequest {
    case insertItem(item: PolymorphicOperationReturnTypeConvertable)
    case clobberItem(item: PolymorphicOperationReturnTypeConvertable)
    case updateItem(newItem: PolymorphicOperationReturnTypeConvertable,
                    existingItem: PolymorphicOperationReturnTypeConvertable)
    case deleteItemAtKey(partitionKey: String, sortKey: String)
    case deleteItem(item: PolymorphicOperationReturnTypeConvertable)
}

public class InMemoryDynamoDBWriteTransaction: DynamoDBWriteTransaction {
    internal var transactItems: [TransactionRequest]
    private let executeHandler: ([TransactionRequest]) -> EventLoopFuture<Void>
    
    internal init(executeHandler: @escaping ([TransactionRequest]) -> EventLoopFuture<Void>) {
        self.transactItems = []
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
                        
        transactItems.append(.insertItem(item: item))
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        transactItems.append(.clobberItem(item: item))
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        transactItems.append(.updateItem(newItem: newItem, existingItem: existingItem))
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws
            where AttributesType: PrimaryKeyAttributes {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        transactItems.append(.deleteItemAtKey(partitionKey: key.partitionKey, sortKey: key.sortKey))
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        guard self.count < dynamoDBWriteTransactionMaximumRequests else {
            throw DynamoDBWriteTransactionError.writeTransactionAtMaxRequests
        }
        
        transactItems.append(.deleteItem(item: existingItem))
    }
}
