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
//  DynamoDBWriteTransaction.swift
//  SmokeDynamoDB
//

import DynamoDBModel
import NIO

public let dynamoDBWriteTransactionMaximumRequests = 25

public enum DynamoDBWriteTransactionError: Error {
    case writeTransactionAtMaxRequests
}

/**
  A protocol that represents a write transaction that is being built up.
 */
public protocol DynamoDBWriteTransaction {
    /// The number of requests currented added to this transaction.
    var count: Int { get }
    
    /// Executes this transaction, returning an `EventLoopFuture` that will be fullfilled when the transaction completes.
    func execute() -> EventLoopFuture<Void>
    
    /**
     * Insert item is a non-destructive API. If an item already exists with the specified key this
     * request in the transaction should fail.
     */
    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable
    
    /**
     * Clobber item is destructive API. Regardless of what is present in the database the provided
     * item will be inserted.
     */
    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable
    
    /**
     * Update item requires having gotten an item from the database previously and will not update
     * if the item at the specified key is not the existing item provided.
     */
    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable
    
    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws
    
    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
}
