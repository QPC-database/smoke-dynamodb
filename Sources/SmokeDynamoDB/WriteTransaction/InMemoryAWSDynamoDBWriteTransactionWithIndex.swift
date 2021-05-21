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
//  InMemoryDynamoDBWriteTransactionWithIndex.swift
//  SmokeDynamoDB
//

import NIO

public class InMemoryDynamoDBWriteTransactionWithIndex<GSILogic: DynamoDBCompositePrimaryKeyGSILogic>: DynamoDBWriteTransaction {
    private let mainTableWriteTransaction: InMemoryDynamoDBWriteTransaction
    private var transactItems: [() -> EventLoopFuture<Void>]
    private let gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable
    private let gsiLogic: GSILogic
    private let eventLoop: EventLoop
    
    internal init(mainTableWriteTransaction: InMemoryDynamoDBWriteTransaction,
                  gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable,
                  gsiLogic: GSILogic,
                  eventLoop: EventLoop) {
        self.transactItems = []
        self.mainTableWriteTransaction = mainTableWriteTransaction
        self.gsiDataStore = gsiDataStore
        self.gsiLogic = gsiLogic
        self.eventLoop = eventLoop
    }
    
    public var count: Int {
        return self.mainTableWriteTransaction.count
    }
    
    public func execute() -> EventLoopFuture<Void> {
        return self.mainTableWriteTransaction.execute().flatMap { _ in
            let futures = self.transactItems.map { transactItem in
                return transactItem()
            }
            return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
        }
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        try self.mainTableWriteTransaction.insertItem(item)
        
        func insertGSIItem() -> EventLoopFuture<Void> {
            return self.gsiLogic.onInsertItem(item, gsiDataStore: self.gsiDataStore)
        }
        
        self.transactItems.append(insertGSIItem)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        try self.mainTableWriteTransaction.clobberItem(item)
        
        func clobberGSIItem() -> EventLoopFuture<Void> {
            return self.gsiLogic.onClobberItem(item, gsiDataStore: self.gsiDataStore)
        }
        
        self.transactItems.append(clobberGSIItem)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        try self.mainTableWriteTransaction.updateItem(newItem: newItem, existingItem: existingItem)
        
        func updateGSIItem() -> EventLoopFuture<Void> {
            return self.gsiLogic.onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: self.gsiDataStore)
        }
        
        self.transactItems.append(updateGSIItem)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws
            where AttributesType: PrimaryKeyAttributes {
        try self.mainTableWriteTransaction.deleteItem(forKey: key)
        
        func deleteGSIItem() -> EventLoopFuture<Void> {
            return self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
        }
        
        self.transactItems.append(deleteGSIItem)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        try self.mainTableWriteTransaction.deleteItem(existingItem: existingItem)
        
        func deleteGSIItem() -> EventLoopFuture<Void> {
            return self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
        }
        
        self.transactItems.append(deleteGSIItem)
    }
}
