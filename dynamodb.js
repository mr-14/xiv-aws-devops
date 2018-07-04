const AWS = require('aws-sdk')
const dynamodb = new AWS.DynamoDB()

exports.upsertTable = (params) => {
  const tableName = params.TableName

  dynamodb.describeTable({ TableName: tableName }, (err, data) => {
    if (err) {
      if (err.name !== 'ResourceNotFoundException') {
        console.error(err)
      }
      console.log(`Creating "${tableName}" table`)
      createTable(params)
    } else {
      console.log(`Updating "${tableName}" table`)
      updateThroughput(tableName, params.ProvisionedThroughput, data.Table.ProvisionedThroughput)
        .then(() => waitFor(tableName))
        .then(() => updateStream(tableName, params.StreamSpecification, data.Table.StreamSpecification))
        .then(() => waitFor(tableName))
        .then(() => updateIndex(tableName, params.AttributeDefinitions, params.GlobalSecondaryIndexes, data.Table.GlobalSecondaryIndexes))
        .then(() => waitFor(tableName))
        .then(() => updateIndex(tableName, params.AttributeDefinitions, params.GlobalSecondaryIndexes, data.Table.GlobalSecondaryIndexes))
        .catch(() => { })
    }
  })
}

function createTable(params) {
  dynamodb.createTable(params, function (err, data) {
    if (err) {
      console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2))
    } else {
      console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2))
    }
  })
}

function updateThroughput(tableName, expectedThroughput, actualThroughput) {
  if (isThroughputEqual(expectedThroughput, actualThroughput)) return Promise.resolve()

  const params = {
    ProvisionedThroughput: Object.assign({}, expectedThroughput),
    TableName: tableName
  }

  return new Promise((resolve, reject) => {
    dynamodb.updateTable(params, function (err, data) {
      if (err) {
        console.error('Unable to update throughput. Error JSON:', JSON.stringify(err, null, 2))
        reject()
      } else {
        console.log('Updated throughput. Table description JSON:', JSON.stringify(data, null, 2))
        resolve()
      }
    })
  })
}

function updateStream(tableName, expectedStreamSpec, actualStreamSpec) {
  if (isStreamEqual(expectedStreamSpec, actualStreamSpec)) return Promise.resolve()
  
  const params = {
    StreamSpecification: Object.assign({ StreamEnabled: false }, expectedStreamSpec),
    TableName: tableName
  }

  return new Promise((resolve, reject) => {
    dynamodb.updateTable(params, function (err, data) {
      if (err) {
        console.error('Unable to update stream specification. Error JSON:', JSON.stringify(err, null, 2))
        reject()
      } else {
        console.log('Updated stream specification. Table description JSON:', JSON.stringify(data, null, 2))
        resolve()
      }
    })
  })
}

function updateIndex(tableName, expectedAttr, expectedGSIs, actualGSIs) {
  const { toAdd, toUpdate, toDelete } = getIndexDiff(expectedGSIs, actualGSIs)
  const indexUpdates = []

  if (toAdd.length === 0 && toUpdate.length === 0 && toDelete.length === 0) {
    return Promise.resolve()
  }

  for (const key of Object.keys(toAdd)) {
    const idx = toAdd[key]
    indexUpdates.push({
      Create: {
        IndexName: idx.IndexName,
        KeySchema: idx.KeySchema,
        Projection: idx.Projection,
        ProvisionedThroughput: idx.ProvisionedThroughput,
      }
    })
  }

  for (const key of Object.keys(toUpdate)) {
    const idx = toUpdate[key]
    indexUpdates.push({
      Update: {
        IndexName: idx.IndexName,
        ProvisionedThroughput: idx.ProvisionedThroughput,
      }
    })
  }

  for (const key of Object.keys(toDelete)) {
    const idx = toDelete[key]
    indexUpdates.push({
      Delete: {
        IndexName: idx.IndexName,
      }
    })
  }

  const params = {
    TableName: tableName,
    AttributeDefinitions: expectedAttr,
    GlobalSecondaryIndexUpdates: indexUpdates,
  }

  return new Promise((resolve, reject) => {
    dynamodb.updateTable(params, function (err, data) {
      if (err) {
        console.error('Unable to update index. Error JSON:', JSON.stringify(err, null, 2))
        reject()
      } else {
        console.log('Updated index. Table description JSON:', JSON.stringify(data, null, 2))
        resolve()
      }
    })
  })
}

function getIndexDiff(expectedGSIs, actualGSIs) {
  const toAdd = {}
  const toUpdate = {}
  const toDelete = {}

  for (const actualGSI of actualGSIs) {
    let missing = true
    for (const expectedGSI of expectedGSIs) {
      if (isIndexEqual(expectedGSI, actualGSI)) {
        missing = false
        if (!isThroughputEqual(expectedGSI, actualGSI)) {
          toUpdate[expectedGSI.IndexName] = expectedGSI
        }
      }
    }

    if (missing) {
      toDelete[actualGSI.IndexName] = actualGSI
    }
  }

  for (const expectedGSI of expectedGSIs) {
    if (!toUpdate[expectedGSI.IndexName]) {
      toAdd[expectedGSI.IndexName] = expectedGSI
    }
  }

  return { toAdd, toUpdate, toDelete }
}

function isIndexEqual(idx1, idx2) {
  if (idx1.IndexName !== idx2.IndexName) {
    return false
  }

  if (idx1.KeySchema.length !== idx2.KeySchema.length) {
    return false
  }

  if (idx1.Projection.ProjectionType !== idx2.Projection.ProjectionType) {
    return false
  }

  if (idx1.Projection.NonKeyAttributes.length !== idx2.Projection.NonKeyAttributes.length) {
    return false
  }

  let count = 0
  for (const attr1 of idx1.Projection.NonKeyAttributes) {
    for (const attr2 of idx2.Projection.NonKeyAttributes) {
      if (attr1 === attr2) {
        count++
      }
    }
  }

  if (count !== idx1.Projection.NonKeyAttributes.length) {
    return false
  }

  count = 0
  for (const ks1 of idx1.KeySchema) {
    for (const ks2 of idx2.KeySchema) {
      if (ks1.AttributeName === ks2.AttributeName && ks1.KeyType === ks2.KeyType) {
        count++
      }
    }
  }

  if (count !== idx1.KeySchema.length) {
    return false
  }

  return true
}

function isThroughputEqual(throughput1, throughput2) {
  if (throughput1.ReadCapacityUnits !== throughput2.ReadCapacityUnits) return false
  if (throughput1.WriteCapacityUnits !== throughput2.WriteCapacityUnits) return false
  return true
}

function isStreamEqual(stream1, stream2) {
  if (stream1 === null && stream2 === null) return true
  if (stream1 === null || stream2 === null) return false
  if (stream1.StreamEnabled !== stream2.StreamEnabled) return false
  if (stream1.StreamViewType !== stream2.StreamViewType) return false
  return true
}

function waitFor(tableName) {
  return new Promise((resolve, reject) => {
    dynamodb.waitFor('tableExists', { TableName: tableName }, err => {
      if (err) {
        console.error('Failed to wait for modification to complete. Error JSON:', JSON.stringify(err, null, 2))
        reject()
      } else {
        resolve()
      }
    })
  })
}

exports.updateTimeToLive = (params) => {
  if (!params) {
    return
  }
  
  console.log(`Updating "${params.TableName}" TTL`)
  dynamodb.updateTimeToLive(params, (err, data) => {
    if (err) {
      console.error('Unable to update TTL. Error JSON:', JSON.stringify(err, null, 2))
    } else {
      console.log('Updated TTL. TTL description JSON:', JSON.stringify(data, null, 2))
    }
  })
}

exports.dropTable = (tableName) => {
  dynamodb.deleteTable({ TableName: tableName }, (err, data) => {
    if (err) {
      console.error('Unable to delete table. Error JSON:', JSON.stringify(err, null, 2))
    } else {
      console.log('Deleted table. Table description JSON:', JSON.stringify(data, null, 2))
    }
  })
}

exports.truncateTable = (table, keys) => {
  return new Promise(resolve => {
    dynamodb.scan({ TableName: table }, (err, data) => {
      if (err) {
        console.error(err)
        resolve()
      }

      const docs = []
      data.Items.forEach(doc => {
        const keySpec = keys.reduce((list, item) => {
          list[item] = doc[item]
          return list
        }, {})
        docs.push(deleteItem(table, keySpec))
      })
      resolve(Promise.all(docs))
    })
  })
}

function deleteItem(tableName, key) {
  const params = {
    TableName: tableName,
    Key: key,
    ReturnValues: 'NONE',
    ReturnConsumedCapacity: 'NONE',
    ReturnItemCollectionMetrics: 'NONE',
  }
  
  return new Promise(resolve => {
    dynamodb.deleteItem(params, err => {
      if (err) {
        console.error(err)
      }
      resolve()
    })
  })
}