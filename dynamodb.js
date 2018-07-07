const AWS = require('aws-sdk')

class DynamoDB {
  constructor(region) {
    AWS.config.update({ region })
    this.db = new AWS.DynamoDB()
  }

  upsertTable(params) {
    const tableName = params.TableName

    this.db.describeTable({ TableName: tableName }, async (err, data) => {
      if (err) {
        if (err.name !== 'ResourceNotFoundException') {
          console.error(err)
        } else {
          console.log(`Creating "${tableName}" table`)
          await this._createTable(params)
        }
      } else {
        console.log(`Updating "${tableName}" table`)

        try {
          await this._updateIndex(tableName, params.AttributeDefinitions, params.GlobalSecondaryIndexes, data.Table.GlobalSecondaryIndexes)
          await this._updateThroughput(tableName, params.ProvisionedThroughput, data.Table.ProvisionedThroughput)
          await this._updateStream(tableName, params.StreamSpecification, data.Table.StreamSpecification)
        } catch (e) {
          console.error(e)
        }

        console.log(`Done updating "${tableName}" table`)
      }
    })
  }

  _createTable(params) {
    return new Promise((resolve, reject) => {
      this.db.createTable(params, (err, data) => {
        if (err) {
          console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2))
          reject()
        } else {
          // console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2))
          resolve(data)
        }
      })
    })
  }

  _updateTable(params) {
    return new Promise((resolve, reject) => {
      this.db.updateTable(params, (err, data) => {
        if (err) {
          console.error('Unable to update table. Error JSON:', JSON.stringify(err, null, 2))
          reject()
        } else {
          // console.log('Updated table. Table description JSON:', JSON.stringify(data, null, 2))
          resolve(data)
        }
      })
    })
  }

  _updateThroughput(tableName, expectedThroughput, actualThroughput) {
    if (this._isThroughputEqual(expectedThroughput, actualThroughput)) return Promise.resolve()

    const params = {
      ProvisionedThroughput: Object.assign({}, expectedThroughput),
      TableName: tableName
    }

    return this._updateTable(params)
  }

  _updateStream(tableName, expectedStreamSpec, actualStreamSpec) {
    if (this._isStreamEqual(expectedStreamSpec, actualStreamSpec)) return Promise.resolve()

    const params = {
      StreamSpecification: Object.assign({ StreamEnabled: false }, expectedStreamSpec),
      TableName: tableName
    }

    return this._updateTable(params)
  }

  async _updateIndex(tableName, expectedAttr, expectedGSIs = [], actualGSIs = []) {
    const { toAdd, toUpdate, toDelete } = this._getIndexDiff(expectedGSIs, actualGSIs)

    if (toAdd.length === 0 && toUpdate.length === 0 && toDelete.length === 0) {
      return Promise.resolve()
    }

    for (const key of Object.keys(toAdd)) {
      const idx = toAdd[key]
      await this._updateTable({
        TableName: tableName,
        AttributeDefinitions: expectedAttr,
        GlobalSecondaryIndexUpdates: [{
          Create: {
            IndexName: idx.IndexName,
            KeySchema: idx.KeySchema,
            Projection: idx.Projection,
            ProvisionedThroughput: idx.ProvisionedThroughput,
          }
        }]
      })
    }

    for (const key of Object.keys(toUpdate)) {
      const idx = toUpdate[key]
      await this._updateTable({
        TableName: tableName,
        AttributeDefinitions: expectedAttr,
        GlobalSecondaryIndexUpdates: [{
          Update: {
            IndexName: idx.IndexName,
            ProvisionedThroughput: idx.ProvisionedThroughput,
          }
        }]
      })
    }

    for (const key of Object.keys(toDelete)) {
      const idx = toDelete[key]
      await this._updateTable({
        TableName: tableName,
        AttributeDefinitions: expectedAttr,
        GlobalSecondaryIndexUpdates: [{
          Delete: {
            IndexName: idx.IndexName,
          }
        }]
      })
    }
  }

  _getIndexDiff(expectedGSIs, actualGSIs) {
    const toAdd = {}
    const toUpdate = {}
    const toDelete = {}
    const unchanged = {}

    for (const actualGSI of actualGSIs) {
      let missing = true
      for (const expectedGSI of expectedGSIs) {
        if (actualGSI.IndexName === expectedGSI.IndexName) {
          missing = false

          if (!this._isIndexEqual(expectedGSI, actualGSI) || !this._isThroughputEqual(expectedGSI, actualGSI)) {
            toUpdate[expectedGSI.IndexName] = expectedGSI
          } else {
            unchanged[expectedGSI.IndexName] = expectedGSI
          }

          break
        }
      }

      if (missing) {
        toDelete[actualGSI.IndexName] = actualGSI
      }
    }

    for (const expectedGSI of expectedGSIs) {
      if (!unchanged[expectedGSI.IndexName] && !toUpdate[expectedGSI.IndexName]) {
        toAdd[expectedGSI.IndexName] = expectedGSI
      }
    }
    
    return { toAdd, toUpdate, toDelete }
  }

  _isIndexEqual(idx1, idx2) {
    if (idx1.IndexName !== idx2.IndexName) {
      return false
    }

    if (idx1.KeySchema.length !== idx2.KeySchema.length) {
      return false
    }

    if (idx1.Projection.ProjectionType !== idx2.Projection.ProjectionType) {
      return false
    }

    const idx1NonKeyAttributes = idx1.Projection.NonKeyAttributes || []
    const idx2NonKeyAttributes = idx2.Projection.NonKeyAttributes || []

    if (idx1NonKeyAttributes.length !== idx2NonKeyAttributes.length) {
      return false
    }

    let count = 0
    for (const attr1 of idx1NonKeyAttributes) {
      for (const attr2 of idx2NonKeyAttributes) {
        if (attr1 === attr2) {
          count++
        }
      }
    }

    if (count !== idx1NonKeyAttributes.length) {
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

  _isThroughputEqual(throughput1, throughput2) {
    if (throughput1.ReadCapacityUnits !== throughput2.ReadCapacityUnits) return false
    if (throughput1.WriteCapacityUnits !== throughput2.WriteCapacityUnits) return false
    return true
  }

  _isStreamEqual(stream1, stream2) {
    if (stream1 === undefined && stream2 === undefined) return true
    if (stream1 === undefined || stream2 === undefined) return false
    if (stream1.StreamEnabled !== stream2.StreamEnabled) return false
    if (stream1.StreamViewType !== stream2.StreamViewType) return false
    return true
  }

  _waitFor(tableName) {
    return new Promise((resolve, reject) => {
      this.db.waitFor('tableExists', { TableName: tableName }, err => {
        if (err) {
          console.error('Failed to wait for modification to complete. Error JSON:', JSON.stringify(err, null, 2))
          reject()
        } else {
          resolve()
        }
      })
    })
  }

  updateTimeToLive(params) {
    if (!params) { return }

    console.log(`Updating "${params.TableName}" TTL`)
    this.db.updateTimeToLive(params, (err, data) => {
      if (err) {
        console.error('Unable to update TTL. Error JSON:', JSON.stringify(err, null, 2))
      } else {
        console.log('Updated TTL. TTL description JSON:', JSON.stringify(data, null, 2))
      }
    })
  }

  dropTable(tableName) {
    this.db.deleteTable({ TableName: tableName }, (err, data) => {
      if (err) {
        console.error('Unable to delete table. Error JSON:', JSON.stringify(err, null, 2))
      } else {
        console.log('Deleted table. Table description JSON:', JSON.stringify(data, null, 2))
      }
    })
  }
}

module.exports = DynamoDB