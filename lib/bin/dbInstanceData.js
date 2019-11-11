'use strict';

const AWS = require('aws-sdk');

class DbInstanceData {
    constructor(config, dbRecord) {
        this.DbTable = config.DbTable;
        this.DbRegion = config.DbRegion;
        this.dbRecord = dbRecord;
        this.limit = config.Limit || 100;
        this.provisionedThroughputPercent = config.ProvisionedThroughputPercent;
        this.totalSegments = config.TotalSegments || 1;
    }

    retrieve() {
        return this.getTableKeys()
            .then(keys => {
                return this.getRecords(keys)
                    .catch(err => {
                        throw err;
                    });
            })
            .catch(err => {
                throw err;
            });
    }

    getItem(Key) {
        let dynamodb = new AWS.DynamoDB({ region: this.DbRegion });
        let params = {
            Key,
            TableName: this.DbTable,
            ConsistentRead: true
        };
        return dynamodb.getItem(params).promise()
            .then(data => {
                if (data && data.Item) {
                    return data.Item;
                }
                return {}
            });
    }

    getTableKeys() {
        return new Promise((resolve, reject) => {
            let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
            dynamoDb.describeTable({ TableName: this.DbTable }, (err, data) => {
                if (err) {
                    return reject(err);
                }
                console.log('Got key schema ' + JSON.stringify(data.Table.KeySchema));
                return resolve(data.Table.KeySchema);
            });
        });
    }

    calculateLimitFromProvisionedThroughputPercentage(percentage, provisionedThroughput) {
        return percentage * provisionedThroughput;
    }

    getTableReadProvisionedThroughput() {
        return new Promise((resolve, reject) => {
            let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
            dynamoDb.describeTable({ TableName: this.DbTable }, (err, data) => {
                if (err) {
                    return reject(err);
                }

                if (
                    !data.Table.ProvisionedThroughput
                    || data.Table.ProvisionedThroughput.ReadCapacityUnits === 0
                ) {
                    return reject("Dynamo Table not configured with provisioned throughput. Specify Limit config option.");
                }
                console.log('Got provisioned throughput: ' + JSON.stringify(data.Table.ProvisionedThroughput));
                return resolve(data.Table.ProvisionedThroughput);
            });
        });
    }

    getRecords(keys) {
        return new Promise((resolve, reject) => {

            if(this.provisionedThroughputPercent) {
                var readCapacity = this.getTableReadProvisionedThroughput().catch(err => {
                    throw err;
                }).ReadCapacityUnits;
                this.limit = this.calculateLimitFromProvisionedThroughputPercentage(this.provisionedThroughputPercent, readCapacity);
            }

            let dynamodb = new AWS.DynamoDB({ region: this.DbRegion });
            let params = {
                TableName: this.DbTable,
                ExclusiveStartKey: null,
                Limit: this.limit,
                TotalSegments: this.totalSegments,
                Select: 'ALL_ATTRIBUTES'
            };

            var numberOfRecords = 0;

            function recursiveCall(params) {
                return new Promise((rs, rj) => {


                    //console.log(`outside scan Segment Num: ${params.Segment}`);
                    dynamodb.scan(params, (err, data) => {
                        //console.log(`params: ${JSON.stringify(params)}`);
                        console.log(`inside scan Segment Num: ${params.Segment}`);
                        if (err) {
                            return rj(err);
                        }
                        
                        let records = [];
                        data.Items.forEach((item) => {
                            let id = {};
                            keys.forEach(key => {
                                id[key.AttributeName] = item[key.AttributeName];
                            });

                            let record = {
                                keys: JSON.stringify(id),
                                data: JSON.stringify(item),
                                event: 'INSERT'
                            };
                            records.push(record);
                        });

                        let promises = [];
                        records.forEach(record => {
                            promises.push(this.dbRecord.backup([record]));
                        });
                        //console.log(`params: ${JSON.stringify(params)}`);
                        Promise.all(promises)
                            .then(() => {
                                numberOfRecords += data.Items.length;
                                console.log('Retrieved ' + data.Items.length + ' records; total at ' + numberOfRecords + ' records. Scanned Count: ' + data.ScannedCount);
                                // console.log(`params: ${JSON.stringify(params)}, LEK: ${JSON.stringify(data.LastEvaluatedKey)}`);
                                //console.log(`RESPONSE DATA All records: ${JSON.stringify(data.Items)}\``);
                                if (data.LastEvaluatedKey) {
                                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                                    return recursiveCall.call(this, params).then(() => rs());
                                } 
                                return rs();
                            })
                            .catch(err => {
                                rj(err);
                            });
                    });
                });
            }

            let segmentPromises = [];
            let i = 0;
            for (i; i < this.totalSegments; i++) {
                params['Segment'] = i;
                segmentPromises.push(recursiveCall.call(this, JSON.parse(JSON.stringify(params))));
            }


            Promise.all(segmentPromises)
            .then(data => { 
                resolve() 
            }).catch(err =>{
                reject(err);
            });
        });
    }

}

module.exports = DbInstanceData;
