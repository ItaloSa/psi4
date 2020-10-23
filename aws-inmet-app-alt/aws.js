const AWS = require('aws-sdk');
const config = require('./kinesisConfig');
const kinesis = new AWS.Kinesis({
    apiVersion: '2020-10-11',
    region: process.env.REGION
});

const savePayload = (payload, StreamName) => {
    if (typeof payload !== config.PAYLOAD_TYPE) {
        try {
            payload = JSON.stringify(payload);
        } catch (e) {
            console.log(e);
        }
    }

    let params = {
        Data: payload,
        PartitionKey: config.PARTITION_KEY,
        StreamName
    };

    kinesis.putRecord(params, (err, data) => {
        if (err) console.log(err, err.stack);
        else console.log('Record added:', data);
    });
};

exports.putKinesis = (payload, StreamName) => {
    kinesis.describeStream({ StreamName }, (err, data) => {
        if (err) console.log(err, err.stack);
        else {
            if (data.StreamDescription.StreamStatus === config.STATE.ACTIVE
                || data.StreamDescription.StreamStatus === config.STATE.UPDATING) {
                savePayload(payload, StreamName);
            } else {
                console.log(`Kinesis stream ${StreamName} is ${data.StreamDescription.StreamStatus}.`);
                console.log(`Record Lost`, JSON.parse(payload));
            }
        }
    });
};