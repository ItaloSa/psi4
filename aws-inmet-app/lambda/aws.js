const AWS = require('aws-sdk');
const config = require('./kinesisConfig');
const kinesis = new AWS.Kinesis({
    apiVersion: config.API_VERSION,
    region: config.REGION
});

const savePayload = (payload) => {
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
        StreamName: config.STREAM_NAME
    };

    kinesis.putRecord(params, function (err, data) {
        if (err) console.log(err, err.stack);
        else console.log('Record added:', data);
    });
};

exports.putKinesis = (payload) => {
    const params = {
        StreamName: config.STREAM_NAME,
    };

    kinesis.describeStream(params, function (err, data) {
        if (err) console.log(err, err.stack);
        else {
            if (data.StreamDescription.StreamStatus === config.STATE.ACTIVE
                || data.StreamDescription.StreamStatus === config.STATE.UPDATING) {
                savePayload(payload);
            } else {
                console.log(`Kinesis stream ${config.STREAM_NAME} is ${data.StreamDescription.StreamStatus}.`);
                console.log(`Record Lost`, JSON.parse(payload));
            }
        }
    });
};