const { populate } = require('./populate')
const { consumer } = require('./consumer')

module.exports.consumer = async (event, context) => {
    const promises = event.Records.map(async (record) => {
        const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8')
        const decoded = JSON.parse(payload)
        console.log('Decoded payload:', decoded)
        return consumer(decoded)
    });
    await Promise.all(promises)
};

module.exports.populate = async (event, context, callback) => {
    await populate(process.env.INPT_STREAM)

    const response = {
        statusCode: 200,
        body: JSON.stringify({
            message: 'ok'
        }),
    }
    callback(null, response);
}

