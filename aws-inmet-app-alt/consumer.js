const axios = require('axios')
const devices = require('./devices')

const api = axios.default.create({
    baseURL: 'http://demo.thingsboard.io/api/v1'
})

module.exports.consumer = async (data) => {
    try {
        const deviceToken = devices[data.cod]
        await api.post(`/${deviceToken}/telemetry`, data)
        const allTopicToken = devices['ALL-E']
        await api.post(`/${allTopicToken}/telemetry`, data)
    } catch (err) {
        console.log(err.message)
    }
}