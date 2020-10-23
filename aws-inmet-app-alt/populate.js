const axios = require('axios')
const datetime = require('node-datetime').create()
const { putKinesis } = require('./aws')

const api = axios.default.create({
    baseURL: 'https://apitempo.inmet.gov.br'
})

const getStations = async () => {
    const manualsReq = api.get('/estacoes/T')
    const autoReq = api.get('/estacoes/M')
    const [{ data: manuals }, { data: auto }] = await Promise.all([manualsReq, autoReq])
    return [...manuals, ...auto]
}

const filterStations = (stations, { state }) => {
    return stations.filter(station => station.SG_ESTADO == state)
}

const putDailyInfo = async (stations, streamName) => {
    datetime.offsetInDays(-1)
    const yesterday = datetime.format('Y-m-d')
    const promises = stations.map(async ({ CD_ESTACAO }) => {
        try {
            const { data } = await api.get(`/estacao/diaria/${yesterday}/${yesterday}/${CD_ESTACAO}`)
            const payload = {
                medTemp: parseFloat(data[0].TEMP_MED) || null,
                minTemp: parseFloat(data[0].TEMP_MIN) || null,
                maxTemp: parseFloat(data[0].TEMP_MAX) || null,
                medhumd: parseFloat(data[0].UMID_MED) || null,
                measuredAt: data[0].DT_MEDICAO,
                name: data[0].DC_NOME,
                cod: `E-${CD_ESTACAO}`
            }
            putKinesis(payload, streamName)
            return payload
        } catch (err) {
            console.log(err)
            return {}
        }
    })

    return Promise.all(promises)
}

module.exports.populate = async (streamName) => {
    try {
        const stations = await getStations({})
        const filtered = filterStations(stations, { state: 'PE' })
        const resp = await putDailyInfo(filtered, streamName)
        return resp
    } catch (err) {
        console.log(err.message)
    }
}