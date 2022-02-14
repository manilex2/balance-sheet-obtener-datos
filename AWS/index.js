require('dotenv').config();
const mysql = require('mysql2');
const fetch = require('node-fetch');
const { database } = require('./keys');
const conexion = mysql.createConnection({
    host: database.host,
    user: database.user,
    password: database.password,
    port: database.port,
    database: database.database
});

exports.handler = async function (event) {
    const promise = new Promise(async function() {
        var sql = `SELECT name FROM ${process.env.TABLE_TICKERS_LIST}`;
        conexion.query(sql, function (err, resultado) {
            if (err) throw err;
            guardarIncomeStatement(resultado);
        });
        async function guardarIncomeStatement(resultado){
            for (let i = 0; i < resultado.length; i++) {
                var ticker = resultado[i].name;
                await fetch(`https://financialmodelingprep.com/api/v3/balance-sheet-statement/${ticker}?apikey=${process.env.FMP_KEY}`)
                .then((res) => {
                    return res.json();
                }).then((json) => {
                    var balanceSheet = json;
                    guardarBaseDeDatos(balanceSheet);
                }).catch((err) => {
                    console.error(err);
                });
            }
            await finalizarEjecucion();
        };
        function guardarBaseDeDatos(datos){
            for (let i = 0; i < datos.length; i++) {
                var sql = `INSERT INTO ${process.env.TABLE_BALANCE_SHEET} (
                    date,
                    symbol, 
                    cik,
                    reportedCurrency,
                    fillingDate,
                    acceptedDate,
                    calendarYear,
                    period,
                    cashAndCashEquivalents,
                    shortTermInvestments,
                    cashAndShortTermInvestments,
                    netReceivables,
                    inventory,
                    otherCurrentAssets,
                    totalCurrentAssets,
                    propertyPlantEquipmentNet,
                    goodwill,
                    intangibleAssets,
                    goodwillAndIntangibleAssets,
                    longTermInvestments,
                    taxAssets,
                    otherNonCurrentAssets,
                    totalNonCurrentAssets,
                    otherAssets,
                    totalAssets,
                    accountPayables,
                    shortTermDebt,
                    taxPayables,
                    deferredRevenue,
                    otherCurrentLiabilities,
                    totalCurrentLiabilities,
                    longTermDebt,
                    deferredRevenueNonCurrent,
                    deferredTaxLiabilitiesNonCurrent,
                    otherNonCurrentLiabilities,
                    totalNonCurrentLiabilities,
                    otherLiabilities,
                    capitalLeaseObligations,
                    totalLiabilities,
                    preferredStock,
                    commonStock,
                    retainedEarnings,
                    accumulatedOtherComprehensiveIncomeLoss,
                    othertotalStockholdersEquity,
                    totalStockholdersEquity,
                    totalLiabilitiesAndStockholdersEquity,
                    minorityInterest,
                    totalEquity,
                    totalLiabilitiesAndTotalEquity,
                    totalInvestments,
                    totalDebt,
                    netDebt,
                    link,
                    finalLink
                    )
                    SELECT * FROM (SELECT
                        '${datos[i].date}' AS date,
                        '${datos[i].symbol}' AS symbol,
                        '${datos[i].cik}' AS cik,
                        '${datos[i].reportedCurrency}' AS reportedCurrency,
                        '${datos[i].fillingDate}' AS fillingDate,
                        '${datos[i].acceptedDate}' AS acceptedDate,
                        '${datos[i].calendarYear}' AS calendarYear,
                        '${datos[i].period}' AS period,
                        ${datos[i].cashAndCashEquivalents} AS cashAndCashEquivalents,
                        ${datos[i].shortTermInvestments} AS shortTermInvestments,
                        ${datos[i].cashAndShortTermInvestments} AS cashAndShortTermInvestments,
                        ${datos[i].netReceivables} AS netReceivables,
                        ${datos[i].inventory} AS inventory,
                        ${datos[i].otherCurrentAssets} AS otherCurrentAssets,
                        ${datos[i].totalCurrentAssets} AS totalCurrentAssets,
                        ${datos[i].propertyPlantEquipmentNet} AS propertyPlantEquipmentNet,
                        ${datos[i].goodwill} AS goodwill,
                        ${datos[i].intangibleAssets} AS intangibleAssets,
                        ${datos[i].goodwillAndIntangibleAssets} AS goodwillAndIntangibleAssets,
                        ${datos[i].longTermInvestments} AS longTermInvestments,
                        ${datos[i].taxAssets} AS taxAssets,
                        ${datos[i].otherNonCurrentAssets} AS otherNonCurrentAssets,
                        ${datos[i].totalNonCurrentAssets} AS totalNonCurrentAssets,
                        ${datos[i].otherAssets} AS otherAssets,
                        ${datos[i].totalAssets} AS totalAssets,
                        ${datos[i].accountPayables} AS accountPayables,
                        ${datos[i].shortTermDebt} AS shortTermDebt,
                        ${datos[i].taxPayables} AS taxPayables,
                        ${datos[i].deferredRevenue} AS deferredRevenue,
                        ${datos[i].otherCurrentLiabilities} AS otherCurrentLiabilities,
                        ${datos[i].totalCurrentLiabilities} AS totalCurrentLiabilities,
                        ${datos[i].longTermDebt} AS longTermDebt,
                        ${datos[i].deferredRevenueNonCurrent} AS deferredRevenueNonCurrent,
                        ${datos[i].deferredTaxLiabilitiesNonCurrent} AS deferredTaxLiabilitiesNonCurrent,
                        ${datos[i].otherNonCurrentLiabilities} AS otherNonCurrentLiabilities,
                        ${datos[i].totalNonCurrentLiabilities} AS totalNonCurrentLiabilities,
                        ${datos[i].otherLiabilities} AS otherLiabilities,
                        ${datos[i].capitalLeaseObligations} AS capitalLeaseObligations,
                        ${datos[i].totalLiabilities} AS totalLiabilities,
                        ${datos[i].preferredStock} AS preferredStock,
                        ${datos[i].commonStock} AS commonStock,
                        ${datos[i].retainedEarnings} AS retainedEarnings,
                        ${datos[i].accumulatedOtherComprehensiveIncomeLoss} AS accumulatedOtherComprehensiveIncomeLoss,
                        ${datos[i].othertotalStockholdersEquity} AS othertotalStockholdersEquity,
                        ${datos[i].totalStockholdersEquity} AS totalStockholdersEquity,
                        ${datos[i].totalLiabilitiesAndStockholdersEquity} AS totalLiabilitiesAndStockholdersEquity,
                        ${datos[i].minorityInterest} AS minorityInterest,
                        ${datos[i].totalEquity} AS totalEquity,
                        ${datos[i].totalLiabilitiesAndTotalEquity} AS totalLiabilitiesAndTotalEquity,
                        ${datos[i].totalInvestments} AS totalInvestments,
                        ${datos[i].totalDebt} AS totalDebt,
                        ${datos[i].netDebt} AS netDebt,
                        '${datos[i].link}' AS link,
                        '${datos[i].finalLink}' AS finalLink
                    ) AS tmp
                    WHERE NOT EXISTS (
                        SELECT date, symbol FROM ${process.env.TABLE_BALANCE_SHEET} WHERE date = '${datos[i].date}' AND symbol = '${datos[i].symbol}'
                    ) LIMIT 1`;
                conexion.query(sql, function (err, resultado) {
                    if (err) throw err;
                    console.log(resultado);
                });
            }
        };
        async function finalizarEjecucion() {
            conexion.end()
            res.send("Ejecutado");
        }
    });
    return promise;
};