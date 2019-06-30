'use strict';

const fs = require('fs');
const _ = require('lodash');
const csv = require('csvtojson');
const moment = require('moment');

const processAllData = async () => {

    let actors = [];
    let timeline = [];
    let stations = [];
    let stationKeys = [];

    let meta = {
        startTime: 0,
        endTime: 0,
        dayRange: 0,
        timeIncrement: 60 * 60 * 48 * 1000, // 1 day
        actors: {
            m: { name: 'manta', color: '#f01eff' },
            s: { name: 'silvertip ', color: '#ff8c00' },//dc3545
            g: { name: 'greyreef', color: '#eae600' },
            v: { name: 'vessel', color: '#ffffff' }
        }
    };

    const sharkTypes = {
        'Carcharhinus albimarginatus': 's',
        'Carcharhinus amblyrhynchos': 'g'
    };

    console.log('Importing CSV data...');

    const stationsData = await csv({ includeColumns: /(station|x|y)/ }).fromFile(__dirname + '/csv/stations.csv');
    const vesselData = await csv({ includeColumns: /(Date|lat|long)/ }).fromFile(__dirname + '/csv/vessel.csv');
    const sharksData = await csv({ includeColumns: /(datetime|animal_id|species|From|To|Movement)/ }).fromFile(__dirname + '/csv/sharks.csv');
    const mantasData = await csv({ includeColumns: /(detect_date|receiver|From|To|Movement)/ }).fromFile(__dirname + '/csv/mantas.csv');

    // Create stations lists
    for (let i = 0; i < stationsData.length; i++) {
        const s = stationsData[i];
        stations.push([parseFloat(s.x).toFixed(4), parseFloat(s.y).toFixed(4)]);
        stationKeys.push(s.station);
    }

    console.log(`Stations: ${stationKeys.length}`);

    // Combine all actor data for processing
    const actorsRaw = sharksData.concat(mantasData, vesselData);

    console.log('Processing data...');

    // Process actors data
    for (let i = 0; i < actorsRaw.length; i++) {
        const a = actorsRaw[i];
        const actorType = a.hasOwnProperty('Date') ? 'vessel' : a.hasOwnProperty('datetime') ? 'shark' : 'manta';

        // Skip samples where actor doesn't move
        if ((actorType === 'shark' || actorType === 'manta') && a.Movement === 'NA') {
            continue;
        }

        switch (actorType) {
            case 'shark':
                actors.push({
                    id: a.animal_id,
                    type: sharkTypes[a.species],
                    time: moment(a.datetime, 'YYYY-MM-DD kk:mm:ss').unix() * 1000,
                    from : a.From,
                    to: a.To
                });
                break;

            case 'manta':
                actors.push({
                    id: a.receiver,
                    type: 'm',
                    time: moment(a.detect_date, 'DD/MM/YYYY kk:mm').unix() * 1000,
                    from : a.From,
                    to: a.To
                });
                break;

            case 'vessel':
                actors.push({
                    id: 1,
                    type: 'v',
                    time: moment(a.Date.replace(' UTC', ''), 'DD/MM/YYYY kk:mm:ss').unix() * 1000,
                    lat : parseFloat(a.lat).toFixed(4),
                    lng: parseFloat(a.long).toFixed(4)
                });
                break;

            default: 
                console.error(`Unknown actor type: ${type}`);
        }
    }

    // Sort data by time
    actors.sort((a,b) => a.time - b.time);

    // Determine meta data
    meta.startTime = moment(actors[0].time).startOf('day').unix() * 1000;
    meta.endTime = moment(actors[actors.length - 1].time).endOf('day').unix() * 1000;
    meta.dayRange = moment(meta.endTime).diff(moment(meta.startTime), 'days');

    console.log(`Start date: ${moment(meta.startTime)}`);
    console.log(`End date: ${moment(meta.endTime)}`);
    console.log(`Day range: ${meta.dayRange}`);

    // Fill timeline to desired size
    for (let i = 0; i <= meta.dayRange; i++) {
        timeline.push({});
    }

    console.log('Mapping data to timeline...');

    // Map animal tracking samples into timeline
    actors.forEach(actor => {
        const actorKey = `${actor.type}_${actor.id}`;
        const index = moment(actor.time).diff(moment(meta.startTime), 'days');

        if (!timeline[index].hasOwnProperty(actorKey)) {
            timeline[index][actorKey] = [];
        }

        const data = actor.type === 'v' 
            ? [actor.lng, actor.lat]
            : [stationKeys.indexOf(actor.from), stationKeys.indexOf(actor.to)];

        timeline[index][actorKey].push(data);
    });

    fs.writeFileSync(__dirname + `/data/data.js`, `window.BERTARELLI_DATA=${JSON.stringify({
        meta,
        stations,
        timeline
    })};`);

    console.log('Processing complete!');
};

processAllData();
