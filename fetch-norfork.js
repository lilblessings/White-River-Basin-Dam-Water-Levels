const fs = require('fs').promises;
const https = require('https');

const folderName = 'historic_data';

// Dam coordinates for all White River Basin dams
const damCoordinates = {
  'norfork': { latitude: 36.2483333, longitude: -92.24 },
  'bullshoals': { latitude: 36.3658, longitude: -92.5808 },
  'tablerock': { latitude: 36.5958, longitude: -93.3108 },
  'beaverlake': { latitude: 36.4281, longitude: -93.8472 },
  'greersferryLake': { latitude: 35.5295, longitude: -92.0343 }
};

// Map official names to display names
const Names = {
  'NORFORK': 'Norfork',
  'BULLSHOALS': 'Bull Shoals',
  'TABLEROCK': 'Table Rock',
  'BEAVERLAKE': 'Beaver Lake',
  'GREERSFERRYLAKE': 'Greers Ferry Lake'
};

// Dam specifications
const damSpecs = {
  'norfork': {
    MWL: '580.00',
    MWLUnit: 'ft',
    FRL: '552.00',
    FRLUnit: 'ft',
    floodPool: '580.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '1,888,448',
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '2,580,000',
    ruleLevel: '510.00',
    ruleLevelUnit: 'ft',
    blueLevel: '552.00',
    blueLevelUnit: 'ft',
    orangeLevel: '570.00',
    orangeLevelUnit: 'ft',
    redLevel: '580.00',
    redLevelUnit: 'ft',
    deadStorageLevel: '380.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '22,000',
    surfaceAreaUnit: 'acres'
  },
  'bullshoals': {
    MWL: '695.00',
    MWLUnit: 'ft',
    FRL: '654.00',
    FRLUnit: 'ft',
    floodPool: '695.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '2,360,000',
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '3,405,000',
    ruleLevel: '620.00',
    ruleLevelUnit: 'ft',
    blueLevel: '654.00',
    blueLevelUnit: 'ft',
    orangeLevel: '675.00',
    orangeLevelUnit: 'ft',
    redLevel: '695.00',
    redLevelUnit: 'ft',
    deadStorageLevel: '477.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '45,440',
    surfaceAreaUnit: 'acres'
  },
  'tablerock': {
    MWL: '931.00',
    MWLUnit: 'ft',
    FRL: '915.00',
    FRLUnit: 'ft',
    floodPool: '931.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '3,462,000',
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '4,293,000',
    ruleLevel: '895.00',
    ruleLevelUnit: 'ft',
    blueLevel: '915.00',
    blueLevelUnit: 'ft',
    orangeLevel: '923.00',
    orangeLevelUnit: 'ft',
    redLevel: '931.00',
    redLevelUnit: 'ft',
    deadStorageLevel: '737.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '43,100',
    surfaceAreaUnit: 'acres'
  },
  'beaverlake': {
    MWL: '1130.00',
    MWLUnit: 'ft',
    FRL: '1120.00',
    FRLUnit: 'ft',
    floodPool: '1130.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '1,952,000',
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '2,347,000',
    ruleLevel: '1100.00',
    ruleLevelUnit: 'ft',
    blueLevel: '1120.00',
    blueLevelUnit: 'ft',
    orangeLevel: '1125.00',
    orangeLevelUnit: 'ft',
    redLevel: '1130.00',
    redLevelUnit: 'ft',
    deadStorageLevel: '935.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '28,370',
    surfaceAreaUnit: 'acres'
  },
  'greersferryLake': {
    MWL: '487.00',
    MWLUnit: 'ft',
    FRL: '461.00',
    FRLUnit: 'ft',
    floodPool: '487.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '2,050,000',
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '3,222,000',
    ruleLevel: '440.00',
    ruleLevelUnit: 'ft',
    blueLevel: '461.00',
    blueLevelUnit: 'ft',
    orangeLevel: '474.00',
    orangeLevelUnit: 'ft',
    redLevel: '487.00',
    redLevelUnit: 'ft',
    deadStorageLevel: '335.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '31,500',
    surfaceAreaUnit: 'acres'
  }
};

// USACE CDA API endpoints for each dam
const USACE_API_BASE = 'https://water.usace.army.mil/cda/reporting/providers/swl/timeseries';

const API_ENDPOINTS = {
  norfork: {
    waterLevel: 'Norfork_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Norfork_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Norfork_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Norfork_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Norfork_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Norfork_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
    precipitation: 'Norfork_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  bullshoals: {
    waterLevel: 'Bull_Shoals_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Bull_Shoals_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Bull_Shoals_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Bull_Shoals_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Bull_Shoals_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Bull_Shoals_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
    precipitation: 'Bull_Shoals_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  tablerock: {
    waterLevel: 'Table_Rock_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Table_Rock_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Table_Rock_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Table_Rock_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Table_Rock_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Table_Rock_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
    precipitation: 'Table_Rock_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  beaverlake: {
    waterLevel: 'Beaver_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Beaver_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Beaver_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Beaver_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Beaver_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Beaver_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
    precipitation: 'Beaver_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  greersferryLake: {
    waterLevel: 'Greers_Ferry_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Greers_Ferry_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Greers_Ferry_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Greers_Ferry_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Greers_Ferry_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Greers_Ferry_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
    precipitation: 'Greers_Ferry_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  }
};

// Lake temperature URLs
const LAKE_TEMP_URLS = {
  norfork: 'https://seatemperature.net/lakes/water-temp-in-norfork-lake',
  bullshoals: 'https://seatemperature.net/lakes/water-temp-in-bull-shoals-lake',
  tablerock: 'https://seatemperature.net/lakes/water-temp-in-table-rock-lake',
  beaverlake: 'https://seatemperature.net/lakes/water-temp-in-beaver-lake',
  greersferryLake: 'https://seatemperature.net/lakes/water-temp-in-greers-ferry-lake'
};

// Storage calculation function
const calculateStoragePercentage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return '0.00';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL);
  const floodPool = parseFloat(specs.floodPool);
  const mwl = parseFloat(specs.MWL);
  const deadLevel = parseFloat(specs.deadStorageLevel);
  
  if (level <= deadLevel) {
    return '0.00';
  } else if (level <= floodPool) {
    const depthRatio = (level - deadLevel) / (floodPool - deadLevel);
    const storageRatio = Math.pow(depthRatio, 2.2);
    const percentage = storageRatio * 100;
    return Math.max(0, Math.min(100, percentage)).toFixed(2);
  } else {
    const baseStorage = 100;
    const surchargeDepth = level - floodPool;
    const maxSurchargeDepth = mwl - floodPool;
    
    if (maxSurchargeDepth > 0) {
      const surchargeRatio = Math.min(1, surchargeDepth / maxSurchargeDepth);
      const additionalSurcharge = surchargeRatio * 15;
      return Math.min(115, baseStorage + additionalSurcharge).toFixed(2);
    }
    
    return '100.00';
  }
};

// HTTP request function
function makeRequest(url) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      },
      timeout: 15000
    };

    const req = https.get(url, options, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          data: data
        });
      });
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    req.on('error', (err) => {
      reject(err);
    });

    req.setTimeout(15000);
  });
}

// Time range function
function getTimeRange() {
  const end = new Date();
  const start = new Date(end.getTime() - (7 * 24 * 60 * 60 * 1000));
  
  return {
    begin: start.toISOString(),
    end: end.toISOString()
  };
}

// Fetch USACE data function
async function fetchUSACEData(endpoint, parameter) {
  const timeRange = getTimeRange();
  const encodedEndpoint = encodeURIComponent(endpoint);
  const url = `${USACE_API_BASE}?name=${encodedEndpoint}&begin=${timeRange.begin}&end=${timeRange.end}`;
  
  console.log(`📡 Fetching ${parameter} data...`);
  
  try {
    const response = await makeRequest(url);
    
    if (response.statusCode !== 200) {
      throw new Error(`USACE API returned status ${response.statusCode} for ${parameter}`);
    }
    
    const data = JSON.parse(response.data);
    
    if (!data.values || !Array.isArray(data.values)) {
      console.log(`⚠️ No values found for ${parameter}`);
      return new Map();
    }
    
    console.log(`✅ Retrieved ${data.values.length} ${parameter} data points`);
    
    const dataMap = new Map();
    data.values.forEach(([timestamp, value]) => {
      const utcDate = new Date(timestamp);
      const localDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000));
      
      const key = `${localDate.getFullYear()}-${(localDate.getMonth() + 1).toString().padStart(2, '0')}-${localDate.getDate().toString().padStart(2, '0')}-${localDate.getHours().toString().padStart(2, '0')}`;
      dataMap.set(key, { value, originalTimestamp: timestamp });
    });
    
    return dataMap;
    
  } catch (error) {
    console.log(`⚠️ Error fetching ${parameter} data:`, error.message);
    return new Map();
  }
}

// Fetch lake water temperature
async function fetchLakeWaterTemperature(damKey) {
  const url = LAKE_TEMP_URLS[damKey];
  if (!url) {
    console.log(`⚠️ No temperature URL for ${damKey}`);
    return '0';
  }
  
  console.log(`🌡️ Fetching lake water temperature for ${damKey}...`);
  
  try {
    const response = await makeRequest(url);
    
    if (response.statusCode !== 200) {
      throw new Error(`Temperature site returned status ${response.statusCode}`);
    }
    
    let tempMatch = null;
    let temperature = null;
    
    // Try multiple patterns
    tempMatch = response.data.match(/(\d+)°F\s*\n\s*TODAY/);
    if (tempMatch) {
      temperature = parseInt(tempMatch[1]);
      console.log(`✅ Retrieved lake water temperature: ${temperature}°F`);
    }
    
    if (!temperature) {
      tempMatch = response.data.match(/Current Lake Water Temperature Information\s*\n\s*(\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved lake water temperature: ${temperature}°F`);
      }
    }
    
    if (!temperature) {
      tempMatch = response.data.match(/water temperature today in .+ is (\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved lake water temperature: ${temperature}°F`);
      }
    }
    
    if (temperature) {
      return temperature.toString();
    }
    
    console.log('⚠️ Could not extract temperature, defaulting to 0');
    return '0';
    
  } catch (error) {
    console.log(`⚠️ Error fetching lake water temperature:`, error.message);
    return '0';
  }
}

// Fetch all USACE data for a specific dam
async function fetchAllUSACEData(damKey) {
  console.log(`🚀 Fetching data from USACE CDA API for ${Names[damKey.toUpperCase()]}...`);
  
  const endpoints = API_ENDPOINTS[damKey];
  if (!endpoints) {
    console.log(`❌ No API endpoints defined for ${damKey}`);
    return null;
  }
  
  try {
    const [
      waterLevelData,
      inflowData, 
      outflowData,
      spillwayData,
      storageData,
      powerData,
      precipData
    ] = await Promise.all([
      fetchUSACEData(endpoints.waterLevel, 'Water Level'),
      fetchUSACEData(endpoints.inflow, 'Inflow'),
      fetchUSACEData(endpoints.totalOutflow, 'Total Outflow'),
      fetchUSACEData(endpoints.spillwayFlow, 'Spillway Flow'),
      fetchUSACEData(endpoints.storage, 'Storage'),
      fetchUSACEData(endpoints.powerGeneration, 'Power Generation'),
      fetchUSACEData(endpoints.precipitation, 'Precipitation')
    ]);
    
    return {
      waterLevel: waterLevelData,
      inflow: inflowData,
      outflow: outflowData,
      spillway: spillwayData,
      storage: storageData,
      power: powerData,
      precipitation: precipData
    };
    
  } catch (error) {
    console.error(`❌ Failed to fetch USACE data for ${damKey}:`, error.message);
    return null;
  }
}

// Forward-fill rainfall data
function forwardFillRainfall(precipitationMap, allTimestamps) {
  const filledMap = new Map(precipitationMap);
  const sortedTimestamps = Array.from(allTimestamps).sort();
  
  let lastValue = null;
  let fillCount = 0;
  
  sortedTimestamps.forEach(timestamp => {
    if (filledMap.has(timestamp)) {
      lastValue = filledMap.get(timestamp);
    } else if (lastValue !== null) {
      filledMap.set(timestamp, lastValue);
      fillCount++;
    }
  });
  
  return { filledMap, fillCount };
}

// Fetch data for a single dam
async function fetchSingleDamData(damKey) {
  try {
    console.log(`\n🏗️ Processing ${Names[damKey.toUpperCase()]} Dam...`);

    const [usaceData, lakeTemperature] = await Promise.all([
      fetchAllUSACEData(damKey),
      fetchLakeWaterTemperature(damKey)
    ]);

    if (!usaceData) {
      console.log(`❌ No USACE data available for ${damKey}`);
      return null;
    }

    const allTimestamps = new Set();
    Object.values(usaceData).forEach(dataMap => {
      if (dataMap) {
        dataMap.forEach((_, timestamp) => {
          allTimestamps.add(timestamp);
        });
      }
    });

    if (allTimestamps.size === 0) {
      console.log(`❌ No data available from USACE APIs for ${damKey}`);
      return null;
    }

    const { filledMap: filledPrecipitation } = forwardFillRainfall(usaceData.precipitation || new Map(), allTimestamps);
    const sortedTimestamps = Array.from(allTimestamps).sort().reverse();
    
    console.log(`📊 Processing ${sortedTimestamps.length} hourly data points for ${damKey}...`);

    const specs = damSpecs[damKey];
    const allDataPoints = [];

    for (const timestamp of sortedTimestamps) {
      const [year, month, day, hour] = timestamp.split('-');

      const waterLevelData = usaceData.waterLevel.get(timestamp);
      const inflowData = usaceData.inflow.get(timestamp);
      const totalOutflowData = usaceData.outflow.get(timestamp);
      const spillwayData = usaceData.spillway.get(timestamp);
      const powerData = usaceData.power.get(timestamp);
      const storageData = usaceData.storage.get(timestamp);
      const precipitationData = filledPrecipitation.get(timestamp);

      const waterLevel = waterLevelData ? waterLevelData.value : null;
      const inflow = inflowData ? inflowData.value : 0;
      const totalOutflow = totalOutflowData ? totalOutflowData.value : 0;
      const spillwayFlow = spillwayData ? spillwayData.value : 0;
      const powerGen = powerData ? powerData.value : 0;
      const storageAcreFeet = storageData ? storageData.value : 0;
      const precipitation = precipitationData ? (typeof precipitationData === 'object' ? precipitationData.value : precipitationData) : 0;

      if (!waterLevel || waterLevel <= 0) {
        continue;
      }

      const storagePercentage = calculateStoragePercentage(waterLevel, specs);
      const liveStorage = Math.round(storageAcreFeet).toLocaleString();
      const turbineFlow = Math.max(0, totalOutflow - spillwayFlow);
      const netFlow = Math.round(inflow - totalOutflow);
      const turbineEfficiency = turbineFlow > 0 ? (powerGen / turbineFlow).toFixed(3) : '0.000';
      
      const hasForwardFilledRainfall = !usaceData.precipitation.has(timestamp) && filledPrecipitation.has(timestamp);
      const originalUTCTimestamp = waterLevelData ? waterLevelData.originalTimestamp : 
                                   inflowData ? inflowData.originalTimestamp :
                                   totalOutflowData ? totalOutflowData.originalTimestamp :
                                   `${year}-${month}-${day}T${(parseInt(hour) + 5).toString().padStart(2, '0')}:00:00.000Z`;

      const dataPoint = {
        date: `${day}.${month}.${year}`,
        time: `${hour.padStart(2, '0')}:00`,
        waterLevel: waterLevel.toFixed(2),
        liveStorage: liveStorage,
        storagePercentage: storagePercentage + '%',
        inflow: Math.round(inflow).toString(),
        powerHouseDischarge: Math.round(turbineFlow).toString(),
        spillwayRelease: Math.round(spillwayFlow).toString(),
        totalOutflow: Math.round(totalOutflow).toString(),
        powerGeneration: Math.round(powerGen).toString(),
        rainfall: precipitation.toFixed(2),
        dataSource: 'USACE CDA API (Official)',
        timestamp: originalUTCTimestamp,
        netFlow: netFlow,
        turbineEfficiency: turbineEfficiency,
        hasForwardFilledRainfall: hasForwardFilledRainfall,
        lakeWaterTemp: `${lakeTemperature}°F`,
        lakeWaterTempSource: 'SeaTemperature.net (Estimated)'
      };

      allDataPoints.push(dataPoint);
    }

    if (allDataPoints.length === 0) {
      console.log(`❌ No valid hourly data points could be processed for ${damKey}`);
      return null;
    }

    console.log(`✅ Successfully processed ${allDataPoints.length} hourly data points for ${damKey}`);

    const damData = {
      id: (Object.keys(damCoordinates).indexOf(damKey) + 1).toString(),
      name: Names[damKey.toUpperCase()],
      officialName: damKey.toUpperCase(),
      MWL: specs.MWL,
      FRL: specs.FRL,
      liveStorageAtFRL: specs.liveStorageAtFRL,
      ruleLevel: specs.ruleLevel,
      blueLevel: specs.blueLevel,
      orangeLevel: specs.orangeLevel,
      redLevel: specs.redLevel,
      latitude: damCoordinates[damKey].latitude,
      longitude: damCoordinates[damKey].longitude,
      data: allDataPoints
    };

    console.log(`✅ Water Level Range: ${allDataPoints[0].waterLevel} - ${allDataPoints[allDataPoints.length-1].waterLevel} ft MSL`);
    console.log(`✅ Storage Range: ${allDataPoints[0].storagePercentage} - ${allDataPoints[allDataPoints.length-1].storagePercentage} of capacity`);
    
    if (allDataPoints.some(d => parseFloat(d.spillwayRelease) > 0)) {
      console.log(`⚠️  Spillway releases detected in data range`);
    }

    return damData;

  } catch (error) {
    console.error(`Error fetching ${damKey} dam data:`, error);
    return null;
  }
}

// Main function to fetch all dam data
async function fetchAllDamsData() {
  const allDams = [];
  
  for (const damKey of Object.keys(damCoordinates)) {
    try {
      const damData = await fetchSingleDamData(damKey);
      if (damData) {
        allDams.push(damData);
      }
    } catch (error) {
      console.error(`❌ Failed to process ${damKey}:`, error.message);
    }
  }
  
  return { dams: allDams };
}

// Main function to fetch dam details and update data files
async function fetchDamDetails() {
  try {
    console.log('🚀 Starting Multi-Dam White River Basin scraper...');
    console.log(`📊 Processing ${Object.keys(damCoordinates).length} dams: ${Object.values(Names).join(', ')}`);
    
    // Create folder if it doesn't exist
    try {
      await fs.access(folderName);
      console.log('✅ historic_data folder exists');
    } catch (error) {
      console.log('📁 Creating historic_data folder...');
      await fs.mkdir(folderName);
    }

    const { dams } = await fetchAllDamsData();
    console.log(`\n📊 Retrieved ${dams.length} dam(s) from data sources`);

    if (dams.length === 0) {
      console.log('❌ No dam data found - cannot create files.');
      return;
    }

    // Load existing data
    const existingData = {};
    try {
      const files = await fs.readdir(folderName);
      console.log('📂 Found existing files:', files);
      
      for (const file of files) {
        if (file.endsWith('.json')) {
          const damName = file.replace('.json', '');
          const data = JSON.parse(await fs.readFile(`${folderName}/${file}`, 'utf8'));
          existingData[damName] = data;
          console.log(`📄 Loaded existing data for ${damName}: ${data.data.length} records`);
        }
      }
    } catch (error) {
      console.log('📝 No existing data files found, creating new ones...');
    }

    let dataChanged = false;

    for (const newDam of dams) {
      console.log(`\n🔄 Processing dam: ${newDam.name}`);
      const existingDam = existingData[newDam.name];

      if (existingDam) {
        console.log(`📊 Found existing data for ${newDam.name} with ${existingDam.data.length} existing records`);
        
        let newPointsAdded = 0;
        let pointsUpdated = 0;
        
        for (const newDataPoint of newDam.data) {
          const newTimestamp = newDataPoint.timestamp;
          const existingIndex = existingDam.data.findIndex(d => d.timestamp === newTimestamp);

          if (existingIndex === -1) {
            console.log(`➕ Adding new hourly data: ${newDataPoint.date} ${newDataPoint.time}`);
            
            let insertIndex = 0;
            for (let i = 0; i < existingDam.data.length; i++) {
              if (new Date(newDataPoint.timestamp) > new Date(existingDam.data[i].timestamp)) {
                insertIndex = i;
                break;
              }
              insertIndex = i + 1;
            }
            
            existingDam.data.splice(insertIndex, 0, newDataPoint);
            newPointsAdded++;
            dataChanged = true;
            
          } else {
            const existingEntry = existingDam.data[existingIndex];
            const newEntry = newDataPoint;
            
            const hasMoreData = Object.values(newEntry).filter(v => v && v !== '0' && v !== 'N/A').length >
                               Object.values(existingEntry).filter(v => v && v !== '0' && v !== 'N/A').length;
            
            if (hasMoreData) {
              console.log(`   🔄 Updating ${newDataPoint.date} ${newDataPoint.time} with more complete data`);
              existingDam.data[existingIndex] = newEntry;
              pointsUpdated++;
              dataChanged = true;
            }
          }
        }
        
        if (newPointsAdded > 0 || pointsUpdated > 0) {
          console.log(`   📈 Added ${newPointsAdded} new hourly records, updated ${pointsUpdated} existing records`);
          
          Object.assign(existingDam, {
            id: newDam.id,
            officialName: newDam.officialName,
            MWL: newDam.MWL,
            FRL: newDam.FRL,
            liveStorageAtFRL: newDam.liveStorageAtFRL,
            ruleLevel: newDam.ruleLevel,
            blueLevel: newDam.blueLevel,
            orangeLevel: newDam.orangeLevel,
            redLevel: newDam.redLevel,
            latitude: newDam.latitude,
            longitude: newDam.longitude
          });
        } else {
          console.log(`   ⏭️  All hourly data already exists and is complete`);
        }
        
      } else {
        console.log(`🆕 Creating new dam file for ${newDam.name} with ${newDam.data.length} hourly records`);
        existingData[newDam.name] = newDam;
        dataChanged = true;
      }
    }

    console.log(`\n📈 Data changed: ${dataChanged}`);

    if (dataChanged) {
      console.log('💾 Saving files...');
      
      for (const [damName, damData] of Object.entries(existingData)) {
        const filename = `${folderName}/${damName}.json`;
        
        try {
          await fs.writeFile(filename, JSON.stringify(damData, null, 4));
          console.log(`✅ Details for dam ${damName} saved successfully in ${filename}.`);
          
          const stats = await fs.stat(filename);
          console.log(`📁 File size: ${stats.size} bytes`);
        } catch (writeError) {
          console.error(`❌ Error writing ${filename}:`, writeError);
        }
      }

      try {
        const liveData = {
          lastUpdate: new Date().toISOString(),
          dams: Object.values(existingData).map(dam => ({
            ...dam,
            data: dam.data.slice(0, 1) // Only include latest data point for live.json
          }))
        };
        await fs.writeFile('live.json', JSON.stringify(liveData, null, 4));
        console.log('✅ Live dam data saved successfully in live.json.');
        
        const liveStats = await fs.stat('live.json');
        console.log(`📁 Live file size: ${liveStats.size} bytes`);
      } catch (liveError) {
        console.error('❌ Error writing live.json:', liveError);
      }
    } else {
      console.log('⏸️  No new data to save.');
    }

    console.log('\n📊 Summary:');
    console.log(`Total dams processed: ${dams.length}`);
    for (const dam of dams) {
      console.log(`- ${dam.name}: ${dam.data.length} hourly records`);
    }

  } catch (error) {
    console.error('💥 Error in fetchDamDetails:', error);
  }
}

// Run the scraper
if (require.main === module) {
  fetchDamDetails().then(() => {
    console.log('\n✅ Multi-Dam White River Basin scraper completed successfully.');
  }).catch(error => {
    console.error('Scraper failed:', error);
  });
}

module.exports = {
  fetchDamDetails,
  fetchAllDamsData,
  fetchSingleDamData,
  fetchAllUSACEData,
  API_ENDPOINTS,
  damCoordinates,
  damSpecs
};
