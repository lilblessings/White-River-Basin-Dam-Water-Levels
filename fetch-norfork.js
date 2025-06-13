const fs = require('fs').promises;
const https = require('https');

const folderName = 'historic_data';

// Dam coordinates and specifications
const damCoordinates = {
  'norfork': { latitude: 36.2483333, longitude: -92.24 },
  'bullshoals': { latitude: 36.3694, longitude: -92.5833 },
  'greersferry': { latitude: 35.4939, longitude: -92.0647 },
  'tablerock': { latitude: 36.6117, longitude: -93.2951 },
  'beaver': { latitude: 36.4625, longitude: -93.8542 },
  'clearwater': { latitude: 36.0417, longitude: -90.1536 }
};

// Map official names to display names
const Names = {
  'NORFORK': 'Norfork',
  'BULL_SHOALS': 'Bull Shoals',
  'GREERS_FERRY': 'Greers Ferry',
  'TABLE_ROCK': 'Table Rock',
  'BEAVER': 'Beaver',
  'CLEARWATER': 'Clearwater'
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
    MWL: '695.00', // Top of flood control pool
    MWLUnit: 'ft',
    FRL: '662.00', // Top of conservation pool (power pool)
    FRLUnit: 'ft',
    floodPool: '695.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '5,408,000', // Power drawdown storage
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '5,408,000', // Total storage
    ruleLevel: '630.00', // Estimated minimum operating level
    ruleLevelUnit: 'ft',
    blueLevel: '654.00', // Conservation pool
    blueLevelUnit: 'ft',
    orangeLevel: '675.00', // Mid flood pool
    orangeLevelUnit: 'ft',
    redLevel: '695.00', // Top of flood pool
    redLevelUnit: 'ft',
    deadStorageLevel: '448.00', // Estimated dead storage level
    deadStorageLevelUnit: 'ft',
    surfaceArea: '45,440', // At conservation pool
    surfaceAreaUnit: 'acres'
  },
  'greersferry': {
    MWL: '487.00', // Top of flood control pool
    MWLUnit: 'ft',
    FRL: '462.00', // Top of conservation pool (normal pool)
    FRLUnit: 'ft',
    floodPool: '487.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '2,844,500', // Storage at conservation pool
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '2,844,500', // Total storage
    ruleLevel: '435.00', // Estimated minimum operating level
    ruleLevelUnit: 'ft',
    blueLevel: '462.00', // Conservation pool
    blueLevelUnit: 'ft',
    orangeLevel: '475.00', // Mid flood pool
    orangeLevelUnit: 'ft',
    redLevel: '487.00', // Top of flood pool
    redLevelUnit: 'ft',
    deadStorageLevel: '380.00', // Estimated dead storage level
    deadStorageLevelUnit: 'ft',
    surfaceArea: '31,500', // At normal pool
    surfaceAreaUnit: 'acres'
  },
  'tablerock': {
    MWL: '931.00', // Top of flood control pool
    MWLUnit: 'ft',
    FRL: '917.00', // Top of conservation pool (normal pool)
    FRLUnit: 'ft',
    floodPool: '931.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '3,462,000', // Storage at conservation pool
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '3,462,000', // Total storage
    ruleLevel: '880.00', // Estimated minimum operating level
    ruleLevelUnit: 'ft',
    blueLevel: '915.00', // Conservation pool
    blueLevelUnit: 'ft',
    orangeLevel: '923.00', // Mid flood pool
    orangeLevelUnit: 'ft',
    redLevel: '931.00', // Top of flood pool
    redLevelUnit: 'ft',
    deadStorageLevel: '820.00', // Estimated dead storage level
    deadStorageLevelUnit: 'ft',
    surfaceArea: '43,100', // At normal pool
    surfaceAreaUnit: 'acres'
  },
  'beaver': {
    MWL: '1130.00', // Top of flood control pool
    MWLUnit: 'ft',
    FRL: '1,121.43', // Top of conservation pool (normal pool)
    FRLUnit: 'ft',
    floodPool: '1130.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '1,947,382', // Storage at conservation pool
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '1,947,382', // Total storage
    ruleLevel: '1077.00', // Estimated minimum operating level
    ruleLevelUnit: 'ft',
    blueLevel: '1120.00', // Conservation pool
    blueLevelUnit: 'ft',
    orangeLevel: '1125.00', // Mid flood pool
    orangeLevelUnit: 'ft',
    redLevel: '1130.00', // Top of flood pool
    redLevelUnit: 'ft',
    deadStorageLevel: '1000.00', // Estimated dead storage level
    deadStorageLevelUnit: 'ft',
    surfaceArea: '28,220', // At normal pool
    surfaceAreaUnit: 'acres'
  },
  'clearwater': {
    MWL: '567.00', // Top of flood control pool
    MWLUnit: 'ft',
    FRL: '497.82', // Top of conservation pool (normal pool)
    FRLUnit: 'ft',
    floodPool: '567.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '413,700', // Storage at conservation pool
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '413,700', // Total storage
    ruleLevel: '320.00', // Estimated minimum operating level
    ruleLevelUnit: 'ft',
    blueLevel: '497.00', // Conservation pool
    blueLevelUnit: 'ft',
    orangeLevel: '560.00', // Mid flood pool
    orangeLevelUnit: 'ft',
    redLevel: '567.00', // Top of flood pool
    redLevelUnit: 'ft',
    deadStorageLevel: '280.00', // Estimated dead storage level
    deadStorageLevelUnit: 'ft',
    surfaceArea: '1,860', // At normal pool
    surfaceAreaUnit: 'acres'
  }
};

// USACE CDA API endpoints
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
    powerGeneration: 'Bull_Shoals_Dam.Energy-Gen_Plant.Total.1Hour.1Hour.CCP-Comp',
    precipitation: 'Bull_Shoals_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev',
    tailwaterLevel: 'Bull_Shoals_Dam-Tailwater.Elev-Downstream.Inst.1Hour.0.Decodes-rev',
    floodPoolPercent: 'Bull_Shoals_Dam-Headwater.%-Flood Pool.Inst.1Hour.0.CCP-Comp'
  },
  greersferry: {
    waterLevel: 'GreersFerry_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'GreersFerry_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'GreersFerry_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'GreersFerry_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'GreersFerry_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'GreersFerry_Dam.Energy-Gen_Plant.Total.1Hour.1Hour.CCP-Comp',
    precipitation: 'GreersFerry_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  tablerock: {
    waterLevel: 'Table_Rock_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Table_Rock_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Table_Rock_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Table_Rock_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Table_Rock_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Table_Rock_Dam.Energy-Gen_Plant.Total.1Hour.1Hour.CCP-Comp',
    precipitation: 'Table_Rock_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev',
    tailwaterLevel: 'Table_Rock_Dam-Tailwater.Elev-Downstream.Inst.1Hour.0.Decodes-rev'
  },
  beaver: {
    waterLevel: 'Beaver_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Beaver_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Beaver_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Beaver_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Beaver_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Beaver_Dam.Energy-Gen_Plant.Total.1Hour.1Hour.CCP-Comp',
    precipitation: 'Beaver_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  },
  clearwater: {
    waterLevel: 'Clearwater_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
    inflow: 'Clearwater_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
    totalOutflow: 'Clearwater_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
    spillwayFlow: 'Clearwater_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
    storage: 'Clearwater_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
    powerGeneration: 'Clearwater_Dam.Energy-Gen_Plant.Total.1Hour.1Hour.CCP-Comp',
    precipitation: 'Clearwater_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
  }
};

// Storage calculation function
const calculateStoragePercentage = (waterLevel, specs, damName) => {
  if (!waterLevel || !specs) return '0.00';
  
  const level = parseFloat(waterLevel);
  const floodPool = parseFloat(specs.floodPool); // Top of flood pool = 100%
  const deadLevel = parseFloat(specs.deadStorageLevel);
  
  if (level <= deadLevel) {
    return '0.00';
  } else if (level <= floodPool) {
    // 0% to 100% between dead storage and top of flood pool
    const percentage = ((level - deadLevel) / (floodPool - deadLevel)) * 100;
    return Math.max(0, Math.min(100, percentage)).toFixed(2);
  } else {
    // Above flood pool - emergency storage (100%+)
    return '100.00';
  }
};

// Live storage calculation
const calculateLiveStorage = (waterLevel, specs, damName) => {
  if (!waterLevel || !specs) return '0';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL);
  const floodPool = parseFloat(specs.floodPool);
  const deadLevel = parseFloat(specs.deadStorageLevel);
  
  // Dam-specific storage calculations
  let conservationStorage, floodPoolStorage;
  if (damName === 'bullshoals') {
    conservationStorage = 3400000; // acre-feet at 654 ft
    floodPoolStorage = 5760000; // acre-feet at 695 ft
  } else if (damName === 'greersferry') {
    conservationStorage = 1100000; // acre-feet at 462 ft
    floodPoolStorage = 1400000; // acre-feet at 470 ft
  } else if (damName === 'tablerock') {
    conservationStorage = 2500000; // acre-feet at 915 ft
    floodPoolStorage = 3462000; // acre-feet at 931 ft
  } else if (damName === 'beaver') {
    conservationStorage = 1590000; // acre-feet at 1120 ft
    floodPoolStorage = 2250000; // acre-feet at 1130 ft
  } else if (damName === 'clearwater') {
    conservationStorage = 150000; // acre-feet at 359 ft
    floodPoolStorage = 210000; // acre-feet at 384 ft
  } else {
    conservationStorage = 1983000; // acre-feet at 552 ft (Norfork)
    floodPoolStorage = 2580000; // acre-feet at 580 ft
  }
  
  if (level <= deadLevel) {
    return '0';
  } else if (level <= floodPool) {
    const depthRatio = (level - deadLevel) / (floodPool - deadLevel);
    const storageRatio = Math.pow(depthRatio, damName === 'bullshoals' ? 2.5 : damName === 'tablerock' ? 2.3 : 2.2);
    const currentStorage = Math.round(floodPoolStorage * storageRatio);
    return currentStorage.toLocaleString();
  } else {
    const baseStorage = floodPoolStorage;
    const surchargeDepth = level - floodPool;
    const maxSurchargeDepth = 10;
    
    if (surchargeDepth > 0 && maxSurchargeDepth > 0) {
      const surchargeRatio = Math.min(1, surchargeDepth / maxSurchargeDepth);
      const additionalSurcharge = Math.round(floodPoolStorage * 0.15 * surchargeRatio);
      return (baseStorage + additionalSurcharge).toLocaleString();
    }
    
    return floodPoolStorage.toLocaleString();
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
  const start = new Date(end.getTime() - (30 * 24 * 60 * 60 * 1000)); // 30 days
  
  return {
    begin: start.toISOString(),
    end: end.toISOString()
  };
}

// Fetch precipitation data from NWS API as fallback
async function fetchNWSPrecipitation(damName, timeRange) {
  console.log(`🌦️ Fetching precipitation data from NWS API for ${damName}...`);
  
  // Get coordinates for the dam
  const coordinates = damCoordinates[damName];
  if (!coordinates) {
    console.log(`⚠️ No coordinates found for ${damName}`);
    return new Map();
  }
  
  try {
    // First, get the NWS grid point for the dam location
    const pointUrl = `https://api.weather.gov/points/${coordinates.latitude},${coordinates.longitude}`;
    const pointResponse = await makeRequest(pointUrl);
    
    if (pointResponse.statusCode !== 200) {
      throw new Error(`NWS Point API returned status ${pointResponse.statusCode}`);
    }
    
    const pointData = JSON.parse(pointResponse.data);
    const gridId = pointData.properties.gridId;
    const gridX = pointData.properties.gridX;
    const gridY = pointData.properties.gridY;
    
    console.log(`📍 NWS Grid for ${damName}: ${gridId} ${gridX},${gridY}`);
    
    // Get observations from the nearest weather station
    const stationsUrl = `https://api.weather.gov/gridpoints/${gridId}/${gridX},${gridY}/stations`;
    const stationsResponse = await makeRequest(stationsUrl);
    
    if (stationsResponse.statusCode !== 200) {
      throw new Error(`NWS Stations API returned status ${stationsResponse.statusCode}`);
    }
    
    const stationsData = JSON.parse(stationsResponse.data);
    
    if (!stationsData.features || stationsData.features.length === 0) {
      throw new Error('No weather stations found for this location');
    }
    
    // Try the first few stations until we get data
    for (let i = 0; i < Math.min(3, stationsData.features.length); i++) {
      const stationId = stationsData.features[i].properties.stationIdentifier;
      console.log(`🌧️ Trying station ${stationId} for ${damName}...`);
      
      try {
        const obsUrl = `https://api.weather.gov/stations/${stationId}/observations?start=${timeRange.begin}&end=${timeRange.end}`;
        const obsResponse = await makeRequest(obsUrl);
        
        if (obsResponse.statusCode === 200) {
          const obsData = JSON.parse(obsResponse.data);
          
          if (obsData.features && obsData.features.length > 0) {
            console.log(`✅ Retrieved ${obsData.features.length} precipitation observations from ${stationId}`);
            
            // Convert NWS observations to our format
            const precipMap = new Map();
            obsData.features.forEach(obs => {
              if (obs.properties.precipitationLastHour && obs.properties.precipitationLastHour.value !== null) {
                const timestamp = obs.properties.timestamp;
                const utcDate = new Date(timestamp);
                const localDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000));
                
                const key = `${localDate.getFullYear()}-${(localDate.getMonth() + 1).toString().padStart(2, '0')}-${localDate.getDate().toString().padStart(2, '0')}-${localDate.getHours().toString().padStart(2, '0')}`;
                
                // Convert from meters to inches (NWS gives meters)
                const precipInches = obs.properties.precipitationLastHour.value * 39.3701;
                
                precipMap.set(key, {
                  value: precipInches,
                  originalTimestamp: timestamp
                });
              }
            });
            
            if (precipMap.size > 0) {
              console.log(`✅ NWS fallback successful for ${damName}: ${precipMap.size} precipitation readings`);
              return precipMap;
            }
          }
        }
      } catch (stationError) {
        console.log(`⚠️ Station ${stationId} failed: ${stationError.message}`);
        continue;
      }
    }
    
    console.log(`⚠️ No precipitation data available from NWS stations for ${damName}`);
    return new Map();
    
  } catch (error) {
    console.log(`⚠️ NWS precipitation fallback failed for ${damName}:`, error.message);
    return new Map();
  }
}

// Enhanced fetch USACE data function with NWS precipitation fallback and retry for critical data
async function fetchUSACEData(endpoint, parameter, damName, retryCount = 0) {
  const timeRange = getTimeRange();
  const encodedEndpoint = encodeURIComponent(endpoint);
  const url = `${USACE_API_BASE}?name=${encodedEndpoint}&begin=${timeRange.begin}&end=${timeRange.end}`;
  
  console.log(`📡 Fetching ${damName} ${parameter} data from USACE API${retryCount > 0 ? ` (retry ${retryCount})` : ''}...`);
  
  try {
    const response = await makeRequest(url);
    
    if (response.statusCode !== 200) {
      // If this is precipitation data and USACE fails, try NWS fallback
      if (parameter === 'Precipitation') {
        console.log(`⚠️ USACE precipitation API failed for ${damName}, trying NWS fallback...`);
        return await fetchNWSPrecipitation(damName, timeRange);
      }
      throw new Error(`USACE API returned status ${response.statusCode} for ${damName} ${parameter}`);
    }
    
    const data = JSON.parse(response.data);
    
    if (!data.values || !Array.isArray(data.values)) {
      // If this is precipitation data and no USACE data, try NWS fallback
      if (parameter === 'Precipitation') {
        console.log(`⚠️ No USACE precipitation values for ${damName}, trying NWS fallback...`);
        return await fetchNWSPrecipitation(damName, timeRange);
      }
      console.log(`⚠️ No values found for ${damName} ${parameter}`);
      return new Map();
    }
    
    console.log(`✅ Retrieved ${data.values.length} ${damName} ${parameter} data points`);
    
    // Convert to Map with timestamp keys
    const dataMap = new Map();
    data.values.forEach(([timestamp, value]) => {
      const utcDate = new Date(timestamp);
      const localDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000)); // Convert to Central Time
      
      const key = `${localDate.getFullYear()}-${(localDate.getMonth() + 1).toString().padStart(2, '0')}-${localDate.getDate().toString().padStart(2, '0')}-${localDate.getHours().toString().padStart(2, '0')}`;
      dataMap.set(key, { value, originalTimestamp: timestamp });
    });
    
    // Check if we got suspicious zero values for critical flow data and retry if needed
    const isCriticalFlowData = parameter === 'Inflow' || parameter === 'Total Outflow';
    if (isCriticalFlowData && retryCount < 2 && dataMap.size > 0) {
      // Get the most recent value to check
      const sortedKeys = Array.from(dataMap.keys()).sort().reverse();
      const latestValue = dataMap.get(sortedKeys[0])?.value;
      
      if (latestValue === 0 || latestValue === null || latestValue === undefined) {
        console.log(`⚠️ Got suspicious zero/null value for ${damName} ${parameter}: ${latestValue}, retrying...`);
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds before retry
        return await fetchUSACEData(endpoint, parameter, damName, retryCount + 1);
      }
    }
    
    return dataMap;
    
  } catch (error) {
    // If this is precipitation data, try NWS fallback
    if (parameter === 'Precipitation') {
      console.log(`⚠️ USACE precipitation error for ${damName}, trying NWS fallback:`, error.message);
      return await fetchNWSPrecipitation(damName, timeRange);
    }
    
    // Retry for critical flow data on error
    const isCriticalFlowData = parameter === 'Inflow' || parameter === 'Total Outflow';
    if (isCriticalFlowData && retryCount < 2) {
      console.log(`⚠️ Error fetching ${damName} ${parameter}, retrying in 2 seconds:`, error.message);
      await new Promise(resolve => setTimeout(resolve, 2000));
      return await fetchUSACEData(endpoint, parameter, damName, retryCount + 1);
    }
    
    console.log(`⚠️ Error fetching ${damName} ${parameter} data:`, error.message);
    return new Map();
  }
}

// Lake water temperature function
async function fetchLakeWaterTemperature(damName) {
  console.log(`🌡️ Fetching ${damName} lake water temperature...`);
  
  let temperatureUrl;
  if (damName === 'norfork') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-norfork-lake';
  } else if (damName === 'bullshoals') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-bull-shoals-lake';
  } else if (damName === 'greersferry') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-greers-ferry-lake';
  } else if (damName === 'tablerock') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-table-rock-lake';
  } else if (damName === 'beaver') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-beaver-lake';
  } else if (damName === 'clearwater') {
    temperatureUrl = 'https://seatemperature.net/lakes/water-temp-in-clearwater-lake';
  } else {
    console.log(`⚠️ No temperature URL configured for ${damName}`);
    return '0';
  }
  
  try {
    const response = await makeRequest(temperatureUrl);
    
    if (response.statusCode !== 200) {
      throw new Error(`Temperature site returned status ${response.statusCode}`);
    }
    
    let tempMatch = null;
    let temperature = null;
    
    // Try multiple patterns to extract temperature
    tempMatch = response.data.match(/(\d+)°F\s*\n\s*TODAY/);
    if (tempMatch) {
      temperature = parseInt(tempMatch[1]);
      console.log(`✅ Retrieved ${damName} lake water temperature: ${temperature}°F`);
    }
    
    if (!temperature) {
      tempMatch = response.data.match(/Current Lake Water Temperature Information\s*\n\s*(\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved ${damName} lake water temperature: ${temperature}°F`);
      }
    }
    
    if (!temperature) {
      tempMatch = response.data.match(/water temperature today in .* is (\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved ${damName} lake water temperature: ${temperature}°F`);
      }
    }
    
    if (temperature) {
      return temperature.toString();
    }
    
    console.log(`⚠️ Could not extract temperature for ${damName}, defaulting to 0`);
    return '0';
    
  } catch (error) {
    console.log(`⚠️ Error fetching ${damName} lake water temperature:`, error.message);
    return '0';
  }
}

// Fetch all USACE data for a specific dam
async function fetchAllUSACEData(damName) {
  console.log(`🚀 Fetching data from USACE CDA API for ${damName}...`);
  
  const endpoints = API_ENDPOINTS[damName];
  if (!endpoints) {
    throw new Error(`No API endpoints configured for dam: ${damName}`);
  }
  
  try {
    const promises = [
      fetchUSACEData(endpoints.waterLevel, 'Water Level', damName),
      fetchUSACEData(endpoints.inflow, 'Inflow', damName),
      fetchUSACEData(endpoints.totalOutflow, 'Total Outflow', damName),
      fetchUSACEData(endpoints.spillwayFlow, 'Spillway Flow', damName),
      fetchUSACEData(endpoints.storage, 'Storage', damName),
      fetchUSACEData(endpoints.powerGeneration, 'Power Generation', damName),
      fetchUSACEData(endpoints.precipitation, 'Precipitation', damName)
    ];
    
    // Add extra endpoints for Bull Shoals and Table Rock if available
    if (endpoints.tailwaterLevel) {
      promises.push(fetchUSACEData(endpoints.tailwaterLevel, 'Tailwater Level', damName));
    }
    if (endpoints.floodPoolPercent) {
      promises.push(fetchUSACEData(endpoints.floodPoolPercent, 'Flood Pool Percent', damName));
    }
    
    const results = await Promise.all(promises);
    
    const dataObject = {
      waterLevel: results[0],
      inflow: results[1],
      outflow: results[2],
      spillway: results[3],
      storage: results[4],
      power: results[5],
      precipitation: results[6]
    };
    
    // Add extra data for Bull Shoals and Table Rock
    if (results[7]) dataObject.tailwater = results[7];
    if (results[8]) dataObject.floodPoolPercent = results[8];
    
    return dataObject;
    
  } catch (error) {
    console.error(`❌ Failed to fetch USACE data for ${damName}:`, error.message);
    throw error;
  }
}

// Process rainfall data to calculate incremental values
function processIncrementalRainfall(precipitationMap, allTimestamps) {
  const filledMap = new Map(precipitationMap);
  const incrementalMap = new Map();
  const sortedTimestamps = Array.from(allTimestamps).sort(); // oldest first for processing
  
  let lastValue = null;
  let fillCount = 0;
  
  // First pass: forward-fill missing values
  sortedTimestamps.forEach(timestamp => {
    if (filledMap.has(timestamp)) {
      lastValue = filledMap.get(timestamp);
    } else if (lastValue !== null) {
      filledMap.set(timestamp, lastValue);
      fillCount++;
    }
  });
  
  // Second pass: calculate incremental rainfall
  let previousValue = null;
  let debugCount = 0;
  
  sortedTimestamps.forEach(timestamp => {
    if (filledMap.has(timestamp)) {
      const currentReading = filledMap.get(timestamp);
      const currentValue = typeof currentReading === 'object' ? currentReading.value : currentReading;
      
      if (previousValue !== null) {
        // Calculate incremental rainfall
        const increment = Math.max(0, currentValue - previousValue);
        incrementalMap.set(timestamp, {
          value: increment,
          originalTimestamp: typeof currentReading === 'object' ? currentReading.originalTimestamp : null
        });
        
        // Debug first few calculations
        if (debugCount < 5) {
          console.log(`🌧️ Rainfall debug ${timestamp}: current=${currentValue.toFixed(2)}, previous=${previousValue.toFixed(2)}, increment=${increment.toFixed(2)}`);
          debugCount++;
        }
      } else {
        // First reading - assume no previous rainfall
        incrementalMap.set(timestamp, {
          value: 0,
          originalTimestamp: typeof currentReading === 'object' ? currentReading.originalTimestamp : null
        });
        console.log(`🌧️ First rainfall reading ${timestamp}: ${currentValue.toFixed(2)} (setting to 0 for first hour)`);
      }
      
      previousValue = currentValue;
    }
  });
  
  console.log(`🌧️ Processed ${incrementalMap.size} rainfall increments (filled ${fillCount} gaps)`);
  return { incrementalMap, fillCount };
}

// Main data fetching function for a specific dam
const fetchDamData = async (damName) => {
  try {
    console.log(`🚀 Fetching ${damName} Dam data using enhanced API method...`);

    const [usaceData, lakeTemperature] = await Promise.all([
      fetchAllUSACEData(damName),
      fetchLakeWaterTemperature(damName)
    ]);

    // Get all unique timestamps
    const allTimestamps = new Set();
    Object.values(usaceData).forEach(dataMap => {
      dataMap.forEach((_, timestamp) => {
        allTimestamps.add(timestamp);
      });
    });

    if (allTimestamps.size === 0) {
      console.log(`❌ No data available from USACE APIs for ${damName}`);
      return null;
    }

    // Process rainfall data to calculate incremental values (needs chronological order)
    const { incrementalMap: incrementalPrecipitation } = processIncrementalRainfall(usaceData.precipitation, allTimestamps);

    const sortedTimestamps = Array.from(allTimestamps).sort().reverse(); // newest first for display
    console.log(`📊 Processing ${sortedTimestamps.length} hourly data points for ${damName}...`);

    const specs = damSpecs[damName];
    const allDataPoints = [];

    // Process each timestamp
    for (const timestamp of sortedTimestamps) {
      const [year, month, day, hour] = timestamp.split('-');

      const waterLevelData = usaceData.waterLevel.get(timestamp);
      const inflowData = usaceData.inflow.get(timestamp);
      const totalOutflowData = usaceData.outflow.get(timestamp);
      const spillwayData = usaceData.spillway.get(timestamp);
      const precipitationData = incrementalPrecipitation.get(timestamp);
      const powerData = usaceData.power.get(timestamp);
      const storageData = usaceData.storage.get(timestamp);

      const waterLevel = waterLevelData ? waterLevelData.value : null;
      const inflow = inflowData ? inflowData.value : 0;
      const totalOutflow = totalOutflowData ? totalOutflowData.value : 0;
      const spillwayFlow = spillwayData ? spillwayData.value : 0;
      const powerGen = powerData ? powerData.value : 0;
      const storageAcreFeet = storageData ? storageData.value : 0;
      const precipitation = precipitationData ? precipitationData.value : 0;

      // Skip if no water level data
      if (!waterLevel || waterLevel <= 0) {
        console.log(`⚠️ Skipping ${damName} ${timestamp} - no water level data`);
        continue;
      }

      // Calculate derived values
      const storagePercentage = calculateStoragePercentage(waterLevel, specs, damName);
      const liveStorage = Math.round(storageAcreFeet).toLocaleString();
      const turbineFlow = Math.max(0, totalOutflow - spillwayFlow);
      const netFlow = Math.round(inflow - totalOutflow);
      const turbineEfficiency = turbineFlow > 0 ? (powerGen / turbineFlow).toFixed(3) : '0.000';
      
      const hasForwardFilledRainfall = !usaceData.precipitation.has(timestamp) && incrementalPrecipitation.has(timestamp);

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
      console.log(`❌ No valid hourly data points could be processed for ${damName}`);
      return null;
    }

    console.log(`✅ Successfully processed ${allDataPoints.length} hourly data points for ${damName}`);

    // Get display name and coordinates
    const displayName = Names[damName.toUpperCase().replace('BULLSHOALS', 'BULL_SHOALS').replace('GREERSFERRY', 'GREERS_FERRY').replace('TABLEROCK', 'TABLE_ROCK')] || damName;
    const coordinates = damCoordinates[damName];

    const damData = {
      id: damName === 'norfork' ? '1' : damName === 'bullshoals' ? '2' : damName === 'greersferry' ? '3' : damName === 'tablerock' ? '4' : damName === 'beaver' ? '5' : '6',
      name: displayName,
      officialName: damName.toUpperCase().replace('BULLSHOALS', 'BULL_SHOALS').replace('GREERSFERRY', 'GREERS_FERRY').replace('TABLEROCK', 'TABLE_ROCK'),
      MWL: specs.MWL,
      FRL: specs.FRL,
      liveStorageAtFRL: specs.liveStorageAtFRL,
      ruleLevel: specs.ruleLevel,
      blueLevel: specs.blueLevel,
      orangeLevel: specs.orangeLevel,
      redLevel: specs.redLevel,
      latitude: coordinates.latitude,
      longitude: coordinates.longitude,
      data: allDataPoints
    };

    console.log(`✅ ${damName} Water Level Range: ${allDataPoints[0].waterLevel} - ${allDataPoints[allDataPoints.length-1].waterLevel} ft MSL`);
    console.log(`✅ ${damName} Storage Range: ${allDataPoints[0].storagePercentage} - ${allDataPoints[allDataPoints.length-1].storagePercentage} of capacity`);
    
    return damData;

  } catch (error) {
    console.error(`Error fetching ${damName} Dam data:`, error);
    return null;
  }
};

// Main function to fetch all dam details
async function fetchDamDetails() {
  try {
    console.log('🚀 Starting Enhanced Multi-Dam scraper...');
    
    // Create folder if it doesn't exist
    try {
      await fs.access(folderName);
      console.log('✅ historic_data folder exists');
    } catch (error) {
      console.log('📁 Creating historic_data folder...');
      await fs.mkdir(folderName);
    }

    // Fetch data for all dams
    const damNames = ['norfork', 'bullshoals', 'greersferry', 'tablerock', 'beaver', 'clearwater'];
    const dams = [];
    
    for (const damName of damNames) {
      console.log(`\n=== Processing ${damName.toUpperCase()} Dam ===`);
      const damData = await fetchDamData(damName);
      if (damData) {
        dams.push(damData);
      }
    }

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
          // Handle both naming conventions: "Bull_Shoals.json" and "Bull Shoals.json"
          const fileNameWithoutExt = file.replace('.json', '');
          const damName = fileNameWithoutExt.replace(/_/g, ' '); // Convert underscores to spaces for consistent internal naming
          
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
      console.log(`\n🔄 Processing dam: ${newDam.name} (ID: ${newDam.id})`);
      console.log(`📋 Dam object keys:`, Object.keys(newDam));
      console.log(`📋 Dam data points:`, newDam.data.length);
      const existingDam = existingData[newDam.name];

      if (existingDam) {
        console.log(`📊 Found existing data for ${newDam.name} with ${existingDam.data.length} existing records`);
        
        let newPointsAdded = 0;
        let pointsUpdated = 0;
        
        for (const newDataPoint of newDam.data) {
          const newTimestamp = newDataPoint.timestamp;
          const existingIndex = existingDam.data.findIndex(d => d.timestamp === newTimestamp);

          if (existingIndex === -1) {
            console.log(`➕ Adding new hourly data: ${newDam.name} ${newDataPoint.date} ${newDataPoint.time}`);
            
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
              console.log(`   🔄 Updating ${newDam.name} ${newDataPoint.date} ${newDataPoint.time} with more complete data`);
              existingDam.data[existingIndex] = newEntry;
              pointsUpdated++;
              dataChanged = true;
            }
          }
        }
        
        if (newPointsAdded > 0 || pointsUpdated > 0) {
          console.log(`   📈 ${newDam.name}: Added ${newPointsAdded} new records, updated ${pointsUpdated} existing records`);
          
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
          console.log(`   ⏭️  ${newDam.name}: All hourly data already exists and is complete`);
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
      console.log('🔍 Debug - existingData keys:', Object.keys(existingData));
      console.log('🔍 Debug - existingData contents:');
      for (const [key, value] of Object.entries(existingData)) {
        console.log(`   - ${key}: ${value.name || 'NO NAME'} (${value.data ? value.data.length : 'NO DATA'} records)`);
      }
      
      // Save individual dam files
      for (const [damName, damData] of Object.entries(existingData)) {
        console.log(`💾 Saving file for dam: ${damName}`);
        console.log(`📋 Dam data structure:`, {
          name: damData.name,
          dataPoints: damData.data ? damData.data.length : 'NO DATA ARRAY',
          keys: Object.keys(damData)
        });
        
        // Create filename without spaces for consistency
        const safeFileName = damName.replace(/\s+/g, '_');
        const filename = `${folderName}/${safeFileName}.json`;
        
        try {
          await fs.writeFile(filename, JSON.stringify(damData, null, 4));
          console.log(`✅ Details for dam ${damName} saved successfully in ${filename}.`);
          
          const stats = await fs.stat(filename);
          console.log(`📁 File size: ${stats.size} bytes`);
        } catch (writeError) {
          console.error(`❌ Error writing ${filename}:`, writeError);
        }
      }

      // Save live JSON file with ONLY THE LATEST data from all dams
      try {
        // Load existing live.json if it exists
        let existingLiveData = { lastUpdate: '', dams: [] };
        try {
          const liveJsonContent = await fs.readFile('live.json', 'utf8');
          existingLiveData = JSON.parse(liveJsonContent);
          console.log(`📄 Loaded existing live.json with ${existingLiveData.dams.length} dams`);
        } catch (readError) {
          console.log('📝 No existing live.json found, creating new one...');
        }

        // Create a map of existing dams by ID for easy lookup
        const existingDamsById = new Map();
        existingLiveData.dams.forEach(dam => {
          existingDamsById.set(dam.id, dam);
        });

        // Update/add only the dams we successfully fetched new data for
        const newlyFetchedDams = dams.map(dam => ({
          id: dam.id,
          name: dam.name,
          officialName: dam.officialName,
          MWL: dam.MWL,
          FRL: dam.FRL,
          liveStorageAtFRL: dam.liveStorageAtFRL,
          ruleLevel: dam.ruleLevel,
          blueLevel: dam.blueLevel,
          orangeLevel: dam.orangeLevel,
          redLevel: dam.redLevel,
          latitude: dam.latitude,
          longitude: dam.longitude,
          data: [dam.data[0]] // ONLY the latest entry (first in array since sorted newest first)
        }));

        // Update existing dams with new data, or add them if they don't exist
        newlyFetchedDams.forEach(newDam => {
          existingDamsById.set(newDam.id, newDam);
          console.log(`✅ Updated/added ${newDam.name} in live data`);
        });

        // Convert back to array, sorted by ID
        const updatedDams = Array.from(existingDamsById.values()).sort((a, b) => parseInt(a.id) - parseInt(b.id));

        const liveData = {
          lastUpdate: dams.length > 0 ? dams[0].data[0].date : existingLiveData.lastUpdate,
          dams: updatedDams
        };
        
        await fs.writeFile('live.json', JSON.stringify(liveData, null, 4));
        console.log(`✅ Live dam data saved successfully in live.json (${updatedDams.length} total dams, ${newlyFetchedDams.length} updated).`);
        
        const liveStats = await fs.stat('live.json');
        console.log(`📁 Live file size: ${liveStats.size} bytes`);
      } catch (liveError) {
        console.error('❌ Error writing live.json:', liveError);
      }
    } else {
      console.log('⏸️  No new data to save.');
    }

  } catch (error) {
    console.error('💥 Error in fetchDamDetails:', error);
  }
}

// Run the scraper
if (require.main === module) {
  fetchDamDetails().then(() => {
    console.log('Enhanced Multi-Dam scraper completed successfully.');
  }).catch(error => {
    console.error('Scraper failed:', error);
  });
}

module.exports = {
  fetchDamDetails,
  fetchDamData,
  fetchAllUSACEData,
  API_ENDPOINTS
};
