const fs = require('fs').promises;
const https = require('https');

const folderName = 'historic_data';

// Norfork Dam coordinates and specifications (from original)
const damCoordinates = {
  'norfork': { latitude: 36.2483333, longitude: -92.24 }
};

// Map official names to display names (from original)
const Names = {
  'NORFORK': 'Norfork'
};

// Dam specifications (from original)
const damSpecs = {
  'norfork': {
    MWL: '580.00', // Updated to match new script
    MWLUnit: 'ft',
    FRL: '552.00',
    FRLUnit: 'ft',
    floodPool: '580.00',
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '1,888,448', // Updated to match new script
    liveStorageAtFRLUnit: 'acre-ft',
    liveStorageAtFloodPool: '2,580,000',
    ruleLevel: '510.00', // Updated
    ruleLevelUnit: 'ft',
    blueLevel: '552.00',
    blueLevelUnit: 'ft',
    orangeLevel: '570.00', // Updated
    orangeLevelUnit: 'ft',
    redLevel: '580.00', // Updated
    redLevelUnit: 'ft',
    deadStorageLevel: '380.00',
    deadStorageLevelUnit: 'ft',
    surfaceArea: '22,000',
    surfaceAreaUnit: 'acres'
  }
};

// USACE CDA API endpoints (from new script)
const USACE_API_BASE = 'https://water.usace.army.mil/cda/reporting/providers/swl/timeseries';

const API_ENDPOINTS = {
  waterLevel: 'Norfork_Dam-Headwater.Elev.Inst.1Hour.0.Decodes-rev',
  inflow: 'Norfork_Dam.Flow-Res In.Ave.1Hour.1Hour.6hr-RunAve-A2W',
  totalOutflow: 'Norfork_Dam.Flow-Res Out.Ave.1Hour.1Hour.Regi-Comp',
  spillwayFlow: 'Norfork_Dam.Flow-Tainter Total.Ave.1Hour.1Hour.Regi-Comp',
  storage: 'Norfork_Dam-Headwater.Stor-Res.Inst.1Hour.0.CCP-Comp',
  powerGeneration: 'Norfork_Dam-House_Unit.Energy-Gen.Total.1Hour.1Hour.Decodes-rev',
  precipitation: 'Norfork_Dam.Precip-Cum.Inst.1Hour.0.Decodes-rev'
};

// Storage calculation function (from original with improvements)
const calculateStoragePercentage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return '0.00';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL); // 552.00 ft (conservation pool)
  const floodPool = parseFloat(specs.floodPool); // 580.00 ft (100% full)
  const mwl = parseFloat(specs.MWL); // 590.00 ft (maximum)
  const deadLevel = parseFloat(specs.deadStorageLevel); // 380.00 ft
  
  if (level <= deadLevel) {
    return '0.00'; // Dead storage
  } else if (level <= floodPool) {
    // Below flood pool (0% to 100%)
    const depthRatio = (level - deadLevel) / (floodPool - deadLevel);
    const storageRatio = Math.pow(depthRatio, 2.2); // Exponential curve for reservoir
    const percentage = storageRatio * 100;
    return Math.max(0, Math.min(100, percentage)).toFixed(2);
  } else {
    // Above flood pool (100%+) - emergency/surcharge storage
    const baseStorage = 100; // 100% at flood pool (580 ft)
    const surchargeDepth = level - floodPool;
    const maxSurchargeDepth = mwl - floodPool; // 10 ft above flood pool
    
    if (maxSurchargeDepth > 0) {
      const surchargeRatio = Math.min(1, surchargeDepth / maxSurchargeDepth);
      const additionalSurcharge = surchargeRatio * 15; // Up to 15% additional in emergency storage
      return Math.min(115, baseStorage + additionalSurcharge).toFixed(2);
    }
    
    return '100.00';
  }
};

// Live storage calculation (from original)
const calculateLiveStorage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return '0';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL); // 552.00 ft (conservation pool)
  const floodPool = parseFloat(specs.floodPool); // 580.00 ft (flood pool)
  const deadLevel = parseFloat(specs.deadStorageLevel); // 380.00 ft
  
  // Estimate storage at flood pool (580 ft) based on conservation pool data
  const conservationStorage = 1983000; // acre-feet at 552 ft
  const floodPoolStorage = 2580000; // Estimated acre-feet at 580 ft (flood pool)
  
  if (level <= deadLevel) {
    return '0';
  } else if (level <= floodPool) {
    // Below flood pool - exponential curve
    const depthRatio = (level - deadLevel) / (floodPool - deadLevel);
    const storageRatio = Math.pow(depthRatio, 2.2);
    const currentStorage = Math.round(floodPoolStorage * storageRatio);
    return currentStorage.toLocaleString();
  } else {
    // Above flood pool - emergency storage
    const baseStorage = floodPoolStorage; // Storage at 580 ft
    const surchargeDepth = level - floodPool;
    const maxSurchargeDepth = 10; // Emergency storage depth
    
    if (surchargeDepth > 0 && maxSurchargeDepth > 0) {
      const surchargeRatio = Math.min(1, surchargeDepth / maxSurchargeDepth);
      const additionalSurcharge = Math.round(floodPoolStorage * 0.15 * surchargeRatio);
      return (baseStorage + additionalSurcharge).toLocaleString();
    }
    
    return floodPoolStorage.toLocaleString();
  }
};

// HTTP request function (from new script)
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

// Time range function (from new script)
function getTimeRange() {
  const end = new Date();
  const start = new Date(end.getTime() - (7 * 24 * 60 * 60 * 1000)); // 7 days for more recent data
  
  return {
    begin: start.toISOString(),
    end: end.toISOString()
  };
}

// Fetch USACE data function (from new script)
async function fetchUSACEData(endpoint, parameter) {
  const timeRange = getTimeRange();
  const encodedEndpoint = encodeURIComponent(endpoint);
  const url = `${USACE_API_BASE}?name=${encodedEndpoint}&begin=${timeRange.begin}&end=${timeRange.end}`;
  
  console.log(`📡 Fetching ${parameter} data from USACE API...`);
  
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
    
    // Show timezone example for debugging 
    if (data.values.length > 0) {
      const firstTimestamp = data.values[0][0];
      const utcDate = new Date(firstTimestamp);
      const localDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000));
      console.log(`   🕐 Latest: ${firstTimestamp} (UTC) → ${localDate.getHours()}:${localDate.getMinutes().toString().padStart(2, '0')} Central`);
    }
    
    // Convert to Map with timestamp keys for easy lookup
    const dataMap = new Map();
    data.values.forEach(([timestamp, value]) => {
      const utcDate = new Date(timestamp); // API timestamp in UTC
      const localDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000)); // Convert to Central Time (CDT = UTC-5)
      
      // Create key using LOCAL time for matching: YYYY-MM-DD-HH
      const key = `${localDate.getFullYear()}-${(localDate.getMonth() + 1).toString().padStart(2, '0')}-${localDate.getDate().toString().padStart(2, '0')}-${localDate.getHours().toString().padStart(2, '0')}`;
      dataMap.set(key, { value, originalTimestamp: timestamp }); // Store both value and original UTC timestamp
    });
    
    return dataMap;
    
  } catch (error) {
    console.log(`⚠️ Error fetching ${parameter} data:`, error.message);
    return new Map();
  }
}

// Lake water temperature function (from new script)
async function fetchLakeWaterTemperature() {
  console.log('🌡️ Fetching lake water temperature...');
  
  try {
    const response = await makeRequest('https://seatemperature.net/lakes/water-temp-in-norfork-lake');
    
    if (response.statusCode !== 200) {
      throw new Error(`Temperature site returned status ${response.statusCode}`);
    }
    
    // Try multiple patterns to extract temperature
    let tempMatch = null;
    let temperature = null;
    
    // Pattern 1: "66°F\nTODAY" (temperature before TODAY)
    tempMatch = response.data.match(/(\d+)°F\s*\n\s*TODAY/);
    if (tempMatch) {
      temperature = parseInt(tempMatch[1]);
      console.log(`✅ Retrieved lake water temperature: ${temperature}°F (Pattern: temp°F\\nTODAY)`);
    }
    
    // Pattern 2: Try "Current Lake Water Temperature Information\n66°F"
    if (!temperature) {
      tempMatch = response.data.match(/Current Lake Water Temperature Information\s*\n\s*(\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved lake water temperature: ${temperature}°F (Pattern: after header)`);
      }
    }
    
    // Pattern 3: Try "water temperature today in Norfork Lake is 66°F"
    if (!temperature) {
      tempMatch = response.data.match(/water temperature today in Norfork Lake is (\d+)°F/);
      if (tempMatch) {
        temperature = parseInt(tempMatch[1]);
        console.log(`✅ Retrieved lake water temperature: ${temperature}°F (Pattern: in sentence)`);
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

// Fetch all USACE data (from new script)
async function fetchAllUSACEData() {
  console.log('🚀 Fetching data from USACE CDA API...');
  
  try {
    // Fetch all endpoints in parallel for speed
    const [
      waterLevelData,
      inflowData, 
      outflowData,
      spillwayData,
      storageData,
      powerData,
      precipData
    ] = await Promise.all([
      fetchUSACEData(API_ENDPOINTS.waterLevel, 'Water Level'),
      fetchUSACEData(API_ENDPOINTS.inflow, 'Inflow'),
      fetchUSACEData(API_ENDPOINTS.totalOutflow, 'Total Outflow'),
      fetchUSACEData(API_ENDPOINTS.spillwayFlow, 'Spillway Flow'),
      fetchUSACEData(API_ENDPOINTS.storage, 'Storage'),
      fetchUSACEData(API_ENDPOINTS.powerGeneration, 'Power Generation'),
      fetchUSACEData(API_ENDPOINTS.precipitation, 'Precipitation')
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
    console.error('❌ Failed to fetch USACE data:', error.message);
    throw error;
  }
}

// Forward-fill rainfall data only (from new script)
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

// Main data fetching function (combined approach)
const fetchNorforkDamData = async () => {
  try {
    console.log('🚀 Fetching Norfork Dam data using enhanced API method...');

    // Fetch data from USACE APIs and lake temperature
    const [usaceData, lakeTemperature] = await Promise.all([
      fetchAllUSACEData(),
      fetchLakeWaterTemperature()
    ]);

    // Get all unique timestamps
    const allTimestamps = new Set();
    Object.values(usaceData).forEach(dataMap => {
      dataMap.forEach((_, timestamp) => {
        allTimestamps.add(timestamp);
      });
    });

    if (allTimestamps.size === 0) {
      console.log('❌ No data available from USACE APIs');
      return { dams: [] };
    }

    // Forward-fill rainfall data only
    const { filledMap: filledPrecipitation } = forwardFillRainfall(usaceData.precipitation, allTimestamps);

    // Process ALL hourly data points, not just the latest
    const sortedTimestamps = Array.from(allTimestamps).sort().reverse(); // newest first
    
    if (sortedTimestamps.length === 0) {
      console.log('❌ No data available from USACE APIs');
      return { dams: [] };
    }

    console.log(`📊 Processing ${sortedTimestamps.length} hourly data points...`);

    const specs = damSpecs.norfork;
    const allDataPoints = [];

    // Process each timestamp to create data points
    for (const timestamp of sortedTimestamps) {
      const [year, month, day, hour] = timestamp.split('-');

      // Get values for this timestamp - now they include original UTC timestamps
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

      // Skip if no water level data (essential field)
      if (!waterLevel || waterLevel <= 0) {
        console.log(`⚠️ Skipping ${timestamp} - no water level data`);
        continue;
      }

      // Calculate derived values
      const storagePercentage = calculateStoragePercentage(waterLevel, specs);
      const liveStorage = Math.round(storageAcreFeet).toLocaleString();
      const turbineFlow = Math.max(0, totalOutflow - spillwayFlow);
      const netFlow = Math.round(inflow - totalOutflow);
      const turbineEfficiency = turbineFlow > 0 ? (powerGen / turbineFlow).toFixed(3) : '0.000';
      
      // Check if rainfall was forward-filled
      const hasForwardFilledRainfall = !usaceData.precipitation.has(timestamp) && filledPrecipitation.has(timestamp);

      // Use the original UTC timestamp from the API
      const originalUTCTimestamp = waterLevelData ? waterLevelData.originalTimestamp : 
                                   inflowData ? inflowData.originalTimestamp :
                                   totalOutflowData ? totalOutflowData.originalTimestamp :
                                   `${year}-${month}-${day}T${(parseInt(hour) + 5).toString().padStart(2, '0')}:00:00.000Z`;

      const dataPoint = {
        date: `${day}.${month}.${year}`,
        time: `${hour.padStart(2, '0')}:00`, // Local Central Time
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
        timestamp: originalUTCTimestamp, // Original UTC timestamp from API
        netFlow: netFlow,
        turbineEfficiency: turbineEfficiency,
        hasForwardFilledRainfall: hasForwardFilledRainfall,
        lakeWaterTemp: `${lakeTemperature}°F`,
        lakeWaterTempSource: 'SeaTemperature.net (Estimated)'
      };

      allDataPoints.push(dataPoint);
    }

    if (allDataPoints.length === 0) {
      console.log('❌ No valid hourly data points could be processed');
      return { dams: [] };
    }

    console.log(`✅ Successfully processed ${allDataPoints.length} hourly data points`);

    // Create dam data structure with ALL data points
    const damData = {
      id: '1',
      name: Names.NORFORK,
      officialName: 'NORFORK',
      MWL: specs.MWL,
      FRL: specs.FRL,
      liveStorageAtFRL: specs.liveStorageAtFRL,
      ruleLevel: specs.ruleLevel,
      blueLevel: specs.blueLevel,
      orangeLevel: specs.orangeLevel,
      redLevel: specs.redLevel,
      latitude: damCoordinates.norfork.latitude,
      longitude: damCoordinates.norfork.longitude,
      data: allDataPoints // All hourly data points, not just the latest
    };

    console.log(`✅ Water Level Range: ${allDataPoints[0].waterLevel} - ${allDataPoints[allDataPoints.length-1].waterLevel} ft MSL`);
    console.log(`✅ Storage Range: ${allDataPoints[0].storagePercentage} - ${allDataPoints[allDataPoints.length-1].storagePercentage} of capacity`);
    console.log(`✅ Time Range: ${allDataPoints[allDataPoints.length-1].time} to ${allDataPoints[0].time} on ${allDataPoints[0].date}`);
    console.log(`✅ Total Data Points: ${allDataPoints.length} hourly records`);
    
    if (allDataPoints.some(d => parseFloat(d.spillwayRelease) > 0)) {
      console.log(`⚠️  Spillway releases detected in data range`);
    }

    return { dams: [damData] };

  } catch (error) {
    console.error('Error fetching Norfork Dam data:', error);
    return { dams: [] };
  }
};

// Main function to fetch dam details and update data files (from original)
async function fetchDamDetails() {
  try {
    console.log('🚀 Starting Enhanced Norfork Dam scraper...');
    
    // Create folder if it doesn't exist
    try {
      await fs.access(folderName);
      console.log('✅ historic_data folder exists');
    } catch (error) {
      console.log('📁 Creating historic_data folder...');
      await fs.mkdir(folderName);
    }

    console.log('Processing Norfork Dam data...');
    const { dams } = await fetchNorforkDamData();

    console.log(`📊 Retrieved ${dams.length} dam(s) from data sources`);

    if (dams.length === 0) {
      console.log('❌ No dam data found - cannot create files.');
      return;
    }

    // Debug: Show what data we have
    const dam = dams[0];
    console.log('🔍 Dam data preview:');
    console.log('- Name:', dam.name);
    console.log('- Water Level:', dam.data[0].waterLevel);
    console.log('- Date:', dam.data[0].date);
    console.log('- Rainfall:', dam.data[0].rainfall);
    console.log('- Data Source:', dam.data[0].dataSource);

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
      console.log(`🔄 Processing dam: ${newDam.name}`);
      const existingDam = existingData[newDam.name];

      if (existingDam) {
        console.log(`📊 Found existing data for ${newDam.name} with ${existingDam.data.length} existing records`);
        
        let newPointsAdded = 0;
        let pointsUpdated = 0;
        
        // Process each new data point
        for (const newDataPoint of newDam.data) {
          const newTimestamp = newDataPoint.timestamp;
          const existingIndex = existingDam.data.findIndex(d => d.timestamp === newTimestamp);

          if (existingIndex === -1) {
            // New timestamp - add it in chronological order (newest first)
            console.log(`➕ Adding new hourly data: ${newDataPoint.date} ${newDataPoint.time}`);
            
            // Find correct position to insert (maintain newest-first order)
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
            // Timestamp exists - check if new data is more complete
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
          
          // Update dam specifications
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

    console.log(`📈 Data changed: ${dataChanged}`);

    if (dataChanged) {
      console.log('💾 Saving files...');
      
      // Save individual dam files
      for (const [damName, damData] of Object.entries(existingData)) {
        const filename = `${folderName}/${damName}.json`;
        
        try {
          await fs.writeFile(filename, JSON.stringify(damData, null, 4));
          console.log(`✅ Details for dam ${damName} saved successfully in ${filename}.`);
          
          // Verify the file was created
          const stats = await fs.stat(filename);
          console.log(`📁 File size: ${stats.size} bytes`);
        } catch (writeError) {
          console.error(`❌ Error writing ${filename}:`, writeError);
        }
      }

      // Save live JSON file with most recent data
      try {
        const liveData = {
          lastUpdate: dams[0].data[0].date,
          dams
        };
        await fs.writeFile('live.json', JSON.stringify(liveData, null, 4));
        console.log('✅ Live dam data saved successfully in live.json.');
        
        // Verify live.json was created
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
    console.log('Enhanced Norfork Dam scraper completed successfully.');
  }).catch(error => {
    console.error('Scraper failed:', error);
  });
}

module.exports = {
  fetchDamDetails,
  fetchNorforkDamData,
  fetchAllUSACEData,
  API_ENDPOINTS
};
