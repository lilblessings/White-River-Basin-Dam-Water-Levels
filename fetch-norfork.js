const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;

const folderName = 'historic_data';

// Norfork Dam coordinates and specifications
const damCoordinates = {
  'norfork': { latitude: 36.2483333, longitude: -92.24 }
};

// Map official names to display names
const Names = {
  'NORFORK': 'Norfork'
};

// Dam specifications based on US Army Corps of Engineers and USGS data
// All values in imperial units for US dams
const damSpecs = {
  'norfork': {
    MWL: '580.00', // Maximum Water Level (top of dam) - feet
    MWLUnit: 'ft',
    FRL: '552.00', // Full Recreation Level (normal conservation pool) - feet
    FRLUnit: 'ft',
    floodPool: '580.00', // Top of flood pool - feet
    floodPoolUnit: 'ft',
    liveStorageAtFRL: '2,580,000', // acre-feet at conservation pool (552 ft)
    liveStorageAtFRLUnit: 'acre-ft',
    ruleLevel: '510.00', // Rule curve level - feet
    ruleLevelUnit: 'ft',
    blueLevel: '552.00', // Recreation level - feet
    blueLevelUnit: 'ft',
    orangeLevel: '565.00', // Action level - feet
    orangeLevelUnit: 'ft',
    redLevel: '570.00', // Flood pool - feet
    redLevelUnit: 'ft',
    deadStorageLevel: '380.00', // Approximate dead storage level - feet
    deadStorageLevelUnit: 'ft',
    surfaceArea: '22,000', // acres at normal pool
    surfaceAreaUnit: 'acres'
  }
};

// More accurate storage calculation based on Corps data and research
// 100% = Flood Pool (580 ft), not Conservation Pool (552 ft)
const calculateStoragePercentage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return '0.00';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL); // 552.00 ft (conservation pool)
  const floodPool = parseFloat(specs.floodPool); // 580.00 ft (100% full)
  const mwl = parseFloat(specs.MWL); // 590.00 ft (maximum)
  const deadLevel = parseFloat(specs.deadStorageLevel); // 380.00 ft
  
  // 100% = 580 ft (flood pool), not 552 ft (conservation pool)
  
  if (level <= deadLevel) {
    return '0.00'; // Dead storage
  } else if (level <= floodPool) {
    // Below flood pool (0% to 100%)
    // Use exponential storage curve approximation
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

// Live storage calculation with 580 ft as 100% reference
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

// Helper function to extract numeric values from Corps data
const extractNumericValue = (text) => {
  if (!text) return null;
  
  // Remove common units and extract number
  const cleaned = text.replace(/[^\d.-]/g, '');
  const number = parseFloat(cleaned);
  
  return isNaN(number) ? null : number.toString();
};

// Fetch rainfall data with multiple fallback options
const fetchRainfallData = async () => {
  // Try Weather.gov API first, then fallback to simpler options
  
  try {
    console.log('Fetching rainfall data from Weather.gov...');
    
    // Use more precise Norfork Dam coordinates
    const lat = 36.2483;
    const lon = -92.2400;
    
    console.log(`Trying coordinates: ${lat}, ${lon}`);
    
    // Get current weather data
    const weatherUrl = `https://api.weather.gov/points/${lat},${lon}`;
    
    const pointResponse = await axios.get(weatherUrl, {
      headers: {
        'User-Agent': 'NorforkDamScraper/1.0 (dam-monitoring@example.com)',
        'Accept': 'application/json'
      },
      timeout: 8000
    });

    console.log('✅ Got point data from Weather.gov');
    const observationStationsUrl = pointResponse.data.properties.observationStations;
    
    // Get observation stations
    const stationsResponse = await axios.get(observationStationsUrl, {
      headers: {
        'User-Agent': 'NorforkDamScraper/1.0 (dam-monitoring@example.com)',
        'Accept': 'application/json'
      },
      timeout: 8000
    });

    if (stationsResponse.data.features && stationsResponse.data.features.length > 0) {
      // Get the nearest station
      const nearestStation = stationsResponse.data.features[0].id;
      console.log('Using weather station:', nearestStation);
      
      // Get latest observations
      const observationsUrl = `https://api.weather.gov/stations/${nearestStation}/observations/latest`;
      
      const obsResponse = await axios.get(observationsUrl, {
        headers: {
          'User-Agent': 'NorforkDamScraper/1.0 (dam-monitoring@example.com)',
          'Accept': 'application/json'
        },
        timeout: 8000
      });

      const observation = obsResponse.data.properties;
      
      // Get precipitation data (usually in mm, convert to inches)
      let rainfall = '0';
      
      if (observation.precipitationLastHour && observation.precipitationLastHour.value !== null) {
        // Convert mm to inches (1 mm = 0.0393701 inches)
        const rainfallMm = observation.precipitationLastHour.value;
        const rainfallInches = (rainfallMm * 0.0393701).toFixed(2);
        rainfall = rainfallInches;
        console.log(`Found rainfall: ${rainfallMm} mm (${rainfallInches} inches) in last hour`);
      } else if (observation.precipitationLast3Hours && observation.precipitationLast3Hours.value !== null) {
        // Use 3-hour precipitation if 1-hour not available
        const rainfallMm = observation.precipitationLast3Hours.value;
        const rainfallInches = (rainfallMm * 0.0393701).toFixed(2);
        rainfall = rainfallInches;
        console.log(`Found rainfall: ${rainfallMm} mm (${rainfallInches} inches) in last 3 hours`);
      } else if (observation.precipitationLast6Hours && observation.precipitationLast6Hours.value !== null) {
        // Use 6-hour precipitation as fallback
        const rainfallMm = observation.precipitationLast6Hours.value;
        const rainfallInches = (rainfallMm * 0.0393701).toFixed(2);
        rainfall = rainfallInches;
        console.log(`Found rainfall: ${rainfallMm} mm (${rainfallInches} inches) in last 6 hours`);
      } else {
        console.log('No recent precipitation data available from Weather.gov');
        rainfall = '0';
      }

      return rainfall;
    }

    console.log('No weather stations found, trying fallback...');
    throw new Error('No stations available');

  } catch (weatherGovError) {
    console.log(`Weather.gov failed (${weatherGovError.message}), trying fallback...`);
    
    // Fallback 1: Try USGS water data (sometimes includes precipitation)
    try {
      console.log('Trying USGS water data as fallback...');
      
      // USGS site near Norfork Dam
      const usgsUrl = 'https://waterservices.usgs.gov/nwis/iv/?format=json&sites=07055875&parameterCd=00045&period=P1D';
      
      const usgsResponse = await axios.get(usgsUrl, {
        timeout: 5000
      });
      
      if (usgsResponse.data && usgsResponse.data.value && usgsResponse.data.value.timeSeries) {
        const timeSeries = usgsResponse.data.value.timeSeries[0];
        if (timeSeries && timeSeries.values && timeSeries.values[0] && timeSeries.values[0].value) {
          const latestValue = timeSeries.values[0].value[0];
          if (latestValue && latestValue.value) {
            const rainfall = parseFloat(latestValue.value).toFixed(2);
            console.log(`Found rainfall from USGS: ${rainfall} inches`);
            return rainfall;
          }
        }
      }
      
      throw new Error('No USGS data available');
      
    } catch (usgsError) {
      console.log(`USGS fallback failed (${usgsError.message})`);
      
      // Fallback 2: Check for any existing weather pattern or default to 0
      console.log('All weather services failed, defaulting to 0');
      return '0';
    }
  }
};

// Fetch data from Army Corps of Engineers official Norfork Dam page
const fetchCorpsData = async () => {
  try {
    console.log('Fetching Army Corps of Engineers data...');
    
    const corpsUrl = 'https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/norfork.htm';
    
    const response = await axios.get(corpsUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
      },
      timeout: 10000
    });

    const html = response.data;
    const $ = cheerio.load(html);

    console.log('Successfully fetched Corps page, parsing data...');
    
    // Get the full text content since it's pre-formatted text, not tables
    const pageText = $('body').text();
    console.log('Full page text length:', pageText.length);

    // Initialize data structure
    let corpsData = {
      poolElevation: null,
      tailwaterElevation: null,
      spillwayRelease: null,
      powerHouseDischarge: null,
      totalOutflow: null,
      powerGeneration: null,
      inflow: null,
      changeIn24Hours: null,
      lastUpdate: null
    };

    // Parse the pre-formatted text data
    const lines = pageText.split('\n');
    console.log(`Processing ${lines.length} lines of text...`);

    // Find the data table section and get the most recent entry
    let inDataSection = false;
    let mostRecentDataLine = null;
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      // Look for the table header to identify data section
      if (line.includes('Date') && line.includes('Elevation') && line.includes('Tailwater')) {
        console.log('Found data table header at line', i);
        inDataSection = true;
        continue;
      }
      
      // Skip separator lines
      if (line.includes('_____') || line.length < 10) {
        continue;
      }
      
      // If we're in the data section, look for data lines
      if (inDataSection) {
        // Look for lines with date pattern (DDMMMYYYY format)
        if (line.match(/\d{2}[A-Z]{3}2025/)) {
          console.log('Found data line:', line);
          mostRecentDataLine = line;
          // Keep going to find the most recent (last) entry
        }
      }
    }

    if (mostRecentDataLine) {
      console.log('Processing most recent data line:', mostRecentDataLine);
      
      // Parse the fixed-width format more carefully
      // The data appears to be in columns, let's extract by position
      const line = mostRecentDataLine.trim();
      
      // Split by multiple spaces to get the columns
      const parts = line.split(/\s+/);
      console.log('Data parts:', parts);
      console.log('Number of parts:', parts.length);
      
      if (parts.length >= 8) {
        const date = parts[0]; // e.g., "10JUN2025"
        const time = parts[1]; // e.g., "1000"
        const elevation = parts[2]; // e.g., "576.79"
        const tailwater = parts[3]; // e.g., "375.96"
        const generation = parts[4]; // e.g., "37"
        const turbineRelease = parts[5]; // e.g., "2449"
        const siphon = parts[6]; // e.g., "0"
        const spillwayRelease = parts[7]; // e.g., "0"
        
        // Total release is usually the last column
        const totalRelease = parts.length > 8 ? parts[8] : turbineRelease;
        
        corpsData.poolElevation = elevation;
        corpsData.tailwaterElevation = tailwater;
        corpsData.powerGeneration = generation;
        corpsData.powerHouseDischarge = generation;
        corpsData.spillwayRelease = spillwayRelease;
        corpsData.totalOutflow = totalRelease;
        corpsData.lastUpdate = `${date} ${time}`;
        
        console.log('✅ Parsed data successfully:');
        console.log('- Date/Time:', `${date} ${time}`);
        console.log('- Pool Elevation:', elevation, 'ft');
        console.log('- Tailwater:', tailwater, 'ft');
        console.log('- Power Generation:', generation, 'MWh');
        console.log('- Turbine Release:', turbineRelease, 'CFS');
        console.log('- Spillway Release:', spillwayRelease, 'CFS');
        console.log('- Total Release:', totalRelease, 'CFS');
      } else {
        console.log('⚠️ Data line does not have expected number of columns');
        
        // Try alternative parsing for different format
        // Sometimes the data might be formatted differently
        const elevationMatch = line.match(/(\d{3}\.\d{2})/);
        if (elevationMatch) {
          corpsData.poolElevation = elevationMatch[1];
          console.log('- Found elevation from line:', elevationMatch[1]);
        }
        
        // Look for discharge values (typically 3-4 digit numbers)
        const dischargeMatches = line.match(/\b(\d{3,5})\b/g);
        if (dischargeMatches && dischargeMatches.length > 0) {
          // The largest number is usually total discharge
          const discharges = dischargeMatches.map(d => parseInt(d));
          corpsData.totalOutflow = Math.max(...discharges).toString();
          console.log('- Found discharge from line:', corpsData.totalOutflow, 'CFS');
        }
      }
    } else {
      console.log('⚠️ No data lines found in expected format');
      
      // Try to find data lines with a different approach
      console.log('Searching for any lines with elevation patterns...');
      let mostRecentLine = null;
      let mostRecentDate = '';
      
      for (const line of lines) {
        if (line.match(/\d{3}\.\d{2}/) && line.length > 50) {
          console.log('Potential data line found:', line);
          
          // Check if this looks like a data line with date
          const dateMatch = line.match(/(\d{2}[A-Z]{3}2025)/);
          if (dateMatch) {
            const lineDate = dateMatch[1];
            
            // Parse this line in detail
            const parts = line.trim().split(/\s+/);
            console.log('Line parts:', parts);
            
            if (parts.length >= 8) {
              const date = parts[0];     // 10JUN2025
              const time = parts[1];     // 1000
              const elevation = parts[2]; // 576.79
              const tailwater = parts[3]; // 375.96
              const generation = parts[4]; // 37
              const turbineRelease = parts[5]; // 2449
              const siphon = parts[6];   // 0
              const spillwayRelease = parts[7]; // 0
              const totalRelease = parts.length > 8 ? parts[8] : parts[5]; // 2449
              
              console.log(`📊 Parsed line: ${date} ${time}`);
              console.log(`   Elevation: ${elevation}, Tailwater: ${tailwater}`);
              console.log(`   Generation: ${generation}, Turbine: ${turbineRelease}`);
              console.log(`   Spillway: ${spillwayRelease}, Total: ${totalRelease}`);
              
              // Keep track of the most recent (latest time)
              if (lineDate >= mostRecentDate) {
                mostRecentDate = lineDate;
                mostRecentLine = {
                  date: date,
                  time: time,
                  elevation: elevation,
                  tailwater: tailwater,
                  generation: generation,
                  turbineRelease: turbineRelease,
                  spillwayRelease: spillwayRelease,
                  totalRelease: totalRelease
                };
              }
            } else {
              // Fallback parsing for lines with different format
              for (let i = 0; i < parts.length; i++) {
                const part = parts[i];
                if (part.match(/\d{3}\.\d{2}/)) {
                  const elevation = parseFloat(part);
                  if (elevation >= 570 && elevation <= 590) {
                    console.log('- Found elevation:', part);
                    
                    // Look for discharge values in subsequent parts
                    for (let j = i + 1; j < parts.length; j++) {
                      const dischargePart = parts[j];
                      if (dischargePart.match(/^\d{3,5}$/)) {
                        const discharge = parseInt(dischargePart);
                        if (discharge > 1000 && discharge < 50000) {
                          console.log('- Found discharge:', dischargePart, 'CFS');
                          
                          // Keep this as backup if no complete line found
                          if (!mostRecentLine) {
                            mostRecentLine = {
                              elevation: part,
                              totalRelease: dischargePart
                            };
                          }
                          break;
                        }
                      }
                    }
                    break;
                  }
                }
              }
            }
          }
        }
      }
      
      // Use the most recent complete data line
      if (mostRecentLine) {
        console.log('🎯 Using most recent data line:', mostRecentLine);
        
        corpsData.poolElevation = mostRecentLine.elevation;
        corpsData.tailwaterElevation = mostRecentLine.tailwater || null;
        corpsData.powerGeneration = mostRecentLine.generation || null;
        corpsData.powerHouseDischarge = mostRecentLine.generation || null;
        corpsData.spillwayRelease = mostRecentLine.spillwayRelease || null;
        corpsData.totalOutflow = mostRecentLine.totalRelease || null;
        corpsData.lastUpdate = mostRecentLine.date && mostRecentLine.time ? 
          `${mostRecentLine.date} ${mostRecentLine.time}` : null;
        
        console.log('✅ Final extracted data:');
        console.log('- Pool Elevation:', corpsData.poolElevation, 'ft');
        console.log('- Tailwater Elevation:', corpsData.tailwaterElevation, 'ft');
        console.log('- Power Generation:', corpsData.powerGeneration, 'MWh');
        console.log('- Turbine Release:', corpsData.powerHouseDischarge, 'CFS');
        console.log('- Spillway Release:', corpsData.spillwayRelease, 'CFS');
        console.log('- Total Outflow:', corpsData.totalOutflow, 'CFS');
        console.log('- Last Update:', corpsData.lastUpdate);
      }
    }

    // Also extract the current power pool from the header
    const powerPoolMatch = pageText.match(/Current Power Pool:\s*(\d+\.\d+)/);
    if (powerPoolMatch) {
      console.log('Found current power pool:', powerPoolMatch[1]);
      // Note: This might be different from the detailed elevation data
    }

    // Extract flood pool level
    const floodPoolMatch = pageText.match(/Top Flood Pool:\s*(\d+\.\d+)/);
    if (floodPoolMatch) {
      console.log('Found flood pool level:', floodPoolMatch[1]);
    }

    // Alternative parsing for different formats
    if (!corpsData.poolElevation) {
      console.log('No data found in main parsing, trying alternative patterns...');
      
      // Look for the most recent elevation in the text
      const elevationMatches = pageText.match(/(\d{3}\.\d{2})/g);
      if (elevationMatches) {
        // Filter for realistic pool elevations (570-590 range for current conditions)
        const validElevations = elevationMatches
          .map(e => parseFloat(e))
          .filter(e => e >= 570 && e <= 590);
        
        if (validElevations.length > 0) {
          // Use the most recent valid elevation
          corpsData.poolElevation = validElevations[validElevations.length - 1].toString();
          console.log(`Found elevation via pattern matching: ${corpsData.poolElevation}`);
        }
      }
    }

    // Validate the data
    if (corpsData.poolElevation) {
      const elevation = parseFloat(corpsData.poolElevation);
      if (elevation < 400 || elevation > 600) {
        console.log(`⚠️ Corps elevation ${elevation} seems unrealistic, setting to null`);
        corpsData.poolElevation = null;
      }
    }

    console.log('Final Corps data:', JSON.stringify(corpsData, null, 2));
    return corpsData;

  } catch (error) {
    console.error('Error fetching Corps data:', error);
    return null;
  }
};

// Main function to fetch and compile dam data
const fetchNorforkDamData = async () => {
  try {
    console.log('Fetching Norfork Dam data...');

    // Fetch data from Corps source and rainfall data
    const [corpsData, rainfallData] = await Promise.all([
      fetchCorpsData(),
      fetchRainfallData()
    ]);

    if (!corpsData || !corpsData.poolElevation) {
      console.log('❌ No valid Corps data available');
      return { dams: [] };
    }

    const currentDate = new Date().toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });

    const waterLevel = corpsData.poolElevation;
    const specs = damSpecs.norfork;
    const storagePercentage = calculateStoragePercentage(waterLevel, specs);
    const liveStorage = calculateLiveStorage(waterLevel, specs);

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
      data: [{
        date: corpsData.lastUpdate || currentDate,
        waterLevel: waterLevel,
        liveStorage: liveStorage,
        storagePercentage: storagePercentage,
        inflow: corpsData.inflow || '0', // Changed from 'N/A' to '0'
        powerHouseDischarge: corpsData.powerHouseDischarge || '0',
        spillwayRelease: corpsData.spillwayRelease || '0',
        totalOutflow: corpsData.totalOutflow || '0',
        rainfall: rainfallData || '0', // Use actual rainfall data or '0'
        tailwaterElevation: corpsData.tailwaterElevation || '0',
        powerGeneration: corpsData.powerGeneration || '0',
        changeIn24Hours: corpsData.changeIn24Hours || '0',
        dataSource: 'Army Corps of Engineers'
      }]
    };

    console.log(`✅ Water Level: ${waterLevel} ft MSL`);
    console.log(`✅ Storage: ${storagePercentage}% of capacity`);
    console.log(`✅ Rainfall: ${rainfallData} inches`);
    
    if (corpsData.spillwayRelease && parseFloat(corpsData.spillwayRelease) > 0) {
      console.log(`⚠️  Spillway Release: ${corpsData.spillwayRelease} CFS`);
    }

    return { dams: [damData] };

  } catch (error) {
    console.error('Error fetching Norfork Dam data:', error);
    return { dams: [] };
  }
};

// Main function to fetch dam details and update data files
async function fetchDamDetails() {
  try {
    console.log('🚀 Starting Norfork Dam scraper...');
    
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
        console.log(`📊 Found existing data for ${newDam.name}`);
        // Check if this date already exists
        const dateExists = existingDam.data.some(d => d.date === newDam.data[0].date);

        if (!dateExists) {
          console.log(`➕ Adding new data point for ${newDam.data[0].date}`);
          // Add new data point to the beginning
          existingDam.data.unshift(newDam.data[0]);
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
          dataChanged = true;
        } else {
          console.log(`⏭️  Data for ${newDam.data[0].date} already exists, skipping`);
        }
      } else {
        console.log(`🆕 Creating new dam file for ${newDam.name}`);
        // New dam, add entire data structure
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
    console.log('Norfork Dam scraper completed successfully.');
  }).catch(error => {
    console.error('Scraper failed:', error);
  });
}

module.exports = {
  fetchDamDetails,
  fetchNorforkDamData,
  fetchCorpsData,
  fetchRainfallData
};
