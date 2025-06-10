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

// Dam specifications based on US Army Corps of Engineers data
const damSpecs = {
  'norfork': {
    MWL: '590.00', // Maximum Water Level (top of dam)
    FRL: '552.00', // Full Recreation Level (normal pool)
    liveStorageAtFRL: '1,983,000', // acre-feet at FRL
    ruleLevel: '552.00', // Rule curve level
    blueLevel: '552.00', // Recreation level
    orangeLevel: '570.00', // Action level
    redLevel: '580.00' // Flood pool
  }
};

// Calculate storage percentage based on water level
const calculateStoragePercentage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return '0.00';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL);
  const mwl = parseFloat(specs.MWL);
  
  if (level <= frl) {
    // Below full pool - calculate based on normal storage curve
    const percentage = ((level - 400) / (frl - 400)) * 100; // Rough approximation
    return Math.max(0, Math.min(100, percentage)).toFixed(2);
  } else {
    // Above full pool - in flood storage
    const normalStorage = 100;
    const floodStorage = ((level - frl) / (mwl - frl)) * 20; // Additional flood storage
    return Math.min(120, normalStorage + floodStorage).toFixed(2);
  }
};

// Calculate live storage based on water level
const calculateLiveStorage = (waterLevel, specs) => {
  if (!waterLevel || !specs) return 'N/A';
  
  const level = parseFloat(waterLevel);
  const frl = parseFloat(specs.FRL);
  const maxStorage = 1983000; // acre-feet at FRL
  
  if (level <= 400) return '0'; // Dead storage level approximation
  if (level >= frl) return maxStorage.toLocaleString();
  
  // Linear approximation for storage curve (actual would be exponential)
  const storageRatio = (level - 400) / (frl - 400);
  const currentStorage = Math.round(maxStorage * storageRatio);
  
  return currentStorage.toLocaleString();
};

// Helper function to extract numeric values from Corps data
const extractNumericValue = (text) => {
  if (!text) return null;
  
  // Remove common units and extract number
  const cleaned = text.replace(/[^\d.-]/g, '');
  const number = parseFloat(cleaned);
  
  return isNaN(number) ? null : number.toString();
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
        corpsData.powerHouseDischarge = turbineRelease;
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
        corpsData.powerHouseDischarge = mostRecentLine.turbineRelease || null;
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

    // Fetch data from Corps source only
    const corpsData = await fetchCorpsData();

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
        inflow: corpsData.inflow || 'N/A',
        powerHouseDischarge: corpsData.powerHouseDischarge || 'N/A',
        spillwayRelease: corpsData.spillwayRelease || '0',
        totalOutflow: corpsData.totalOutflow || 'N/A',
        rainfall: 'N/A',
        tailwaterElevation: corpsData.tailwaterElevation || 'N/A',
        powerGeneration: corpsData.powerGeneration || 'N/A',
        changeIn24Hours: corpsData.changeIn24Hours || 'N/A',
        dataSource: 'Army Corps of Engineers'
      }]
    };

    console.log(`✅ Water Level: ${waterLevel} ft MSL`);
    console.log(`✅ Storage: ${storagePercentage}% of capacity`);
    
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
  fetchCorpsData
};
