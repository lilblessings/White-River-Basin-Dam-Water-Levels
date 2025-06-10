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

    // Parse the tabular data from the Corps page
    $('table').each((tableIndex, table) => {
      $(table).find('tr').each((rowIndex, row) => {
        const cells = $(row).find('td, th');
        if (cells.length >= 2) {
          const label = $(cells[0]).text().trim().toLowerCase();
          const value = $(cells[1]).text().trim();

          console.log(`Found row: "${label}" = "${value}"`);

          // Match common Corps data labels
          if (label.includes('pool') && (label.includes('elevation') || label.includes('level'))) {
            corpsData.poolElevation = extractNumericValue(value);
            console.log(`Pool elevation: ${corpsData.poolElevation}`);
          } else if (label.includes('tailwater') && label.includes('elevation')) {
            corpsData.tailwaterElevation = extractNumericValue(value);
          } else if (label.includes('spillway') && label.includes('release')) {
            corpsData.spillwayRelease = extractNumericValue(value);
          } else if (label.includes('powerhouse') && label.includes('discharge')) {
            corpsData.powerHouseDischarge = extractNumericValue(value);
          } else if (label.includes('total') && (label.includes('outflow') || label.includes('discharge'))) {
            corpsData.totalOutflow = extractNumericValue(value);
          } else if (label.includes('power') && label.includes('generation')) {
            corpsData.powerGeneration = extractNumericValue(value);
          } else if (label.includes('inflow')) {
            corpsData.inflow = extractNumericValue(value);
          } else if (label.includes('change') && label.includes('24')) {
            corpsData.changeIn24Hours = extractNumericValue(value);
          } else if (label.includes('time') || label.includes('date') || label.includes('update')) {
            corpsData.lastUpdate = value;
          }
        }
      });
    });

    // Alternative parsing for different formats
    if (!corpsData.poolElevation) {
      console.log('Trying alternative parsing methods...');
      
      // Look for elevation in any text content
      const pageText = $('body').text();
      
      // Try different elevation patterns
      const elevationPatterns = [
        /pool.*?elevation.*?(\d+\.\d+)/i,
        /elevation.*?pool.*?(\d+\.\d+)/i,
        /lake.*?level.*?(\d+\.\d+)/i,
        /(\d{3}\.\d{2})\s*(?:ft|feet)/i
      ];
      
      for (const pattern of elevationPatterns) {
        const match = pageText.match(pattern);
        if (match) {
          const elevation = parseFloat(match[1]);
          if (elevation > 500 && elevation < 600) {
            corpsData.poolElevation = match[1];
            console.log(`Found elevation via pattern matching: ${corpsData.poolElevation}`);
            break;
          }
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
    // Create folder if it doesn't exist
    try {
      await fs.access(folderName);
    } catch (error) {
      await fs.mkdir(folderName);
    }

    console.log('Processing Norfork Dam data...');
    const { dams } = await fetchNorforkDamData();

    if (dams.length === 0) {
      console.log('No dam data found.');
      return;
    }

    // Load existing data
    const existingData = {};
    try {
      const files = await fs.readdir(folderName);
      for (const file of files) {
        if (file.endsWith('.json')) {
          const damName = file.replace('historic_data_', '').replace('.json', '').replace(/_/g, ' ');
          const data = JSON.parse(await fs.readFile(`${folderName}/${file}`, 'utf8'));
          existingData[damName] = data;
        }
      }
    } catch (error) {
      console.log('No existing data files found, creating new ones...');
    }

    let dataChanged = false;

    for (const newDam of dams) {
      const existingDam = existingData[newDam.name];

      if (existingDam) {
        // Check if this date already exists
        const dateExists = existingDam.data.some(d => d.date === newDam.data[0].date);

        if (!dateExists) {
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
        }
      } else {
        // New dam, add entire data structure
        existingData[newDam.name] = newDam;
        dataChanged = true;
      }
    }

    if (dataChanged) {
      // Save individual dam files
      for (const [damName, damData] of Object.entries(existingData)) {
        const filename = `${folderName}/historic_data_${damName.replace(/\s+/g, '_')}.json`;
        await fs.writeFile(filename, JSON.stringify(damData, null, 4));
        console.log(`Details for dam ${damName} saved successfully in ${filename}.`);
      }

      // Save live JSON file with most recent data
      const liveData = {
        lastUpdate: dams[0].data[0].date,
        dams
      };
      await fs.writeFile('live.json', JSON.stringify(liveData, null, 4));
      console.log('Live dam data saved successfully in live.json.');
    } else {
      console.log('No new data to save.');
    }

  } catch (error) {
    console.error('Error:', error);
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
