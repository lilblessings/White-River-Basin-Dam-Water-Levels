const axios = require('axios');
const fs = require('fs').promises;

const folderName = 'historic_data';

// Format date to DD.MM.YYYY
const formatDate = (date) => {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  return `${day}.${month}.${year}`;
};

// Calculate storage percentage (100% = 580 ft flood pool)
const calculateStoragePercentage = (waterLevel) => {
  const level = parseFloat(waterLevel);
  const floodPool = 580.0; // 100% full
  const deadLevel = 380.0; // 0% full
  
  if (level <= deadLevel) return '0.00';
  if (level >= floodPool) return '100.00';
  
  const percentage = ((level - deadLevel) / (floodPool - deadLevel)) * 100;
  return percentage.toFixed(2);
};

// Calculate live storage in thousands of acre-feet
const calculateLiveStorage = (waterLevel) => {
  const level = parseFloat(waterLevel);
  const floodPool = 580.0;
  const deadLevel = 380.0;
  const maxStorage = 2580; // thousands of acre-feet at flood pool
  
  if (level <= deadLevel) return '0.000';
  if (level >= floodPool) return maxStorage.toFixed(3);
  
  const depthRatio = (level - deadLevel) / (floodPool - deadLevel);
  const storage = maxStorage * Math.pow(depthRatio, 2.2);
  return storage.toFixed(3);
};

// Fetch Corps data
async function fetchCorpsData() {
  try {
    const response = await axios.get('https://www.swl.usace.army.mil/Missions/Water-Management/Real-time-Reports/Norfork/', {
      timeout: 15000
    });
    
    const lines = response.data.split('\n');
    let mostRecentData = null;
    
    // Find the most recent data line
    for (const line of lines) {
      if (line.includes('JUN2025') && line.length > 80) {
        const parts = line.trim().split(/\s+/);
        if (parts.length >= 8) {
          mostRecentData = {
            elevation: parts[2],
            generation: parts[4], // MW generation
            spillway: parts[7],
            totalOutflow: parts[8] || parts[5]
          };
        }
      }
    }
    
    return mostRecentData;
  } catch (error) {
    console.error('Error fetching Corps data:', error.message);
    return null;
  }
}

// Main function
async function fetchDamDetails() {
  try {
    console.log('🚀 Starting Norfork Dam scraper...');
    
    // Create folder if needed
    try {
      await fs.access(folderName);
    } catch (error) {
      await fs.mkdir(folderName);
    }

    // Get Corps data
    const corpsData = await fetchCorpsData();
    
    if (!corpsData) {
      console.log('❌ No dam data found');
      return;
    }

    console.log('✅ Found data - Water Level:', corpsData.elevation, 'ft');

    // Create the basic dam data structure - EXACTLY like original
    const damData = {
      id: '1',
      name: 'Norfork',
      officialName: 'NORFORK',
      MWL: '590.00',
      FRL: '552.00',
      liveStorageAtFRL: '1,983,000',
      ruleLevel: '552.00',
      blueLevel: '552.00',
      orangeLevel: '570.00',
      redLevel: '580.00',
      latitude: '36.2356',
      longitude: '-92.3829',
      data: [{
        date: formatDate(new Date()),
        waterLevel: corpsData.elevation,
        liveStorage: calculateLiveStorage(corpsData.elevation),
        storagePercentage: calculateStoragePercentage(corpsData.elevation) + '%',
        inflow: '0.00',
        powerHouseDischarge: corpsData.generation, // MW generation goes here
        spillwayRelease: corpsData.spillway || '0.00',
        totalOutflow: corpsData.totalOutflow,
        rainfall: '0.00'
      }]
    };

    console.log('📊 Final data:');
    console.log('- Water Level:', damData.data[0].waterLevel, 'ft');
    console.log('- Storage:', damData.data[0].storagePercentage);
    console.log('- Power:', damData.data[0].powerHouseDischarge, 'MW');
    console.log('- Outflow:', damData.data[0].totalOutflow, 'CFS');

    // Load existing data
    const existingData = {};
    try {
      const files = await fs.readdir(folderName);
      for (const file of files) {
        if (file.endsWith('.json')) {
          const damName = file.replace('.json', '');
          const data = JSON.parse(await fs.readFile(`${folderName}/${file}`, 'utf8'));
          existingData[damName] = data;
        }
      }
    } catch (error) {
      console.log('📝 No existing files found');
    }

    // Check if this date already exists
    const existingDam = existingData['Norfork'];
    let dataChanged = false;

    if (existingDam) {
      const dateExists = existingDam.data.some(d => d.date === damData.data[0].date);
      if (!dateExists) {
        existingDam.data.unshift(damData.data[0]);
        dataChanged = true;
      }
    } else {
      existingData['Norfork'] = damData;
      dataChanged = true;
    }

    if (dataChanged) {
      // Save individual dam file
      await fs.writeFile(`${folderName}/Norfork.json`, JSON.stringify(existingData['Norfork'], null, 4));
      console.log('✅ Saved historic_data/Norfork.json');

      // Save live data
      await fs.writeFile('live.json', JSON.stringify({
        lastUpdate: damData.data[0].date,
        dams: [existingData['Norfork']]
      }, null, 4));
      console.log('✅ Saved live.json');
    } else {
      console.log('⏸️ No new data to save');
    }

  } catch (error) {
    console.error('💥 Error:', error.message);
  }
}

// Run the scraper
fetchDamDetails();
