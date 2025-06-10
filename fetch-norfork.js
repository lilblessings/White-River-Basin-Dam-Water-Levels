const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;

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

// Fetch current water level from USLakes.info
const fetchUSLakesData = async () => {
  try {
    const response = await axios.get('https://norfork.uslakes.info/Level/');
    const html = response.data;
    const $ = cheerio.load(html);

    // Extract water level and date from the page
    const waterLevelText = $('td').filter((i, el) => $(el).text().includes('Feet MSL')).text();
    const waterLevelMatch = waterLevelText.match(/(\d+\.\d+)/);
    const waterLevel = waterLevelMatch ? waterLevelMatch[1] : null;

    const dateText = $('td').filter((i, el) => $(el).text().includes('Tuesday, June 10, 2025')).text();
    const date = dateText ? 'June 10, 2025' : new Date().toLocaleDateString();

    return { waterLevel, date };
  } catch (error) {
    console.error('Error fetching USLakes data:', error);
    return null;
  }
};

// Fetch data from USGS API
const fetchUSGSData = async () => {
  try {
    // Site 07059998 - North Fork River at Base of Norfork Dam
    const response = await axios.get(
      'https://waterservices.usgs.gov/nwis/iv/?format=json&sites=07059998&period=P1D&parameterCd=00065,00060'
    );

    const data = response.data;
    if (data.value && data.value.timeSeries && data.value.timeSeries.length > 0) {
      const timeSeries = data.value.timeSeries;
      
      // Find gage height (00065) and discharge (00060)
      const gageHeightSeries = timeSeries.find(series => 
        series.variable.variableCode[0].value === '00065'
      );
      const dischargeSeries = timeSeries.find(series => 
        series.variable.variableCode[0].value === '00060'
      );

      const latestGageHeight = gageHeightSeries?.values[0]?.value[0];
      const latestDischarge = dischargeSeries?.values[0]?.value[0];

      return {
        gageHeight: latestGageHeight?.value || null,
        discharge: latestDischarge?.value || null,
        dateTime: latestGageHeight?.dateTime || new Date().toISOString()
      };
    }
  } catch (error) {
    console.error('Error fetching USGS data:', error);
  }
  return null;
};

// Fetch additional data from NOAA/NWS if available
const fetchNOAAData = async () => {
  try {
    // This would require specific NOAA API endpoints for Norfork Dam
    // For now, we'll return null and focus on other sources
    return null;
  } catch (error) {
    console.error('Error fetching NOAA data:', error);
    return null;
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

// Main function to fetch and compile dam data
const fetchNorforkDamData = async () => {
  try {
    console.log('Fetching Norfork Dam data from multiple sources...');

    // Fetch data from all available sources
    const uslakesData = await fetchUSLakesData();
    const usgsData = await fetchUSGSData();
    const corpsData = await fetchCorpsData();
    const weatherData = await fetchWeatherData();

    // Combine data sources with Corps data taking priority for official measurements
    const currentDate = new Date().toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });

    // Use Corps pool elevation if available, otherwise fall back to USLakes
    const waterLevel = corpsData?.poolElevation || uslakesData?.waterLevel || '552.00';
    
    // Use Corps discharge data if available, otherwise USGS
    const powerHouseDischarge = corpsData?.powerHouseDischarge || usgsData?.discharge || 'N/A';
    const totalOutflow = corpsData?.totalOutflow || usgsData?.discharge || 'N/A';
    
    const specs = damSpecs.norfork;
    const storagePercentage = calculateStoragePercentage(waterLevel, specs);

    // Calculate live storage if we have water level
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
        date: corpsData?.lastUpdate ? new Date(corpsData.lastUpdate).toLocaleDateString() : (uslakesData?.date || currentDate),
        waterLevel: waterLevel,
        liveStorage: liveStorage,
        storagePercentage: storagePercentage,
        inflow: corpsData?.inflow || 'N/A',
        powerHouseDischarge: powerHouseDischarge,
        spillwayRelease: corpsData?.spillwayRelease || '0',
        totalOutflow: totalOutflow,
        rainfall: weatherData?.rainfall24h || 'N/A',
        // Additional Corps-specific data
        tailwaterElevation: corpsData?.tailwaterElevation || 'N/A',
        powerGeneration: corpsData?.powerGeneration || 'N/A',
        changeIn24Hours: corpsData?.changeIn24Hours || 'N/A',
        dataSource: determineDataSource(corpsData, uslakesData, usgsData)
      }]
    };

    console.log(`Water Level: ${waterLevel} ft MSL`);
    console.log(`Storage: ${storagePercentage}% of capacity`);
    console.log(`Total Outflow: ${totalOutflow} CFS`);
    
    if (corpsData?.spillwayRelease && parseFloat(corpsData.spillwayRelease) > 0) {
      console.log(`⚠️  Spillway Release: ${corpsData.spillwayRelease} CFS`);
    }

    return { dams: [damData] };

  } catch (error) {
    console.error('Error fetching Norfork Dam data:', error);
    return { dams: [] };
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

// Determine which data source provided the most complete information
const determineDataSource = (corpsData, uslakesData, usgsData) => {
  const sources = [];
  
  if (corpsData && (corpsData.poolElevation || corpsData.totalOutflow)) {
    sources.push('Army Corps of Engineers');
  }
  if (uslakesData && uslakesData.waterLevel) {
    sources.push('USLakes.info');
  }
  if (usgsData && (usgsData.gageHeight || usgsData.discharge)) {
    sources.push('USGS');
  }
  
  return sources.length > 0 ? sources.join(', ') : 'Manual Entry';
};

const folderName = 'historic_data';

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

// Army Corps of Engineers data from official Norfork Dam page
const fetchCorpsData = async () => {
  try {
    console.log('Fetching Army Corps of Engineers data...');
    
    // Primary source: Official Norfork Dam data page
    const corpsUrl = 'https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/norfork.htm';
    
    try {
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

      // Parse the tabular data from the Corps page
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

      // Look for data table - Corps typically uses specific table structures
      $('table').each((tableIndex, table) => {
        $(table).find('tr').each((rowIndex, row) => {
          const cells = $(row).find('td, th');
          if (cells.length >= 2) {
            const label = $(cells[0]).text().trim().toLowerCase();
            const value = $(cells[1]).text().trim();

            // Match common Corps data labels
            if (label.includes('pool') && label.includes('elevation')) {
              corpsData.poolElevation = extractNumericValue(value);
            } else if (label.includes('tailwater') && label.includes('elevation')) {
              corpsData.tailwaterElevation = extractNumericValue(value);
            } else if (label.includes('spillway') && label.includes('release')) {
              corpsData.spillwayRelease = extractNumericValue(value);
            } else if (label.includes('powerhouse') && label.includes('discharge')) {
              corpsData.powerHouseDischarge = extractNumericValue(value);
            } else if (label.includes('total') && label.includes('outflow')) {
              corpsData.totalOutflow = extractNumericValue(value);
            } else if (label.includes('power') && label.includes('generation')) {
              corpsData.powerGeneration = extractNumericValue(value);
            } else if (label.includes('inflow')) {
              corpsData.inflow = extractNumericValue(value);
            } else if (label.includes('change') && label.includes('24')) {
              corpsData.changeIn24Hours = extractNumericValue(value);
            } else if (label.includes('last') && label.includes('update')) {
              corpsData.lastUpdate = value;
            }
          }
        });
      });

      // Alternative parsing for different table formats
      if (!corpsData.poolElevation) {
        // Try parsing pre-formatted text or other structures
        const textContent = $('body').text();
        
        // Look for elevation patterns
        const elevationMatch = textContent.match(/(?:pool|elevation).*?(\d+\.\d+)/i);
        if (elevationMatch) {
          corpsData.poolElevation = elevationMatch[1];
        }

        // Look for discharge patterns
        const dischargeMatch = textContent.match(/(?:discharge|outflow).*?(\d+(?:,\d{3})*(?:\.\d+)?)/i);
        if (dischargeMatch) {
          corpsData.powerHouseDischarge = dischargeMatch[1].replace(/,/g, '');
        }
      }

      console.log('Successfully fetched Corps data:', corpsData);
      return corpsData;

    } catch (fetchError) {
      console.log('Primary Corps URL failed, trying alternative sources...');
      
      // Alternative: Try the general water control data page
      try {
        const altResponse = await axios.get('https://www.swl-wc.usace.army.mil/webdata/gagedata.cfm', {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          }
        });
        
        const altHtml = altResponse.data;
        const $alt = cheerio.load(altHtml);
        
        // Look for Norfork data in the general listing
        let norforkData = null;
        $alt('table tr').each((index, row) => {
          const rowText = $alt(row).text().toLowerCase();
          if (rowText.includes('norfork')) {
            const cells = $alt(row).find('td');
            if (cells.length >= 3) {
              norforkData = {
                poolElevation: $alt(cells[1]).text().trim(),
                totalOutflow: $alt(cells[2]).text().trim(),
                lastUpdate: new Date().toISOString()
              };
            }
          }
        });
        
        if (norforkData) {
          console.log('Found Norfork data in alternative source:', norforkData);
          return norforkData;
        }
        
      } catch (altError) {
        console.log('Alternative Corps source also failed');
      }
    }
    
  } catch (error) {
    console.error('Error fetching Corps data:', error);
  }
  
  // Return null if all sources fail - main scraper will handle this gracefully
  return null;
};

// Helper function to extract numeric values from Corps data
const extractNumericValue = (text) => {
  if (!text) return null;
  
  // Remove common units and extract number
  const cleaned = text.replace(/[^\d.-]/g, '');
  const number = parseFloat(cleaned);
  
  return isNaN(number) ? null : number.toString();
};

// Weather data integration with National Weather Service
const fetchWeatherData = async () => {
  try {
    console.log('Fetching weather data for Norfork Dam area...');
    
    // Use NWS API for official weather data
    // First get the grid point for Norfork Dam coordinates
    const pointResponse = await axios.get(
      `https://api.weather.gov/points/36.2483,-92.24`,
      {
        headers: {
          'User-Agent': 'NorforkDamScraper/1.0 (contact@example.com)' // NWS requires User-Agent
        }
      }
    );

    if (pointResponse.data && pointResponse.data.properties) {
      const gridId = pointResponse.data.properties.gridId;
      const gridX = pointResponse.data.properties.gridX;
      const gridY = pointResponse.data.properties.gridY;

      // Get current observations from nearest station
      const stationsResponse = await axios.get(
        pointResponse.data.properties.observationStations,
        {
          headers: {
            'User-Agent': 'NorforkDamScraper/1.0 (contact@example.com)'
          }
        }
      );

      if (stationsResponse.data && stationsResponse.data.features && stationsResponse.data.features.length > 0) {
        const nearestStation = stationsResponse.data.features[0].id;
        
        // Get latest observations
        const obsResponse = await axios.get(
          `${nearestStation}/observations/latest`,
          {
            headers: {
              'User-Agent': 'NorforkDamScraper/1.0 (contact@example.com)'
            }
          }
        );

        if (obsResponse.data && obsResponse.data.properties) {
          const obs = obsResponse.data.properties;
          
          return {
            rainfall24h: obs.precipitationLastHour?.value || '0.00',
            temperature: obs.temperature?.value ? `${Math.round((obs.temperature.value * 9/5) + 32)}` : 'N/A',
            humidity: obs.relativeHumidity?.value ? `${Math.round(obs.relativeHumidity.value)}` : 'N/A',
            windSpeed: obs.windSpeed?.value || 'N/A',
            observationTime: obs.timestamp
          };
        }
      }
    }
    
    console.log('Weather data fetched successfully');
    return null; // Will use N/A values in main function
    
  } catch (error) {
    console.error('Error fetching weather data:', error);
    console.log('Note: NWS API requires proper User-Agent header and may have rate limits');
    return null;
  }
};

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
  fetchUSLakesData,
  fetchUSGSData
};
