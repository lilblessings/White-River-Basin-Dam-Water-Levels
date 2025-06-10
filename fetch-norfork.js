const axios = require('axios');
const fs = require('fs').promises;

async function fetchNorforkData() {
  const url = 'https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/norfork.htm';

  try {
    const { data } = await axios.get(url);
    const lines = data.split('\n');

    // Find the most recent data row
    const recentLine = lines.reverse().find(line => /^\d{2}\/\d{2}\/\d{4}/.test(line));

    if (!recentLine) throw new Error('No valid data row found');

    const parts = recentLine.trim().split(/\s+/);

    const entry = {
      date: parts[0],
      hour: parts[1],
      waterLevel: parts[3],
      inflow: parts[4],
      outflow: parts[5],
      generation: parts[6],
    };

    const damData = {
      name: 'Norfork Dam',
      MWL: "580", // placeholder
      FRL: "573", // placeholder
      latitude: 36.3594,
      longitude: -92.2794,
      data: [entry],
    };

    await fs.writeFile('live.json', JSON.stringify({ lastUpdate: entry.date, dams: [damData] }, null, 4));
    await fs.writeFile('historic_data/Norfork_Dam.json', JSON.stringify(damData, null, 4));

    console.log('Norfork dam data saved.');
  } catch (err) {
    console.error('Error fetching Norfork dam data:', err);
  }
}

fetchNorforkData();
