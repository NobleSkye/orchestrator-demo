const fetch = require('node-fetch');
const fs = require('fs');

async function updateBadge() {
  // Fetch player count from Minecraft server (replace with your server details)
  const serverAddress = 'play.skyemc.net';
  const serverPort = 25565;

  const response = await fetch(`https://api.minetools.eu/ping/${serverAddress}/${serverPort}`);
  const data = await response.json();
  const playerCount = data.players.online;

  // Create a dynamic badge URL
const playerCount + 1; // Replace with the actual player count
const markdown = `![Players Online](https://img.shields.io/badge/Players%20Online-${playerCount}-brightgreen)`;
// Now use the 'markdown' variable where appropriate

  // Write the badge URL to a file (badge.md)
  fs.writeFileSync('badge.md', `[![Players Online](${badgeUrl})](https://sites.google.com/view/skyenetwork/home)`);
}

updateBadge();
