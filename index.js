const { Client, GatewayIntentBits, Collection, ActivityType } = require('discord.js');
const { REST, Routes } = require('discord.js');
const fs = require('fs');
const path = require('path');
const config = require('./config.json');
const autoImport = require('./scripts/autoImport');
const createIndexes = require('./scripts/createIndexes'); // ✅ AJOUTÉ
const handleInteraction = require('./handlers/interactionHandler');

// ── Client Discord ──
const client = new Client({
  intents: [GatewayIntentBits.Guilds]
});
client.commands = new Collection();

// ── Charger les commandes ──
const commands = [];
const commandsPath = path.join(__dirname, 'commands');
const commandFiles = fs.readdirSync(commandsPath).filter(f => f.endsWith('.js'));

for (const file of commandFiles) {
  try {
    const command = require(path.join(commandsPath, file));
    if (!command?.data || !command?.execute) {
      console.warn(`⚠️  [COMMANDS] Structure invalide : ${file}`);
      continue;
    }
    client.commands.set(command.data.name, command);
    commands.push(command.data.toJSON());
    console.log(`✅ [COMMANDS] Chargée : /${command.data.name}`);
  } catch (err) {
    console.error(`❌ [COMMANDS] Erreur ${file} :`, err.message);
  }
}

// ── Enregistrer les slash commands ──
async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(config.token);
  try {
    console.log('\n⏳ [DISCORD] Enregistrement des slash commands...');
    await rest.put(
      Routes.applicationGuildCommands(config.clientId, config.guildId),
      { body: commands }
    );
    console.log(`✅ [DISCORD] ${commands.length} commande(s) enregistrée(s).\n`);
  } catch (err) {
    console.error('❌ [DISCORD] Erreur enregistrement :', err.message);
  }
}

// ── Interactions ──
client.on('interactionCreate', async interaction => {
  try {
    await handleInteraction(interaction, client);
  } catch (err) {
    console.error('❌ [INTERACTION] Erreur globale :', err);
  }
});

// ── Événements bot ──
client.once('ready', () => {

  const statuses = [
    { name: '/dblookup', type: ActivityType.Streaming },
    { name: '50.2go of db by atxdb', type: ActivityType.Streaming },
    { name: 'V1.5 by adkdb', type: ActivityType.Streaming }
  ];

  let i = 0;

  setInterval(() => {
    const status = statuses[i];

    client.user.setActivity(status.name, { 
      type: status.type,
      url: 'https://www.twitch.tv/dblookup'
    });

    i = (i + 1) % statuses.length;

  }, 10000);

  console.log('──────────────────────────────────');
  console.log(`✅ [BOT] Connecté : ${client.user.tag}`);
  console.log(`📡 [BOT] Guild : ${config.guildId}`);
  console.log(`📂 [BOT] Commandes : ${client.commands.size}`);
  console.log('──────────────────────────────────\n');

});

client.on('warn',  msg => console.warn('⚠️  [DISCORD]', msg));
client.on('error', err => console.error('❌ [DISCORD]', err.message));

// ── Erreurs globales ──
process.on('unhandledRejection', err => console.error('❌ [UNHANDLED]', err));
process.on('uncaughtException',  err => console.error('❌ [EXCEPTION]', err));

// ── Démarrage ──
(async () => {
  try {
    await autoImport();
    await createIndexes(); // ✅ AJOUTÉ — index SQLite au démarrage
    await registerCommands();
    await client.login(config.token);
  } catch (err) {
    console.error('❌ [STARTUP] Erreur critique :', err);
    process.exit(1);
  }
})();
