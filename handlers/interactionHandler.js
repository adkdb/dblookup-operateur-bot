const handleOperateur = require('./operateurHandler');

const allowedChannelNames = ["operateur", "terminaluhq"];

// ✅ Commandes sans restriction de salon
const noChannelRestriction = ['dbdelete'];

module.exports = async function handleInteraction(interaction, client) {

  // ── Slash commands ──
  if (interaction.isChatInputCommand()) {

    // ✅ Bypass restriction salon pour certaines commandes
    if (!noChannelRestriction.includes(interaction.commandName)) {
      if (!allowedChannelNames.includes(interaction.channel.name)) {
        return interaction.reply({
          content: `❌ Tu ne peux pas utiliser cette commande dans ce salon.`,
          ephemeral: true
        });
      }
    }

    const command = client.commands.get(interaction.commandName);
    if (!command) return;

    command.execute(interaction).catch(err => {
      console.error(`❌ [CMD] /${interaction.commandName} :`, err);
    });
    return;
  }

  // ── Menu opérateur ──
  if (interaction.isStringSelectMenu() && interaction.customId === 'select_operateur') {
    await handleOperateur(interaction).catch(err => {
      console.error('❌ [OPERATEUR] Menu :', err.message);
    });
    return;
  }

  // ── Modal lookup ──
  if (interaction.isModalSubmit() && interaction.customId.startsWith('modal_lookup_')) {
    handleOperateur(interaction).catch(err => {
      console.error('❌ [OPERATEUR] Modal :', err.message);
    });
    return;
  }

  // ── Boutons ──
  if (interaction.isButton() && (
    interaction.customId.startsWith('lookup_') ||
    interaction.customId.startsWith('gnav_')
  )) {
    handleOperateur(interaction).catch(err => {
      console.error('❌ [OPERATEUR] Button :', err.message);
    });
    return;
  }

};
