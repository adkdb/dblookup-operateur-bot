const {
  SlashCommandBuilder, EmbedBuilder,
  ActionRowBuilder, ButtonBuilder,
  ButtonStyle, ComponentType
} = require('discord.js');
const config = require('../config.json');
const EU = config.emojis.ui;
const EO = config.emojis.operateurs;

const DOT_FRAMES = ['⣾','⣽','⣻','⢿','⡿','⣟','⣯','⣷'];
const DB_SIZE    = '50.2 GB';

module.exports = {
  data: new SlashCommandBuilder()
    .setName('dbdelete')
    .setDescription('🗑️ Supprime une personne de toutes les bases de données')
    .addStringOption(opt =>
      opt.setName('query')
        .setDescription('Numéro, Nom, Prénom, Email, IBAN...')
        .setRequired(true)
        .setMaxLength(100)
    ),

  async execute(interaction) {

    if (!interaction.member.roles.cache.has(config.adminRoleId)) {
      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`${EU.error} Accès refusé`)
            .setDescription('Tu n\'as pas le rôle requis pour utiliser cette commande.')
            .setColor(0xff0000)
        ],
        flags: 64
      });
    }

    const query = interaction.options.getString('query').trim();
    await interaction.deferReply();

    // ── Étape 1 — Recherche avec animation ──
    let frame   = 0;
    let dbsDone = 0;
    const dbsTotal = 9;

    const buildSearchEmbed = (label) => {
      const pct    = Math.min(Math.round((dbsDone / dbsTotal) * 100), 99);
      const filled = Math.round(pct / 10);
      const bar    = '█'.repeat(filled) + '░'.repeat(10 - filled);
      return new EmbedBuilder()
        .setTitle(`${EU.search} Recherche des entrées...`)
        .setDescription(
          `${EU.search} **Requête :** \`${query}\`\n` +
          `${EU.database} **Base de données :** ${DB_SIZE}\n\n` +
          `${DOT_FRAMES[frame % DOT_FRAMES.length]} ${label}\n` +
          `\`[${bar}] ${pct}%\` — ${dbsDone}/${dbsTotal} DB`
        )
        .setColor(0xffa500);
    };

    await interaction.editReply({ embeds: [buildSearchEmbed('Connexion aux bases...')] });

    const searchInterval = setInterval(async () => {
      frame++;
      if (dbsDone < dbsTotal) dbsDone = Math.min(dbsDone + 1, dbsTotal - 1);
      try {
        await interaction.editReply({ embeds: [buildSearchEmbed('Recherche en cours...')] });
      } catch (_) {}
    }, 1500);

    const search = require('../utils/search');
    let found;
    try {
      found = await search(query, { limit: 99999, dbFilter: null });
    } catch (err) {
      clearInterval(searchInterval);
      return interaction.editReply({
        embeds: [new EmbedBuilder().setTitle(`${EU.error} Erreur recherche`).setDescription(`\`\`\`${err.message}\`\`\``).setColor(0xff0000)]
      });
    }
    clearInterval(searchInterval);

    if (!Array.isArray(found)) found = [];

    if (found.length === 0) {
      return interaction.editReply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`${EU.error} Aucune entrée trouvée`)
            .setDescription(`Aucune correspondance pour \`${query}\` dans les bases.`)
            .setColor(0xff4444).setTimestamp()
        ]
      });
    }

    // ── Étape 2 — Preview + confirmation ──
    const grouped = {};
    for (const r of found) {
      if (!grouped[r.source]) grouped[r.source] = 0;
      grouped[r.source]++;
    }

    const previewLines = Object.entries(grouped)
      .map(([src, cnt]) => `${EU.database} **${src}** — \`${cnt}\` entrée(s)`)
      .join('\n');

    const confirmRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId('delete_confirm')
        .setLabel(`${EU.delete} Supprimer ${found.length} entrée(s)`)
        .setStyle(ButtonStyle.Danger),
      new ButtonBuilder()
        .setCustomId('delete_cancel')
        .setLabel(`${EU.cancel} Annuler`)
        .setStyle(ButtonStyle.Secondary)
    );

    const previewMsg = await interaction.editReply({
      embeds: [
        new EmbedBuilder()
          .setTitle(`${EU.warning} Confirmation de suppression`)
          .setDescription(
            `${EU.search} **Requête :** \`${query}\`\n` +
            `${EU.delete} **${found.length}** entrée(s) trouvée(s) :\n` +
            `━━━━━━━━━━━━━━━━━━━━━━\n` +
            previewLines + '\n\n' +
            `${EU.warning} **Cette action est irréversible !**`
          )
          .setColor(0xff8800).setTimestamp()
      ],
      components: [confirmRow]
    });

    let confirmed = false;
    try {
      const btn = await previewMsg.awaitMessageComponent({
        componentType: ComponentType.Button,
        time: 30_000,
        filter: i => i.user.id === interaction.user.id
      });
      await btn.deferUpdate();
      confirmed = btn.customId === 'delete_confirm';
    } catch {
      confirmed = false;
    }

    if (!confirmed) {
      return interaction.editReply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`${EU.cancel} Suppression annulée`)
            .setDescription('Aucune donnée supprimée.')
            .setColor(0x808080).setTimestamp()
        ],
        components: []
      });
    }

    // ── Étape 3 — Suppression avec animation ──
    frame   = 0;
    dbsDone = 0;

    const buildDeleteEmbed = (label) => {
      const pct    = Math.min(Math.round((dbsDone / dbsTotal) * 100), 99);
      const filled = Math.round(pct / 10);
      const bar    = '█'.repeat(filled) + '░'.repeat(10 - filled);
      return new EmbedBuilder()
        .setTitle(`${EU.loading} Suppression en cours...`)
        .setDescription(
          `${EU.search} **Requête :** \`${query}\`\n` +
          `${EU.database} **Base de données :** ${DB_SIZE}\n\n` +
          `${DOT_FRAMES[frame % DOT_FRAMES.length]} ${label}\n` +
          `\`[${bar}] ${pct}%\` — ${dbsDone}/${dbsTotal} DB`
        )
        .setColor(0xff4400);
    };

    await interaction.editReply({
      embeds:     [buildDeleteEmbed('Connexion aux bases...')],
      components: []
    });

    const deleteInterval = setInterval(async () => {
      frame++;
      if (dbsDone < dbsTotal) dbsDone = Math.min(dbsDone + 1, dbsTotal - 1);
      try {
        await interaction.editReply({ embeds: [buildDeleteEmbed('Suppression en cours...')] });
      } catch (_) {}
    }, 1500);

    const deleteWorker = require('../utils/deleteWorker');
    let result;
    try {
      result = await deleteWorker(query);
    } catch (err) {
      clearInterval(deleteInterval);
      return interaction.editReply({
        embeds: [new EmbedBuilder().setTitle(`${EU.error} Erreur suppression`).setDescription(`\`\`\`${err.message}\`\`\``).setColor(0xff0000)]
      });
    }
    clearInterval(deleteInterval);

    // ── Étape 4 — Résultat final ──
    const resultLines = result.details.length > 0
      ? result.details.map(d => `${EU.database} **${d.db}** — \`${d.deleted}\` ligne(s) supprimée(s)`).join('\n')
      : '*(aucun détail)*';

    return interaction.editReply({
      embeds: [
        new EmbedBuilder()
          .setTitle(`${EU.success} Suppression effectuée`)
          .setDescription(
            `${EU.search} **Requête :** \`${query}\`\n` +
            `${EU.delete} **Total supprimé :** \`${result.totalDeleted}\` ligne(s)\n` +
            `━━━━━━━━━━━━━━━━━━━━━━\n` +
            resultLines
          )
          .setColor(0x00ff99)
          .setFooter({ text: `Exécuté par ${interaction.user.tag}` })
          .setTimestamp()
      ]
    });
  }
};
