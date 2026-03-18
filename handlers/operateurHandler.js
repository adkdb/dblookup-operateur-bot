const {
  EmbedBuilder, ModalBuilder, TextInputBuilder,
  TextInputStyle, ActionRowBuilder,
  ButtonBuilder, ButtonStyle, ComponentType,
  StringSelectMenuBuilder
} = require('discord.js');
const search = require('../utils/search');
const config = require('../config.json');

const E  = config.emojis;
const EO = E.operateurs;
const EF = E.fields;
const EU = E.ui;

const OPERATEURS = {
  global:     { label: 'Global',          emoji: EO.global,     color: 0x5865F2 },
  sfr:        { label: 'SFR',             emoji: EO.sfr,        color: 0xFF0000 },
  bouygues:   { label: 'Bouygues',        emoji: EO.bouygues,   color: 0x0066CC },
  free:       { label: 'Free',            emoji: EO.free,       color: 0x8B00FF },
  orange:     { label: 'Orange',          emoji: EO.orange,     color: 0xFF8C00 },
  lycamobile: { label: 'Lycamobile',      emoji: EO.lycamobile, color: 0x00AA00 },
  zenmobile:  { label: 'Zenmobile',       emoji: EO.zenmobile,  color: 0xFFD700 },
  laposte:    { label: 'La Poste Mobile', emoji: EO.laposte,    color: 0xFFCC00 },
  syma:       { label: 'Syma Mobile',     emoji: EO.syma,       color: 0xFF6600 },
  coriolis:   { label: 'Coriolis',        emoji: EO.coriolis,   color: 0x0099CC },
  nrj:        { label: 'NRJ Mobile',      emoji: EO.nrj,        color: 0xFF3399 },
  orange_be:  { label: 'Orange BE',       emoji: EO.orange_be,  color: 0xFF8C00 },
};

const PANEL_OPTIONS = [
  { label: 'Global',          value: 'global',     emoji: EO.global     },
  { label: 'SFR',             value: 'sfr',        emoji: EO.sfr        },
  { label: 'Bouygues',        value: 'bouygues',   emoji: EO.bouygues   },
  { label: 'Free',            value: 'free',       emoji: EO.free       },
  { label: 'Orange',          value: 'orange',     emoji: EO.orange     },
  { label: 'Lycamobile',      value: 'lycamobile', emoji: EO.lycamobile },
  { label: 'Zenmobile',       value: 'zenmobile',  emoji: EO.zenmobile  },
  { label: 'La Poste Mobile', value: 'laposte',    emoji: EO.laposte    },
  { label: 'Syma Mobile',     value: 'syma',       emoji: EO.syma       },
  { label: 'Coriolis',        value: 'coriolis',   emoji: EO.coriolis   },
  { label: 'NRJ Mobile',      value: 'nrj',        emoji: EO.nrj        },
  { label: 'Orange BE',       value: 'orange_be',  emoji: EO.orange_be  },
];

const DOT_FRAMES = ['⣾','⣽','⣻','⢿','⡿','⣟','⣯','⣷'];
const DB_SIZE    = '50.2 GB';

const PRIORITY = [
  'tel','tel_fixe','tel_porte','prenom','nom','email',
  'adresse','rue','numero_rue','ville','code_postal',
  'date_naissance','iban','bic','imei','iccid',
  'operateur','pays','departement','piece_numero','piece_type',
  'abonnement','abonnement_prix','abonnement_statut',
  'date_inscription','date_activation','statut_compte','login'
];

// ─────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────
function normalizeSource(source) {
  const s = source.toLowerCase();
  if (s.includes('zenmobile'))  return 'zenmobile';
  if (s.includes('free'))       return 'free';
  if (s.includes('bouygues'))   return 'bouygues';
  if (s.includes('orange_be'))  return 'orange_be';
  if (s.includes('orange'))     return 'orange';
  if (s.includes('sfr'))        return 'sfr';
  if (s.includes('syma'))       return 'syma';
  if (s.includes('laposte'))    return 'laposte';
  if (s.includes('lyca'))       return 'lycamobile';
  if (s.includes('coriolis'))   return 'coriolis';
  if (s.includes('nrj'))        return 'nrj';
  return source;
}

function buildResetMenu() {
  return new ActionRowBuilder().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId('select_operateur')
      .setPlaceholder('Choisissez un opérateur')
      .addOptions(PANEL_OPTIONS)
  );
}

function formatField(key, val) {
  const icon = EF[key] || '▪️';
  return `${icon} **${key}:** \`${String(val).slice(0, 100)}\``;
}

function formatResult(result) {
  const entries = Object.entries(result.data)
    .filter(([, v]) => v !== '' && v !== null && v !== undefined);
  if (entries.length === 0) return '*(aucune donnée)*';
  entries.sort(([a], [b]) => {
    const pa = PRIORITY.indexOf(a), pb = PRIORITY.indexOf(b);
    if (pa === -1 && pb === -1) return a.localeCompare(b);
    if (pa === -1) return 1;
    if (pb === -1) return -1;
    return pa - pb;
  });
  return entries.map(([k, v]) => formatField(k, v)).join('\n');
}

function buildExportTxt(results, query) {
  const header =
    `${'━'.repeat(45)}\n` +
    `Data Lookup — Export\n` +
    `Recherche : ${query}\n` +
    `Total     : ${results.length} résultat(s)\n` +
    `Date      : ${new Date().toLocaleString('fr-FR')}\n` +
    `${'━'.repeat(45)}\n\n`;

  const body = results.map((r, idx) => {
    const fields = Object.entries(r.data)
      .filter(([, v]) => v !== '' && v !== null)
      .map(([k, v]) => `  ${k.padEnd(24)}: ${v}`)
      .join('\n');
    return `[${String(idx + 1).padStart(4, '0')}] Source: ${r.source}\n${fields}`;
  }).join('\n\n' + '─'.repeat(45) + '\n\n');

  return header + body;
}

// ─────────────────────────────────────────
// EMBEDS
// ─────────────────────────────────────────
function buildLoadingEmbed(query, op, frame, dbsDone, dbsTotal) {
  const pct    = dbsTotal > 0 ? Math.min(Math.round((dbsDone / dbsTotal) * 100), 99) : 0;
  const filled = Math.round(pct / 10);
  const bar    = '█'.repeat(filled) + '░'.repeat(10 - filled);
  return new EmbedBuilder()
    .setTitle(`${EU.loading} Recherche en cours...`)
    .setDescription(
      `${EU.search} **Requête :** \`${query}\`\n` +
      `${EU.operateur_label} **Opérateur :** ${op.emoji} ${op.label}\n` +
      `${EU.database} **Base de données :** ${DB_SIZE}\n\n` +
      `${DOT_FRAMES[frame % DOT_FRAMES.length]} Interrogation en cours...\n` +
      `\`[${bar}] ${pct}%\` — ${dbsDone}/${dbsTotal} DB`
    )
    .setColor(0xffa500);
}

function buildSingleEmbed(result, index, total, query, operateur, elapsed) {
  const op      = OPERATEURS[operateur] || { label: operateur, emoji: EU.operateur_label, color: 0x00ff99 };
  const srcNorm = normalizeSource(result.source);
  const srcOp   = OPERATEURS[srcNorm] || { emoji: EU.operateur_label, label: result.source };
  return new EmbedBuilder()
    .setTitle(`${op.emoji} ${op.label}`)
    .setColor(op.color)
    .setDescription(
      `${EU.search} **Recherche :** \`${query}\`\n` +
      `${EU.source} **Source :** ${srcOp.emoji} \`${result.source}\`\n` +
      `━━━━━━━━━━━━━━━━━━━━━━\n` +
      formatResult(result)
    )
    .setFooter({ text: `Data Lookup • ${index + 1}/${total} résultats • ${elapsed}s` })
    .setTimestamp();
}

function buildGlobalSummaryEmbed(results, sources, query, elapsed) {
  const lines = sources.map(s => {
    const op    = OPERATEURS[s] || { emoji: EU.operateur_label, label: s };
    const count = results.filter(r => normalizeSource(r.source) === s).length;
    return `${op.emoji} **${op.label}** — \`${count}\` résultat(s)`;
  });
  return new EmbedBuilder()
    .setTitle(`${EU.search} Résultats trouvés`)
    .setColor(0x5865F2)
    .setDescription(
      `${EU.search} **Recherche :** \`${query}\`\n` +
      `${EU.total} **Total :** ${results.length} résultat(s)\n` +
      `${EU.time} **Temps :** \`${elapsed}s\`\n` +
      `━━━━━━━━━━━━━━━━━━━━━━\n` +
      lines.join('\n')
    )
    .setFooter({ text: 'db lookup • Naviguez avec les menus ci-dessous' })
    .setTimestamp();
}

function buildGlobalResultEmbed(source, list, query, page, elapsed) {
  const op     = OPERATEURS[source] || { label: source, emoji: EU.operateur_label, color: 0x00ff99 };
  const result = list[page];
  return new EmbedBuilder()
    .setTitle(`${op.emoji} ${op.label}`)
    .setColor(op.color)
    .setDescription(
      `${EU.search} **Recherche :** \`${query}\`\n` +
      `━━━━━━━━━━━━━━━━━━━━━━\n` +
      formatResult(result)
    )
    .setFooter({ text: `Data Lookup • ${op.label} • ${page + 1}/${list.length} • ${elapsed}s` })
    .setTimestamp();
}

// ─────────────────────────────────────────
// BOUTONS
// ─────────────────────────────────────────
function buildNavButtons(id, index, total) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`${id}_first`).setEmoji('⏮️')
      .setStyle(ButtonStyle.Secondary).setDisabled(index === 0),
    new ButtonBuilder().setCustomId(`${id}_prev`).setEmoji('◀️')
      .setStyle(ButtonStyle.Primary).setDisabled(index === 0),
    new ButtonBuilder().setCustomId(`${id}_info`)
      .setLabel(`${index + 1} / ${total}`)
      .setStyle(ButtonStyle.Secondary).setDisabled(true),
    new ButtonBuilder().setCustomId(`${id}_next`).setEmoji('▶️')
      .setStyle(ButtonStyle.Primary).setDisabled(index >= total - 1),
    new ButtonBuilder().setCustomId(`${id}_last`).setEmoji('⏭️')
      .setStyle(ButtonStyle.Secondary).setDisabled(index >= total - 1)
  );
}

function buildActionButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('lookup_export')
      .setLabel(`${EU.export} Exporter`).setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('lookup_close')
      .setLabel(`${EU.fermer} Fermer`).setStyle(ButtonStyle.Danger)
  );
}

// ─────────────────────────────────────────
// HANDLER PRINCIPAL
// ─────────────────────────────────────────
module.exports = async function handleOperateur(interaction) {

  // ─── Menu panel → ouvrir modal ───
  if (interaction.isStringSelectMenu() && interaction.customId === 'select_operateur') {
    const operateur = interaction.values[0];
    const op        = OPERATEURS[operateur] || { label: operateur };
    const modal     = new ModalBuilder()
      .setCustomId(`modal_lookup_${operateur}`)
      .setTitle(`${EU.search} Recherche — ${op.label}`);
    const input = new TextInputBuilder()
      .setCustomId('search_input')
      .setLabel('Données à rechercher')
      .setPlaceholder('Numéro, Nom, Prénom, Email, IBAN, date...')
      .setStyle(TextInputStyle.Short)
      .setRequired(true).setMaxLength(100);
    modal.addComponents(new ActionRowBuilder().addComponents(input));
    await interaction.showModal(modal);
    return;
  }

  // ─── Modal soumis ───
  if (interaction.isModalSubmit()) {
    const operateur = interaction.customId.replace('modal_lookup_', '');
    const query     = interaction.fields.getTextInputValue('search_input').trim();
    const isGlobal  = operateur === 'global';
    const op        = OPERATEURS[operateur] || { label: operateur, emoji: EU.operateur_label, color: 0x00ff99 };

    // ✅ Reset menu au placeholder
    try {
      await interaction.message.edit({ components: [buildResetMenu()] });
    } catch (_) {}

    await interaction.deferReply({ flags: 64 });

    let frame    = 0;
    let dbsDone  = 0;
    const dbsTotal = isGlobal ? 9 : 1;

    await interaction.editReply({ embeds: [buildLoadingEmbed(query, op, frame, dbsDone, dbsTotal)] });

    const loadingInterval = setInterval(async () => {
      frame++;
      if (dbsDone < dbsTotal) dbsDone = Math.min(dbsDone + 1, dbsTotal - 1);
      try {
        await interaction.editReply({ embeds: [buildLoadingEmbed(query, op, frame, dbsDone, dbsTotal)] });
      } catch (_) {}
    }, 1500);

    const startTime = Date.now();
    let results;
    try {
      results = await search(query, { limit: 99999, dbFilter: isGlobal ? null : operateur });
    } catch (err) {
      clearInterval(loadingInterval);
      return interaction.editReply({
        embeds: [new EmbedBuilder().setTitle(`${EU.error} Erreur`).setDescription(`\`\`\`${err.message}\`\`\``).setColor(0xff0000)]
      });
    }
    clearInterval(loadingInterval);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);

    if (!Array.isArray(results)) results = [];

    if (results.length === 0) {
      return interaction.editReply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`${EU.error} Aucun résultat`)
            .setDescription(
              `${EU.search} **Recherche :** \`${query}\`\n` +
              `${EU.operateur_label} **Opérateur :** ${op.emoji} ${op.label}\n\n` +
              `Aucune correspondance trouvée.\n` +
              `💡 Essaie avec un autre format ou sélectionne **Global**.`
            )
            .setColor(0xff4444)
            .setFooter({ text: `Recherche effectuée en ${elapsed}s` })
            .setTimestamp()
        ]
      });
    }

    // ══════════════════════════════════════
    // MODE OPÉRATEUR UNIQUE
    // ══════════════════════════════════════
    if (!isGlobal) {
      let index = 0;
      const total = results.length;

      const getComponents = () => {
        const rows = [];
        if (total > 1) rows.push(buildNavButtons(`nav_${operateur}`, index, total));
        rows.push(buildActionButtons());
        return rows;
      };

      const msg = await interaction.editReply({
        embeds:     [buildSingleEmbed(results[index], index, total, query, operateur, elapsed)],
        components: getComponents()
      });

      const collector = msg.createMessageComponentCollector({
        componentType: ComponentType.Button,
        time:   10 * 60 * 1000,
        filter: i => i.user.id === interaction.user.id
      });

      collector.on('collect', async i => {
        await i.deferUpdate();
        const id = i.customId;

        if      (id === `nav_${operateur}_first`) index = 0;
        else if (id === `nav_${operateur}_prev`)  index = Math.max(0, index - 1);
        else if (id === `nav_${operateur}_next`)  index = Math.min(total - 1, index + 1);
        else if (id === `nav_${operateur}_last`)  index = total - 1;

        else if (id === 'lookup_export') {
          const txt = buildExportTxt(results, query);
          await interaction.followUp({
            content: `${EU.export} **Export** — \`${query}\` — **${total}** résultat(s) — \`${elapsed}s\``,
            files: [{
              attachment: Buffer.from(txt, 'utf-8'),
              name: `lookup_${operateur}_${query.replace(/\s+/g, '_')}.txt`
            }],
            flags: 64
          });
          return;
        }

        else if (id === 'lookup_close') {
          collector.stop('closed');
          await interaction.editReply({
            embeds: [
              new EmbedBuilder()
                .setTitle(`${EU.locked} Recherche fermée`)
                .setDescription(`\`${query}\` — **${total}** résultat(s) en \`${elapsed}s\``)
                .setColor(0x808080).setTimestamp()
            ],
            components: []
          });
          return;
        }

        await interaction.editReply({
          embeds:     [buildSingleEmbed(results[index], index, total, query, operateur, elapsed)],
          components: getComponents()
        });
      });

      collector.on('end', async (_, reason) => {
        if (reason === 'closed') return;
        try { await interaction.editReply({ components: [] }); } catch (_) {}
      });

      return;
    }

    // ══════════════════════════════════════
    // MODE GLOBAL
    // ══════════════════════════════════════
    const grouped = {};
    for (const r of results) {
      const src = normalizeSource(r.source);
      if (!grouped[src]) grouped[src] = [];
      grouped[src].push(r);
    }

    const sources   = Object.keys(grouped);
    const pageState = {};
    sources.forEach(s => pageState[s] = 0);

    await interaction.editReply({
      embeds:     [buildGlobalSummaryEmbed(results, sources, query, elapsed)],
      components: [buildActionButtons()]
    });

    const followUpMsgs = {};

    for (const src of sources) {
      const list  = grouped[src];
      const total = list.length;

      const getComps = (page) => {
        const rows = [];
        if (total > 1) rows.push(buildNavButtons(`gnav_${src}`, page, total));
        return rows;
      };

      const msg = await interaction.followUp({
        embeds:     [buildGlobalResultEmbed(src, list, query, 0, elapsed)],
        components: getComps(0),
        flags:      64
      });
      followUpMsgs[src] = msg;

      if (total > 1) {
        const collector = msg.createMessageComponentCollector({
          componentType: ComponentType.Button,
          time:   10 * 60 * 1000,
          filter: i => i.user.id === interaction.user.id
        });

        collector.on('collect', async i => {
          await i.deferUpdate();
          const match = i.customId.match(/^gnav_(.+)_(first|prev|next|last)$/);
          if (!match) return;
          const source = match[1];
          const action = match[2];
          const t      = grouped[source]?.length || 0;

          if      (action === 'first') pageState[source] = 0;
          else if (action === 'prev')  pageState[source] = Math.max(0, pageState[source] - 1);
          else if (action === 'next')  pageState[source] = Math.min(t - 1, pageState[source] + 1);
          else if (action === 'last')  pageState[source] = t - 1;

          await i.editReply({
            embeds:     [buildGlobalResultEmbed(source, grouped[source], query, pageState[source], elapsed)],
            components: getComps(pageState[source])
          });
        });

        collector.on('end', async () => {
          try { await msg.edit({ components: [] }); } catch (_) {}
        });
      }
    }

    const mainMsg       = await interaction.fetchReply();
    const mainCollector = mainMsg.createMessageComponentCollector({
      time:   10 * 60 * 1000,
      filter: i => i.user.id === interaction.user.id && i.message.id === mainMsg.id
    });

    mainCollector.on('collect', async i => {
      await i.deferUpdate();

      if (i.customId === 'lookup_export') {
        const txt = buildExportTxt(results, query);
        await interaction.followUp({
          content: `${EU.export} **Export Global** — \`${query}\` — **${results.length}** résultat(s) — \`${elapsed}s\``,
          files: [{
            attachment: Buffer.from(txt, 'utf-8'),
            name: `lookup_global_${query.replace(/\s+/g, '_')}.txt`
          }],
          flags: 64
        });
        return;
      }

      if (i.customId === 'lookup_close') {
        mainCollector.stop('closed');
        await interaction.editReply({
          embeds: [
            new EmbedBuilder()
              .setTitle(`${EU.locked} Recherche fermée`)
              .setDescription(`\`${query}\` — **${results.length}** résultat(s) en \`${elapsed}s\``)
              .setColor(0x808080).setTimestamp()
          ],
          components: []
        });
      }
    });

    mainCollector.on('end', async (_, reason) => {
      if (reason === 'closed') return;
      try { await interaction.editReply({ components: [] }); } catch (_) {}
    });
  }
};
