const {
  SlashCommandBuilder, EmbedBuilder,
  ActionRowBuilder, StringSelectMenuBuilder
} = require('discord.js');
const config = require('../config.json');
const EO = config.emojis.operateurs;

module.exports = {
  data: new SlashCommandBuilder()
    .setName('genpaneloperator')
    .setDescription('📡 Génère le panel opérateur dans ce salon'),

  async execute(interaction) {
    await interaction.deferReply({ ephemeral: true });

    const embed = new EmbedBuilder()
      .setTitle('📡 Opérateurs')
      .setDescription('Choisissez l\'opérateur que vous voulez pour ensuite faire la recherche')
      .setColor(0x5865f2)
      .setImage('https://cdn.discordapp.com/attachments/1470938724102701146/1482328968194555944/image.png')
      .setFooter({ text: 'db lookup Opérateur' })
      .setTimestamp();

    const menu = new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId('select_operateur')
        .setPlaceholder('Choisissez un opérateur')
        .addOptions([
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
        ])
    );

    await interaction.channel.send({ embeds: [embed], components: [menu] });
    await interaction.editReply({ content: '✅ Panel généré !' });
  }
};
