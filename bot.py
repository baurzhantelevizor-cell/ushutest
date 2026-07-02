"""
Discord-бот для кастомных матчей 5x5 в Mobile Legends.
Хостинг: Railway + PostgreSQL.
Настройки хранятся в .env (локально) или в Variables (Railway).
"""

import os
import random
import asyncio
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands
import asyncpg
from dotenv import load_dotenv

# Импортируем маппинг ролей героев
try:
    from roles import HERO_ROLES
except ImportError:
    HERO_ROLES = {}

# ─────────────────────────── .env ────────────────────────────────
load_dotenv()  # Загружает .env в os.environ (на Railway .env не нужен)

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
GUILD_ID = int(os.environ.get("GUILD_ID", "0"))

# Путь к файлу с героями
HEROES_FILE = Path(__file__).parent / "heroes.txt"

# Дефолтный ЭЛО для новых игроков
DEFAULT_ELO = 1000

# ─────────────────────────── БОТ ─────────────────────────────────
intents = discord.Intents.default()
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
db_pool: asyncpg.Pool | None = None

# Кэш настроек гильдии: guild_id → {voice_channel_id, ready_channel_id}
guild_settings_cache: dict[int, dict[str, int | None]] = {}


# ═══════════════════════════ DATABASE ════════════════════════════
async def init_db() -> asyncpg.Pool:
    """Создаём пул соединений и таблицы, если их ещё нет."""
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                user_id  BIGINT PRIMARY KEY,
                elo      INTEGER NOT NULL DEFAULT 1000
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id          BIGINT PRIMARY KEY,
                voice_channel_id  BIGINT,
                ready_channel_id  BIGINT
            );
            """
        )
        # Таблица для хранения выданных игрокам героев в текущих матчах
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_match_heroes (
                user_id   BIGINT PRIMARY KEY,
                guild_id  BIGINT NOT NULL,
                hero_name VARCHAR(100) NOT NULL,
                role      VARCHAR(50) NOT NULL,
                match_id  VARCHAR(100) NOT NULL
            );
            """
        )
    return pool


async def load_all_guild_settings() -> None:
    """Загружает настройки всех гильдий из БД в кэш."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM guild_settings")
        for row in rows:
            guild_settings_cache[row["guild_id"]] = {
                "voice_channel_id": row["voice_channel_id"],
                "ready_channel_id": row["ready_channel_id"],
            }


async def save_guild_setting(guild_id: int, key: str, value: int) -> None:
    """Сохраняет одну настройку гильдии в БД и кэш."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO guild_settings (guild_id, {key}) VALUES ($1, $2)
            ON CONFLICT (guild_id) DO UPDATE SET {key} = $2;
            """,
            guild_id,
            value,
        )
    if guild_id not in guild_settings_cache:
        guild_settings_cache[guild_id] = {"voice_channel_id": None, "ready_channel_id": None}
    guild_settings_cache[guild_id][key] = value


def get_guild_voice_channel(guild_id: int) -> int | None:
    """Возвращает ID настроенного голосового канала для гильдии."""
    s = guild_settings_cache.get(guild_id)
    return s["voice_channel_id"] if s else None


def get_guild_ready_channel(guild_id: int) -> int | None:
    """Возвращает ID настроенного текстового канала для списка готовых."""
    s = guild_settings_cache.get(guild_id)
    return s["ready_channel_id"] if s else None


# ─────────────── Players ─────────────────
async def get_elo(user_id: int) -> int:
    """Возвращает ЭЛО игрока (DEFAULT_ELO, если не найден)."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT elo FROM players WHERE user_id = $1", user_id
        )
        if row:
            return row["elo"]
        await conn.execute(
            "INSERT INTO players (user_id, elo) VALUES ($1, $2) "
            "ON CONFLICT DO NOTHING",
            user_id,
            DEFAULT_ELO,
        )
        return DEFAULT_ELO


async def set_elo_db(user_id: int, elo: int) -> None:
    """Устанавливает ЭЛО для игрока (upsert)."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (user_id, elo) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET elo = $2;
            """,
            user_id,
            elo,
        )


async def get_elos_bulk(user_ids: list[int]) -> dict[int, int]:
    """Пакетно получает ЭЛО для списка игроков."""
    result: dict[int, int] = {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, elo FROM players WHERE user_id = ANY($1::bigint[])",
            user_ids,
        )
        for row in rows:
            result[row["user_id"]] = row["elo"]

        missing = [uid for uid in user_ids if uid not in result]
        if missing:
            await conn.executemany(
                "INSERT INTO players (user_id, elo) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING",
                [(uid, DEFAULT_ELO) for uid in missing],
            )
            for uid in missing:
                result[uid] = DEFAULT_ELO
    return result


# ═══════════════════════════ HEROES & ROLES ══════════════════════
def load_heroes() -> list[str]:
    """Загружает список героев из heroes.txt."""
    with open(HEROES_FILE, encoding="utf-8") as f:
        heroes = [line.strip() for line in f if line.strip()]
    if len(heroes) < 10:
        raise ValueError(
            f"В heroes.txt слишком мало героев ({len(heroes)}). Нужно минимум 10."
        )
    return heroes


def pick_unique_heroes_by_roles(heroes: list[str]) -> list[tuple[str, str]]:
    """
    Выбирает по 2 уникальных героя для каждой роли:
    gold, exp, mid, jungle, roam.
    Возвращает список из 10 кортежей (имя_героя, роль).
    """
    # Распределяем доступных героев по 5 спискам
    by_role = {"gold": [], "exp": [], "mid": [], "jungle": [], "roam": []}
    for h in heroes:
        role = HERO_ROLES.get(h, "exp")  # По дефолту exp, если нет в словаре
        if role in by_role:
            by_role[role].append(h)

    result = []
    roles_list = ["gold", "exp", "mid", "jungle", "roam"]
    for role in roles_list:
        role_heroes = by_role[role]
        if len(role_heroes) < 2:
            # Если для какой-то роли мало героев, добираем из всех
            available = [h for h in heroes if h not in [r[0] for r in result]]
            sampled = random.sample(available, 2)
            for h in sampled:
                result.append((h, role))
        else:
            sampled = random.sample(role_heroes, 2)
            for h in sampled:
                result.append((h, role))
    return result


def pick_unique_heroes(heroes: list[str], count: int = 10) -> list[str]:
    """Возвращает count уникальных случайных героев."""
    return random.sample(heroes, count)


# ═══════════════════════════ BALANCER ════════════════════════════
def balance_teams_snake(
    players_with_elo: list[tuple[discord.Member, int]],
) -> tuple[list[tuple[discord.Member, int]], list[tuple[discord.Member, int]]]:
    """
    Распределяет 10 игроков «змейкой» по ЭЛО.
    Сортировка по убыванию ЭЛО, далее:
      Pick 1 → Team A
      Pick 2 → Team B
      Pick 3 → Team B
      Pick 4 → Team A
      ...и так далее.
    """
    sorted_players = sorted(players_with_elo, key=lambda x: x[1], reverse=True)
    team_a: list[tuple[discord.Member, int]] = []
    team_b: list[tuple[discord.Member, int]] = []

    for idx, player in enumerate(sorted_players):
        if idx % 2 == 0:
            team_a.append(player)
        else:
            team_b.append(player)

    return team_a, team_b


# ═══════════════════════════ EMBEDS ══════════════════════════════
EMBED_COLOR_LOBBY = 0x2B2D31       # тёмно-серый (дискорд-стиль)
EMBED_COLOR_READY = 0x57F287       # зелёный
EMBED_COLOR_TEAM_A = 0x5865F2      # синий (Blurple)
EMBED_COLOR_TEAM_B = 0xED4245      # красный


def build_lobby_embed(
    voice_members: list[discord.Member],
    ready_ids: set[int],
    voice_channel_name: str,
) -> discord.Embed:
    """Строит embed со списком игроков лобби."""
    ready_count = sum(1 for m in voice_members if m.id in ready_ids)
    total = len(voice_members)

    embed = discord.Embed(
        title="⚔️  MOBILE LEGENDS — КАСТОМНЫЙ МАТЧ 5×5",
        description=(
            f"🔊 Голосовой канал: **{voice_channel_name}**\n"
            f"👥 Игроков: **{total}** · ✅ Готовы: **{ready_count}** / 10\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=EMBED_COLOR_READY if ready_count >= 10 else EMBED_COLOR_LOBBY,
    )

    lines: list[str] = []
    for idx, member in enumerate(voice_members, start=1):
        if member.id in ready_ids:
            status = "✅ Готов"
        else:
            status = "⏳ Ожидание"
        lines.append(f"`{idx:>2}.` **{member.display_name}** — {status}")

    embed.add_field(
        name="📋 Список игроков",
        value="\n".join(lines) if lines else "_Пусто_",
        inline=False,
    )

    if ready_count < 10:
        embed.set_footer(text="Нажмите кнопку «Готов ✅», когда будете готовы к игре!")
    else:
        embed.set_footer(text="🎮 Все готовы! Формирование команд...")

    return embed


def build_result_embed(
    team: list[tuple[discord.Member, int, str, str]],
    team_name: str,
    color: int,
    total_elo: int,
) -> discord.Embed:
    """Embed для одной команды в итоговом сообщении."""
    embed = discord.Embed(
        title=f"{team_name}",
        description=f"Суммарный ЭЛО: **{total_elo}**",
        color=color,
    )

    role_emojis = {
        "gold": "🪙 Gold",
        "exp": "🛡️ Exp",
        "mid": "🔮 Mid",
        "jungle": "⚔️ Jungle",
        "roam": "👣 Roam"
    }

    lines: list[str] = []
    for idx, (member, elo, hero, role) in enumerate(team, start=1):
        role_label = role_emojis.get(role, role.capitalize())
        lines.append(
            f"`{idx}.` {member.mention} — 🎖️ {elo} ЭЛО\n"
            f"    Роль: **{role_label}** · 🦸 Герой: **{hero}**"
        )

    embed.add_field(
        name="Состав",
        value="\n".join(lines),
        inline=False,
    )
    return embed


# ═══════════════════════════ LOBBY STATE ═════════════════════════
class LobbyState:
    """Хранит активное лобби для одного голосового канала."""

    __slots__ = (
        "voice_channel",
        "text_channel",
        "message",
        "ready_ids",
        "lock",
        "finished",
    )

    def __init__(
        self,
        voice_channel: discord.VoiceChannel,
        text_channel: discord.TextChannel,
        message: discord.Message,
    ):
        self.voice_channel = voice_channel
        self.text_channel = text_channel
        self.message = message
        self.ready_ids: set[int] = set()
        self.lock = asyncio.Lock()
        self.finished = False


# Активные лобби: voice_channel_id → LobbyState
active_lobbies: dict[int, LobbyState] = {}


# ═══════════════════════════ BUTTON VIEW ═════════════════════════
class ReadyButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Готов ✅",
            style=discord.ButtonStyle.success,
            custom_id="mlbb_ready_button",
        )

    async def callback(self, interaction: discord.Interaction):
        # Ищем лобби, к которому относится эта кнопка
        lobby: LobbyState | None = None
        for lob in active_lobbies.values():
            if lob.message.id == interaction.message.id:
                lobby = lob
                break

        if lobby is None or lobby.finished:
            await interaction.response.send_message(
                "❌ Это лобби больше не активно.", ephemeral=True
            )
            return

        member = interaction.user
        # Проверяем, что игрок сидит в нужном войсе
        vc_members = lobby.voice_channel.members
        if member not in vc_members:
            await interaction.response.send_message(
                "❌ Вы должны быть в голосовом канале, чтобы нажать «Готов».",
                ephemeral=True,
            )
            return

        async with lobby.lock:
            if lobby.finished:
                await interaction.response.send_message(
                    "❌ Матч уже запущен.", ephemeral=True
                )
                return

            lobby.ready_ids.add(member.id)

            # Обновляем embed
            embed = build_lobby_embed(
                vc_members, lobby.ready_ids, lobby.voice_channel.name
            )

            # Считаем готовых, которые ещё в войсе
            ready_in_voice = [m for m in vc_members if m.id in lobby.ready_ids]

            if len(ready_in_voice) >= 10:
                lobby.finished = True
                # Блокируем кнопку
                self.disabled = True
                self.label = "Матч начат 🎮"
                self.style = discord.ButtonStyle.secondary
                await interaction.response.edit_message(embed=embed, view=self.view)
                # Запускаем формирование матча
                await start_match(lobby, ready_in_voice[:10])
            else:
                await interaction.response.edit_message(embed=embed, view=self.view)


class ReadyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # Без таймаута
        self.add_item(ReadyButton())


# ═══════════════════════════ MATCH LOGIC ═════════════════════════
async def start_match(lobby: LobbyState, players: list[discord.Member]):
    """Формирует команды, раздаёт героев и выводит результат."""
    heroes = load_heroes()
    
    # Распределяем роли и героев: 5 ролей, по 2 уникальных героя на роль
    role_picks = pick_unique_heroes_by_roles(heroes)

    # Получаем ЭЛО из БД
    user_ids = [p.id for p in players]
    elos = await get_elos_bulk(user_ids)

    players_with_elo = [(p, elos[p.id]) for p in players]
    team_a, team_b = balance_teams_snake(players_with_elo)

    # Каждой команде выдаём по одному герою каждого типа
    # Роли: gold, exp, mid, jungle, roam
    roles_order = ["gold", "exp", "mid", "jungle", "roam"]
    
    # Разделяем героев по ролям
    role_to_heroes = {r: [] for r in roles_order}
    for h_name, role in role_picks:
        role_to_heroes[role].append(h_name)

    # Распределяем роли внутри команд
    # Для Команды 1
    team_a_full = []
    for i, (member, elo) in enumerate(team_a):
        role = roles_order[i]
        hero = role_to_heroes[role][0]
        team_a_full.append((member, elo, hero, role))

    # Для Команды 2
    team_b_full = []
    for i, (member, elo) in enumerate(team_b):
        role = roles_order[i]
        hero = role_to_heroes[role][1]
        team_b_full.append((member, elo, hero, role))

    total_a = sum(elo for _, elo, _, _ in team_a_full)
    total_b = sum(elo for _, elo, _, _ in team_b_full)

    # Сохраняем выданных героев в БД для возможности крутки (reroll)
    match_id = f"{lobby.voice_channel.id}_{int(asyncio.get_event_loop().time())}"
    async with db_pool.acquire() as conn:
        # Сначала очистим старые записи для этой гильдии
        await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", lobby.voice_channel.guild.id)
        
        # Сохраняем новых
        for member, elo, hero, role in team_a_full + team_b_full:
            await conn.execute(
                """
                INSERT INTO active_match_heroes (user_id, guild_id, hero_name, role, match_id)
                VALUES ($1, $2, $3, $4, $5)
                """,
                member.id, lobby.voice_channel.guild.id, hero, role, match_id
            )

    embed_a = build_result_embed(team_a_full, "🔵 КОМАНДА 1", EMBED_COLOR_TEAM_A, total_a)
    embed_b = build_result_embed(team_b_full, "🔴 КОМАНДА 2", EMBED_COLOR_TEAM_B, total_b)

    mentions = " ".join(m.mention for m in players)

    header_embed = discord.Embed(
        title="🏆  МАТЧ СФОРМИРОВАН!",
        description=(
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Игроки распределены по ролям. Удачи на поле боя! 🎮\n"
            "Если у вас нет выпавшего героя, напишите слэш-команду `/reroll`"
        ),
        color=0xFEE75C,
    )

    await lobby.text_channel.send(
        content=mentions,
        embeds=[header_embed, embed_a, embed_b],
    )

    # Удаляем лобби из активных сборов
    active_lobbies.pop(lobby.voice_channel.id, None)


# ═══════════════════════════ EVENTS ══════════════════════════════
@bot.event
async def on_ready():
    global db_pool
    db_pool = await init_db()
    await load_all_guild_settings()
    print(f"[DB] PostgreSQL подключен, таблицы готовы.")
    print(f"[DB] Загружены настройки для {len(guild_settings_cache)} гильдий.")

    # Регистрируем persistent view для кнопки
    bot.add_view(ReadyView())

    # Синхронизируем слэш-команды
    try:
        if GUILD_ID:
            guild_obj = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild_obj)
            synced = await bot.tree.sync(guild=guild_obj)
            print(f"[BOT] Синхронизировано {len(synced)} команд для гильдии {GUILD_ID}.")
        else:
            synced = await bot.tree.sync()
            print(f"[BOT] Синхронизировано {len(synced)} глобальных команд.")
    except Exception as e:
        print(f"[BOT] Ошибка синхронизации команд: {e}")

    print(f"[BOT] {bot.user} запущен и готов к работе!")


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
):
    """Автоматически обновляет embed лобби при входе/выходе из войса."""
    guild_id = member.guild.id

    # Проверяем, настроен ли голосовой канал для этой гильдии
    configured_vc = get_guild_voice_channel(guild_id)

    channels_to_check: set[int] = set()
    if before.channel:
        channels_to_check.add(before.channel.id)
    if after.channel:
        channels_to_check.add(after.channel.id)

    for vc_id in channels_to_check:
        # Если настроен конкретный войс — реагируем только на него
        if configured_vc and vc_id != configured_vc:
            continue

        lobby = active_lobbies.get(vc_id)
        if lobby is None or lobby.finished:
            continue

        async with lobby.lock:
            if lobby.finished:
                continue

            vc_members = lobby.voice_channel.members

            # Убираем «Готов» у тех, кто вышел из войса
            current_ids = {m.id for m in vc_members}
            lobby.ready_ids &= current_ids

            embed = build_lobby_embed(
                vc_members, lobby.ready_ids, lobby.voice_channel.name
            )
            try:
                await lobby.message.edit(embed=embed)
            except discord.NotFound:
                active_lobbies.pop(vc_id, None)


# ═══════════════════════════ SLASH COMMANDS ══════════════════════

# ───────────── /set_voice ─────────────
@bot.tree.command(
    name="set_voice",
    description="[Админ] Назначить голосовой канал для сбора лобби",
)
@app_commands.describe(channel="Голосовой канал, который бот будет сканировать")
@app_commands.default_permissions(administrator=True)
async def cmd_set_voice(
    interaction: discord.Interaction,
    channel: discord.VoiceChannel,
):
    await save_guild_setting(interaction.guild_id, "voice_channel_id", channel.id)
    await interaction.response.send_message(
        f"✅ Голосовой канал для сбора установлен: **{channel.name}** (`{channel.id}`)\n"
        f"Бот будет сканировать только этот войс.",
        ephemeral=True,
    )


# ───────────── /set_ready ─────────────
@bot.tree.command(
    name="set_ready",
    description="[Админ] Назначить текстовый канал для списка готовых",
)
@app_commands.describe(channel="Текстовый канал, куда бот будет писать лобби и результаты")
@app_commands.default_permissions(administrator=True)
async def cmd_set_ready(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
):
    await save_guild_setting(interaction.guild_id, "ready_channel_id", channel.id)
    await interaction.response.send_message(
        f"✅ Текстовый канал для лобби установлен: **{channel.name}** (`{channel.id}`)\n"
        f"Список готовых и результаты матчей будут публиковаться туда.",
        ephemeral=True,
    )


# ───────────── /settings ──────────────
@bot.tree.command(
    name="settings",
    description="[Админ] Показать текущие настройки бота на сервере",
)
@app_commands.default_permissions(administrator=True)
async def cmd_settings(interaction: discord.Interaction):
    vc_id = get_guild_voice_channel(interaction.guild_id)
    rc_id = get_guild_ready_channel(interaction.guild_id)

    vc_text = f"<#{vc_id}>" if vc_id else "❌ _Не задан_ — используй `/set_voice`"
    rc_text = f"<#{rc_id}>" if rc_id else "❌ _Не задан_ — используй `/set_ready`"

    embed = discord.Embed(
        title="⚙️  Настройки бота",
        color=0x5865F2,
    )
    embed.add_field(name="🔊 Голосовой канал", value=vc_text, inline=False)
    embed.add_field(name="📝 Текстовый канал (лобби)", value=rc_text, inline=False)
    embed.set_footer(text="Используй /set_voice and /set_ready для настройки")

    await interaction.response.send_message(embed=embed, ephemeral=True)


# ───────────── /start ─────────────────
@bot.tree.command(
    name="start",
    description="Начать сбор на кастомный матч 5×5 в Mobile Legends",
)
async def cmd_start(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    configured_vc = get_guild_voice_channel(guild_id)
    configured_rc = get_guild_ready_channel(guild_id)

    # Если настроен текстовый канал — команду можно использовать только там
    if configured_rc and interaction.channel_id != configured_rc:
        await interaction.response.send_message(
            f"❌ Эту команду можно использовать только в <#{configured_rc}>.",
            ephemeral=True,
        )
        return

    # Проверяем, что автор в голосовом канале
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "❌ Вы должны находиться в голосовом канале, чтобы начать сбор.",
            ephemeral=True,
        )
        return

    voice_channel = interaction.user.voice.channel

    # Если настроен конкретный войс — проверяем, что автор именно в нём
    if configured_vc and voice_channel.id != configured_vc:
        await interaction.response.send_message(
            f"❌ Сбор можно начать только из канала <#{configured_vc}>.",
            ephemeral=True,
        )
        return

    # Проверяем, нет ли уже лобби для этого войса
    if voice_channel.id in active_lobbies:
        await interaction.response.send_message(
            "❌ Для этого голосового канала уже идёт сбор!",
            ephemeral=True,
        )
        return

    vc_members = voice_channel.members
    ready_ids: set[int] = set()

    embed = build_lobby_embed(vc_members, ready_ids, voice_channel.name)
    view = ReadyView()

    # Если настроен текстовый канал для лобби — шлём туда, иначе отвечаем на месте
    if configured_rc and interaction.channel_id == configured_rc:
        await interaction.response.send_message(embed=embed, view=view)
        msg = await interaction.original_response()
        lobby = LobbyState(voice_channel, interaction.channel, msg)
    elif configured_rc:
        target_ch = interaction.guild.get_channel(configured_rc)
        if target_ch is None:
            await interaction.response.send_message(
                "❌ Настроенный текстовый канал не найден. Обратитесь к администратору.",
                ephemeral=True,
            )
            return
        msg = await target_ch.send(embed=embed, view=view)
        await interaction.response.send_message(
            f"✅ Сбор запущен в <#{configured_rc}>!", ephemeral=True
        )
        lobby = LobbyState(voice_channel, target_ch, msg)
    else:
        # Каналы не настроены — работаем прямо тут
        await interaction.response.send_message(embed=embed, view=view)
        msg = await interaction.original_response()
        lobby = LobbyState(voice_channel, interaction.channel, msg)

    active_lobbies[voice_channel.id] = lobby


# ───────────── /set_elo ───────────────
@bot.tree.command(name="set_elo", description="[Админ] Установить ЭЛО игроку")
@app_commands.describe(player="Игрок", elo="Новое значение ЭЛО")
@app_commands.default_permissions(administrator=True)
async def cmd_set_elo(
    interaction: discord.Interaction,
    player: discord.Member,
    elo: int,
):
    if elo < 0 or elo > 9999:
        await interaction.response.send_message(
            "❌ ЭЛО должно быть от 0 до 9999.", ephemeral=True
        )
        return

    await set_elo_db(player.id, elo)
    await interaction.response.send_message(
        f"✅ ЭЛО игрока **{player.display_name}** установлено на **{elo}**.",
        ephemeral=True,
    )


# ───────────── /elo ───────────────────
@bot.tree.command(name="elo", description="Посмотреть свой текущий ЭЛО-рейтинг")
async def cmd_elo(interaction: discord.Interaction):
    elo = await get_elo(interaction.user.id)
    await interaction.response.send_message(
        f"🎖️ Ваш текущий ЭЛО: **{elo}**", ephemeral=True
    )


# ───────────── /cancel ────────────────
@bot.tree.command(name="cancel", description="Отменить текущий сбор на матч")
@app_commands.default_permissions(administrator=True)
async def cmd_cancel(interaction: discord.Interaction):
    to_remove: list[int] = []
    for vc_id, lobby in active_lobbies.items():
        if lobby.text_channel.id == interaction.channel_id:
            lobby.finished = True
            to_remove.append(vc_id)
            try:
                await lobby.message.edit(
                    embed=discord.Embed(
                        title="❌ Сбор отменён",
                        description="Администратор отменил сбор на матч.",
                        color=0xED4245,
                    ),
                    view=None,
                )
            except discord.NotFound:
                pass

    for vc_id in to_remove:
        active_lobbies.pop(vc_id, None)

    if to_remove:
        await interaction.response.send_message("✅ Сбор отменён.", ephemeral=True)
    else:
        await interaction.response.send_message(
            "❌ В этом канале нет активного сбора.", ephemeral=True
        )


# ───────────── /reroll ────────────────
@bot.tree.command(
    name="reroll",
    description="Перекрутить (заменить) вашего выданного героя на другого для вашей роли"
)
async def cmd_reroll(interaction: discord.Interaction):
    user = interaction.user
    guild_id = interaction.guild_id

    async with db_pool.acquire() as conn:
        # Проверяем, есть ли активный герой у игрока в этой гильдии
        row = await conn.fetchrow(
            """
            SELECT hero_name, role, match_id 
            FROM active_match_heroes 
            WHERE user_id = $1 AND guild_id = $2
            """,
            user.id, guild_id
        )

        if not row:
            await interaction.response.send_message(
                "❌ Вы не участвуете в текущем сформированном матче.",
                ephemeral=True
            )
            return

        current_hero = row["hero_name"]
        role = row["role"]
        match_id = row["match_id"]

        # Получаем всех героев, которые сейчас выданы в этом матче, чтобы избежать повторов
        busy_rows = await conn.fetch(
            "SELECT hero_name FROM active_match_heroes WHERE match_id = $1",
            match_id
        )
        busy_heroes = {r["hero_name"] for r in busy_rows}

        # Выбираем всех героев с такой же ролью
        heroes = load_heroes()
        role_heroes = [
            h for h in heroes 
            if HERO_ROLES.get(h, "exp") == role and h not in busy_heroes and h != current_hero
        ]

        if not role_heroes:
            # Если нет свободных героев с этой ролью, берем из общего пула (кроме занятых)
            role_heroes = [h for h in heroes if h not in busy_heroes and h != current_hero]

        if not role_heroes:
            await interaction.response.send_message(
                "❌ К сожалению, нет доступных героев для замены.",
                ephemeral=True
            )
            return

        new_hero = random.choice(role_heroes)

        # Обновляем героя в БД
        await conn.execute(
            """
            UPDATE active_match_heroes 
            SET hero_name = $1 
            WHERE user_id = $2 AND guild_id = $3
            """,
            new_hero, user.id, guild_id
        )

        role_emojis = {
            "gold": "🪙 Gold",
            "exp": "🛡️ Exp",
            "mid": "🔮 Mid",
            "jungle": "⚔️ Jungle",
            "roam": "👣 Roam"
        }
        role_label = role_emojis.get(role, role.capitalize())

        await interaction.response.send_message(
            f"🔄 {user.mention}, ваш герой был заменен!\n"
            f"Роль: **{role_label}**\n"
            f"Старый герой: ~~{current_hero}~~\n"
            f"Новый герой: **{new_hero}**"
        )


# ───────────── /start_test ────────────
@bot.tree.command(
    name="start_test",
    description="[Админ] Тестовый запуск — симулирует матч с 10 фейковыми игроками",
)
@app_commands.default_permissions(administrator=True)
async def cmd_start_test(interaction: discord.Interaction):
    """Генерирует полный вывод матча с фейковыми игроками для превью."""
    await interaction.response.defer()

    # 10 фейковых имён и случайный ЭЛО
    fake_names = [
        "🎮 Player_1", "🎮 Player_2", "🎮 Player_3", "🎮 Player_4",
        "🎮 Player_5", "🎮 Player_6", "🎮 Player_7", "🎮 Player_8",
        "🎮 Player_9", "🎮 Player_10",
    ]
    fake_elos = [random.randint(700, 1500) for _ in range(10)]

    # Сортировка по ЭЛО и распределение змейкой
    indexed = sorted(enumerate(fake_elos), key=lambda x: x[1], reverse=True)
    team_a_idx: list[int] = []
    team_b_idx: list[int] = []
    for pick, (orig_idx, _) in enumerate(indexed):
        if pick % 2 == 0:
            team_a_idx.append(orig_idx)
        else:
            team_b_idx.append(orig_idx)

    # Рандомные герои по ролям
    heroes = load_heroes()
    role_picks = pick_unique_heroes_by_roles(heroes)
    
    roles_order = ["gold", "exp", "mid", "jungle", "roam"]
    role_to_heroes = {r: [] for r in roles_order}
    for h_name, role in role_picks:
        role_to_heroes[role].append(h_name)

    # Сборка команд
    role_emojis = {
        "gold": "🪙 Gold",
        "exp": "🛡️ Exp",
        "mid": "🔮 Mid",
        "jungle": "⚔️ Jungle",
        "roam": "👣 Roam"
    }

    def build_test_team_lines(indices: list[int], hero_index: int) -> tuple[str, int]:
        lines = []
        total = 0
        for i, idx in enumerate(indices):
            name = fake_names[idx]
            elo = fake_elos[idx]
            role = roles_order[i]
            hero = role_to_heroes[role][hero_index]
            total += elo
            role_label = role_emojis.get(role, role.capitalize())
            lines.append(
                f"`{i+1}.` **{name}** — 🎖️ {elo} ЭЛО\n"
                f"    Роль: **{role_label}** · 🦸 Герой: **{hero}**"
            )
        return "\n".join(lines), total

    lines_a, total_a = build_test_team_lines(team_a_idx, 0)
    lines_b, total_b = build_test_team_lines(team_b_idx, 1)

    # ── Embed: лобби (как выглядит сбор, когда все готовы) ──
    lobby_embed = discord.Embed(
        title="⚔️  MOBILE LEGENDS — КАСТОМНЫЙ МАТЧ 5×5",
        description=(
            "🔊 Голосовой канал: **Тестовый канал**\n"
            "👥 Игроков: **10** · ✅ Готовы: **10** / 10\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=EMBED_COLOR_READY,
    )
    lobby_lines = []
    for i in range(10):
        lobby_lines.append(f"`{i+1:>2}.` **{fake_names[i]}** — ✅ Готов")
    lobby_embed.add_field(
        name="📋 Список игроков",
        value="\n".join(lobby_lines),
        inline=False,
    )
    lobby_embed.set_footer(text="🎮 Все готовы! Формирование команд...")

    # ── Embed: результат матча ──
    header_embed = discord.Embed(
        title="🏆  МАТЧ СФОРМИРОВАН!",
        description=(
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Игроки распределены по ролям. Удачи на поле боя! 🎮"
        ),
        color=0xFEE75C,
    )

    embed_a = discord.Embed(
        title="🔵 КОМАНДА 1",
        description=f"Суммарный ЭЛО: **{total_a}**",
        color=EMBED_COLOR_TEAM_A,
    )
    embed_a.add_field(name="Состав", value=lines_a, inline=False)

    embed_b = discord.Embed(
        title="🔴 КОМАНДА 2",
        description=f"Суммарный ЭЛО: **{total_b}**",
        color=EMBED_COLOR_TEAM_B,
    )
    embed_b.add_field(name="Состав", value=lines_b, inline=False)

    # Отправляем оба этапа
    await interaction.followup.send(
        content="📋 **ТЕСТ — Так выглядит лобби когда все готовы:**",
        embed=lobby_embed,
    )
    await interaction.channel.send(
        content="⬇️ **ТЕСТ — Так выглядит результат матча:**",
        embeds=[header_embed, embed_a, embed_b],
    )


# ═══════════════════════════ RUN ═════════════════════════════════
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
