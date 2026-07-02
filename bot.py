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

# Путь к файлу с героями
HEROES_FILE = Path(__file__).parent / "heroes.txt"

# Дефолтный ЭЛО для новых игроков
DEFAULT_ELO = 300

# ─────────────────────────── БОТ ─────────────────────────────────
intents = discord.Intents.default()
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
db_pool: asyncpg.Pool | None = None

# Кэш настроек гильдии: guild_id → {voice_channel_id, ready_channel_id, voice_team1_id, voice_team2_id, host_role_id}
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
                elo      INTEGER NOT NULL DEFAULT 300
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id          BIGINT PRIMARY KEY,
                voice_channel_id  BIGINT,
                ready_channel_id  BIGINT,
                voice_team1_id    BIGINT,
                voice_team2_id    BIGINT,
                host_role_id      BIGINT
            );
            """
        )
        
        # Миграции для старой базы данных (если колонки еще не существуют)
        try:
            await conn.execute("ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS voice_team1_id BIGINT;")
            await conn.execute("ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS voice_team2_id BIGINT;")
            await conn.execute("ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS host_role_id BIGINT;")
        except Exception:
            pass

        try:
            await conn.execute("ALTER TABLE active_match_heroes ADD COLUMN IF NOT EXISTS team_name VARCHAR(50) NOT NULL DEFAULT 'Команда 1';")
            await conn.execute("ALTER TABLE active_match_heroes ADD COLUMN IF NOT EXISTS is_ranked BOOLEAN NOT NULL DEFAULT TRUE;")
        except Exception:
            pass

        # Таблица для хранения выданных игроков в матче
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_match_heroes (
                user_id   BIGINT PRIMARY KEY,
                guild_id  BIGINT NOT NULL,
                hero_name VARCHAR(100) NOT NULL,
                role      VARCHAR(50) NOT NULL,
                match_id  VARCHAR(100) NOT NULL,
                team_name VARCHAR(50) NOT NULL DEFAULT 'Команда 1',
                is_ranked BOOLEAN NOT NULL DEFAULT TRUE
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
                "voice_team1_id": row.get("voice_team1_id"),
                "voice_team2_id": row.get("voice_team2_id"),
                "host_role_id": row.get("host_role_id"),
            }


async def save_guild_setting(guild_id: int, key: str, value: int | None) -> None:
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
        guild_settings_cache[guild_id] = {
            "voice_channel_id": None, 
            "ready_channel_id": None,
            "voice_team1_id": None,
            "voice_team2_id": None,
            "host_role_id": None
        }
    guild_settings_cache[guild_id][key] = value


def get_guild_voice_channel(guild_id: int) -> int | None:
    return guild_settings_cache.get(guild_id, {}).get("voice_channel_id")


def get_guild_ready_channel(guild_id: int) -> int | None:
    return guild_settings_cache.get(guild_id, {}).get("ready_channel_id")


def get_guild_voice_team1(guild_id: int) -> int | None:
    return guild_settings_cache.get(guild_id, {}).get("voice_team1_id")


def get_guild_voice_team2(guild_id: int) -> int | None:
    return guild_settings_cache.get(guild_id, {}).get("voice_team2_id")


def get_guild_host_role(guild_id: int) -> int | None:
    return guild_settings_cache.get(guild_id, {}).get("host_role_id")


# Проверка: имеет ли пользователь права ведущего (ведущий, админ или роль ведущего)
def is_moderator(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    host_r_id = get_guild_host_role(member.guild.id)
    if host_r_id:
        return any(r.id == host_r_id for r in member.roles)
    return False


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
    by_role = {"gold": [], "exp": [], "mid": [], "jungle": [], "roam": []}
    for h in heroes:
        role = HERO_ROLES.get(h, "exp")
        if role in by_role:
            by_role[role].append(h)

    result = []
    roles_list = ["gold", "exp", "mid", "jungle", "roam"]
    for role in roles_list:
        role_heroes = by_role[role]
        if len(role_heroes) < 2:
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
EMBED_COLOR_LOBBY = 0x2B2D31
EMBED_COLOR_READY = 0x57F287
EMBED_COLOR_TEAM_A = 0x5865F2
EMBED_COLOR_TEAM_B = 0xED4245


def build_lobby_embed(
    voice_members: list[discord.Member],
    ready_ids: set[int],
    voice_channel_name: str,
    match_type: str = "Рейтинговый",
) -> discord.Embed:
    ready_count = sum(1 for m in voice_members if m.id in ready_ids)
    total = len(voice_members)

    embed = discord.Embed(
        title=f"⚔️  MOBILE LEGENDS — {match_type.upper()} МАТЧ 5×5",
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
        lines.append(f"`{idx:>2}.` {member.mention} — {status}")

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
            f"    {role_label} · 🦸 : {hero}"
        )

    embed.add_field(
        name="Состав",
        value="\n".join(lines),
        inline=False,
    )
    return embed


# ═══════════════════════════ LOBBY STATE ═════════════════════════
class LobbyState:
    __slots__ = (
        "voice_channel",
        "text_channel",
        "message",
        "ready_ids",
        "lock",
        "finished",
        "match_type",
    )

    def __init__(
        self,
        voice_channel: discord.VoiceChannel,
        text_channel: discord.TextChannel,
        message: discord.Message,
        match_type: str = "ranked",
    ):
        self.voice_channel = voice_channel
        self.text_channel = text_channel
        self.message = message
        self.ready_ids: set[int] = set()
        self.lock = asyncio.Lock()
        self.finished = False
        self.match_type = match_type


# Active lobbies
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

            match_type_labels = {
                "ranked": "Рейтинговый",
                "classic": "Классический",
                "chaos": "Хаос"
            }
            label = match_type_labels.get(lobby.match_type, "Рейтинговый")
            embed = build_lobby_embed(
                vc_members, lobby.ready_ids, lobby.voice_channel.name, label
            )

            ready_in_voice = [m for m in vc_members if m.id in lobby.ready_ids]

            if len(ready_in_voice) >= 10:
                lobby.finished = True
                self.disabled = True
                self.label = "Матч начат 🎮"
                self.style = discord.ButtonStyle.secondary
                await interaction.response.edit_message(embed=embed, view=self.view)
                
                # Создаем предпросмотр баланса команд с кнопками управления для Ведущего
                await prepare_match_preview(lobby, ready_in_voice[:10])
            else:
                await interaction.response.edit_message(embed=embed, view=self.view)


class ReadyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ReadyButton())


# ═══════════════════════ MODERATOR DECISION VIEW ═══════════════════════
class MatchPreviewView(discord.ui.View):
    """Панель управления с кнопками для Ведущего матча."""
    def __init__(self, lobby: LobbyState, players: list[discord.Member], rerolls_left: int = 1):
        super().__init__(timeout=600)  # Срок действия 10 минут
        self.lobby = lobby
        self.players = players
        self.rerolls_left = rerolls_left

        # Кнопка Рандом (перерандом состава) доступна, если остались попытки
        self.random_btn = discord.ui.Button(
            label=f"Перерандом 🎲 ({self.rerolls_left})",
            style=discord.ButtonStyle.primary,
            disabled=(self.rerolls_left <= 0)
        )
        self.random_btn.callback = self.on_reroll_click
        self.add_item(self.random_btn)

        # Кнопка подтверждения старта
        self.start_btn = discord.ui.Button(
            label="Старт ⚔️",
            style=discord.ButtonStyle.success
        )
        self.start_btn.callback = self.on_start_click
        self.add_item(self.start_btn)

    async def on_start_click(self, interaction: discord.Interaction):
        # Проверяем, является ли нажавший ведущим (модератором)
        if not is_moderator(interaction.user):
            await interaction.response.send_message(
                "❌ Только ведущий матча (или администратор) может запустить игру.",
                ephemeral=True
            )
            return

        # Отключаем кнопки
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)

        # Переходим к финальной стадии: раздача героев, сохранение в БД и перенос по каналам
        await finalize_and_launch_match(self.lobby, self.players)

    async def on_reroll_click(self, interaction: discord.Interaction):
        if not is_moderator(interaction.user):
            await interaction.response.send_message(
                "❌ Только ведущий матча (или администратор) может сделать перерандом.",
                ephemeral=True
            )
            return

        if self.rerolls_left <= 0:
            await interaction.response.send_message(
                "❌ Перерандом больше не доступен (можно использовать только 1 раз).",
                ephemeral=True
            )
            return

        # Уменьшаем попытки и готовим новый предпросмотр
        self.rerolls_left -= 1
        self.random_btn.disabled = True
        self.random_btn.label = "Перерандом использован 🎲"
        
        # Обновляем сообщение с новым разделением на команды
        await prepare_match_preview(self.lobby, self.players, self.rerolls_left, interaction)


# ═══════════════════════════ MATCH PREVIEW ═════════════════════════
async def prepare_match_preview(
    lobby: LobbyState, 
    players: list[discord.Member], 
    rerolls_left: int = 1,
    interaction_to_use: discord.Interaction | None = None
):
    """Создаёт предварительный баланс команд и отправляет его ведущему на утверждение."""
    user_ids = [p.id for p in players]
    elos = await get_elos_bulk(user_ids)
    players_with_elo = [(p, elos[p.id]) for p in players]

    # Балансируем в зависимости от режима
    if lobby.match_type in ("ranked", "classic"):
        team_a, team_b = balance_teams_snake(players_with_elo)
    else:
        # Chaos
        shuffled = players_with_elo.copy()
        random.shuffle(shuffled)
        team_a, team_b = shuffled[:5], shuffled[5:]

    total_a = sum(elo for _, elo in team_a)
    total_b = sum(elo for _, elo in team_b)

    # Строим красивый текстовый предпросмотр команд (без героев на этом этапе)
    def build_preview_lines(team: list[tuple[discord.Member, int]]) -> str:
        return "\n".join(f"`{i+1}.` {m.mention} — 🎖️ {elo} ЭЛО" for i, (m, elo) in enumerate(team))

    preview_embed = discord.Embed(
        title="⚖️  ПРЕДПРОСМОТР БАЛАНСА КОМАНД",
        description=(
            f"Режим: **{lobby.match_type.upper()}**\n"
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Ведущий, проверьте состав. Вы можете сделать **один перерандом**, если баланс вас не устраивает."
        ),
        color=0xFEE75C
    )

    embed_a = discord.Embed(
        title="🔵 КОМАНДА 1",
        description=f"Суммарный ЭЛО: **{total_a}**",
        color=EMBED_COLOR_TEAM_A
    )
    embed_a.add_field(name="Состав", value=build_preview_lines(team_a), inline=False)

    embed_b = discord.Embed(
        title="🔴 КОМАНДА 2",
        description=f"Суммарный ЭЛО: **{total_b}**",
        color=EMBED_COLOR_TEAM_B
    )
    embed_b.add_field(name="Состав", value=build_preview_lines(team_b), inline=False)

    view = MatchPreviewView(lobby, players, rerolls_left)

    if interaction_to_use:
        # Если вызвано по кнопке перерандома, редактируем старое сообщение
        await interaction_to_use.response.edit_message(
            embeds=[preview_embed, embed_a, embed_b],
            view=view
        )
    else:
        # Если это первый вывод предпросмотра
        await lobby.text_channel.send(
            embeds=[preview_embed, embed_a, embed_b],
            view=view
        )


# ═══════════════════════════ FINAL LAUNCH ═════════════════════════
async def finalize_and_launch_match(lobby: LobbyState, players: list[discord.Member]):
    """Финальная раздача героев, запись в БД и перемещение игроков."""
    heroes = load_heroes()
    
    user_ids = [p.id for p in players]
    elos = await get_elos_bulk(user_ids)
    players_with_elo = [(p, elos[p.id]) for p in players]

    # Снова делим на команды (чтобы зафиксировать текущий состав)
    if lobby.match_type in ("ranked", "classic"):
        team_a, team_b = balance_teams_snake(players_with_elo)
    else:
        # Chaos
        shuffled = players_with_elo.copy()
        random.shuffle(shuffled)
        team_a, team_b = shuffled[:5], shuffled[5:]

    roles_order = ["gold", "exp", "mid", "jungle", "roam"]
    team_a_full = []
    team_b_full = []

    if lobby.match_type in ("ranked", "classic"):
        role_picks = pick_unique_heroes_by_roles(heroes)
        role_to_heroes = {r: [] for r in roles_order}
        for h_name, role in role_picks:
            role_to_heroes[role].append(h_name)

        for i, (member, elo) in enumerate(team_a):
            role = roles_order[i]
            hero = role_to_heroes[role][0]
            team_a_full.append((member, elo, hero, role))

        for i, (member, elo) in enumerate(team_b):
            role = roles_order[i]
            hero = role_to_heroes[role][1]
            team_b_full.append((member, elo, hero, role))
    else:
        picked_heroes = pick_unique_heroes(heroes, 10)
        random.shuffle(picked_heroes)
        
        all_possible_roles = list(HERO_ROLES.values())
        if not all_possible_roles:
            all_possible_roles = roles_order

        for i, (member, elo) in enumerate(team_a):
            hero = picked_heroes[i]
            role = random.choice(all_possible_roles)
            team_a_full.append((member, elo, hero, role))

        for i, (member, elo) in enumerate(team_b):
            hero = picked_heroes[i + 5]
            role = random.choice(all_possible_roles)
            team_b_full.append((member, elo, hero, role))

    total_a = sum(elo for _, elo, _, _ in team_a_full)
    total_b = sum(elo for _, elo, _, _ in team_b_full)

    # Сохраняем в БД
    is_ranked_bool = (lobby.match_type == "ranked")
    match_id = f"{lobby.voice_channel.id}_{int(asyncio.get_event_loop().time())}"
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", lobby.voice_channel.guild.id)
        
        for member, elo, hero, role in team_a_full:
            await conn.execute(
                """
                INSERT INTO active_match_heroes (user_id, guild_id, hero_name, role, match_id, team_name, is_ranked)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                member.id, lobby.voice_channel.guild.id, hero, role, match_id, "Команда 1", is_ranked_bool
            )
        for member, elo, hero, role in team_b_full:
            await conn.execute(
                """
                INSERT INTO active_match_heroes (user_id, guild_id, hero_name, role, match_id, team_name, is_ranked)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                member.id, lobby.voice_channel.guild.id, hero, role, match_id, "Команда 2", is_ranked_bool
            )

    embed_a = build_result_embed(team_a_full, "🔵 КОМАНДА 1", EMBED_COLOR_TEAM_A, total_a)
    embed_b = build_result_embed(team_b_full, "🔴 КОМАНДА 2", EMBED_COLOR_TEAM_B, total_b)

    mentions = " ".join(m.mention for m in players)

    match_title = "🏆 РЕЙТИНГОВЫЙ МАТЧ СФОРМИРОВАН!" if lobby.match_type == "ranked" else (
        "⚔️ КЛАССИЧЕСКИЙ МАТЧ СФОРМИРОВАН!" if lobby.match_type == "classic" else "🌀 ХАОС МАТЧ СФОРМИРОВАН!"
    )
    
    # ─── АВТОПЕРЕНОС ИГРОКОВ В ГОЛОСОВЫЕ КАНАЛЫ КОМАНД ───
    move_notes = ""
    guild_id = lobby.voice_channel.guild.id
    v_team1_id = get_guild_voice_team1(guild_id)
    v_team2_id = get_guild_voice_team2(guild_id)

    chan_team1 = lobby.voice_channel.guild.get_channel(v_team1_id) if v_team1_id else None
    chan_team2 = lobby.voice_channel.guild.get_channel(v_team2_id) if v_team2_id else None

    moved_count = 0
    if chan_team1:
        for member, _, _, _ in team_a_full:
            if member.voice and member.voice.channel:
                try:
                    await member.move_to(chan_team1)
                    moved_count += 1
                except discord.Forbidden:
                    pass
    if chan_team2:
        for member, _, _, _ in team_b_full:
            if member.voice and member.voice.channel:
                try:
                    await member.move_to(chan_team2)
                    moved_count += 1
                except discord.Forbidden:
                    pass

    if moved_count > 0:
        move_notes = f"\n👉 Перемещено игроков в каналы команд: **{moved_count}** из 10."
    elif chan_team1 or chan_team2:
        move_notes = "\n⚠️ Не удалось переместить игроков (возможно, у бота нет прав «Перемещать участников»)."

    header_embed = discord.Embed(
        title=match_title,
        description=(
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Герои распределены по ролям. Удачи на поле боя! 🎮\n"
            "Если у вас нет выпавшего героя, напишите слэш-команду `/reroll`"
            f"{move_notes}\n\n"
            "**После игры администратор может выбрать победителя с помощью `/win_team`**" if lobby.match_type == "ranked"
            else f"Герои распределены. Удачи на поле боя! 🎮\nЕсли у вас нет выпавшего героя, напишите слэш-команду `/reroll`{move_notes}\n\n*(Этот матч не влияет на ЭЛО)*"
        ),
        color=0xFEE75C,
    )

    await lobby.text_channel.send(
        content=mentions,
        embeds=[header_embed, embed_a, embed_b],
    )

    active_lobbies.pop(lobby.voice_channel.id, None)


# ═══════════════════════════ EVENTS ══════════════════════════════
@bot.event
async def on_ready():
    global db_pool
    db_pool = await init_db()
    await load_all_guild_settings()
    print(f"[DB] PostgreSQL подключен, таблицы готовы.")
    print(f"[DB] Загружены настройки для {len(guild_settings_cache)} гильдий.")

    bot.add_view(ReadyView())

    try:
        guild_ids_str = os.environ.get("GUILD_ID", "")
        guild_ids = [int(x.strip()) for x in guild_ids_str.split(",") if x.strip()]
        
        if guild_ids:
            for g_id in guild_ids:
                try:
                    guild_obj = discord.Object(id=g_id)
                    bot.tree.clear_commands(guild=guild_obj)
                    bot.tree.copy_global_to(guild=guild_obj)
                    synced = await bot.tree.sync(guild=guild_obj)
                    print(f"[BOT] Синхронизировано {len(synced)} команд для гильдии {g_id}.")
                except Exception as e_guild:
                    print(f"[BOT] Ошибка синхронизации для гильдии {g_id}: {e_guild}")
        else:
            synced = await bot.tree.sync()
            print(f"[BOT] Синхронизировано {len(synced)} глобальных команд.")
    except Exception as e:
        print(f"[BOT] Ошибка синхронизации команд: {e}")

    print(f"[BOT] {bot.user} запущен и готов к работе!")


@bot.event
# ═══════════════════════ AUTO WIN DECISION VIEW ═══════════════════════
class AutoWinPollView(discord.ui.View):
    """Интерактивные кнопки выбора победителя после возвращения игроков."""
    def __init__(self, guild_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id

    async def check_moderator(self, interaction: discord.Interaction) -> bool:
        if not is_moderator(interaction.user):
            await interaction.response.send_message(
                "❌ Только ведущий матча (или администратор) может выбрать победителя.",
                ephemeral=True
            )
            return False
        return True

    async def apply_win(self, interaction: discord.Interaction, winner_team_val: str):
        if not await self.check_moderator(interaction):
            return

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)

        async with db_pool.acquire() as conn:
            players = await conn.fetch(
                """
                SELECT user_id, team_name, is_ranked 
                FROM active_match_heroes 
                WHERE guild_id = $1
                """,
                self.guild_id
            )

            if not players:
                await interaction.followup.send("❌ Данные об активном матче не найдены или уже были удалены.", ephemeral=True)
                return

            is_ranked_match = players[0]["is_ranked"]
            winners_ids = [p["user_id"] for p in players if p["team_name"] == winner_team_val]

            if not winners_ids:
                await interaction.followup.send("❌ Не удалось найти игроков выбранной команды.", ephemeral=True)
                return

            if is_ranked_match:
                for uid in winners_ids:
                    await conn.execute(
                        """
                        INSERT INTO players (user_id, elo) VALUES ($1, $2)
                        ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 50
                        """,
                        uid, DEFAULT_ELO + 50
                    )
                
                await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", self.guild_id)
                mentions = " ".join(f"<@{uid}>" for uid in winners_ids)
                await interaction.followup.send(
                    f"🎉 **Команда {winner_team_val} побеждает в рейтинговом матче!**\n"
                    f"Все участники команды получают **+50 ЭЛО**:\n{mentions}"
                )
            else:
                await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", self.guild_id)
                await interaction.followup.send(
                    f"🎉 **Команда {winner_team_val} побеждает!**\n*(Этот матч был не рейтинговым, ЭЛО начислено не было)*"
                )

    @discord.ui.button(label="🔵 Команда 1", style=discord.ButtonStyle.primary)
    async def team1_win(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.apply_win(interaction, "Команда 1")

    @discord.ui.button(label="🔴 Команда 2", style=discord.ButtonStyle.danger)
    async def team2_win(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.apply_win(interaction, "Команда 2")


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
):
    guild_id = member.guild.id
    configured_vc = get_guild_voice_channel(guild_id)
    ready_ch_id = get_guild_ready_channel(guild_id)

    # 1. Обновление обычного лобби сбора
    channels_to_check: set[int] = set()
    if before.channel:
        channels_to_check.add(before.channel.id)
    if after.channel:
        channels_to_check.add(after.channel.id)

    for vc_id in channels_to_check:
        if configured_vc and vc_id != configured_vc:
            continue

        lobby = active_lobbies.get(vc_id)
        if lobby is None or lobby.finished:
            continue

        async with lobby.lock:
            if lobby.finished:
                continue

            vc_members = lobby.voice_channel.members
            current_ids = {m.id for m in vc_members}
            lobby.ready_ids &= current_ids

            match_type_labels = {
                "ranked": "Рейтинговый",
                "classic": "Классический",
                "chaos": "Хаос"
            }
            label = match_type_labels.get(lobby.match_type, "Рейтинговый")

            embed = build_lobby_embed(
                vc_members, lobby.ready_ids, lobby.voice_channel.name, label
            )
            try:
                await lobby.message.edit(embed=embed)
            except discord.NotFound:
                active_lobbies.pop(vc_id, None)

    # 2. Логика автоопределения окончания матча при возвращении 5+ игроков в войс сбора
    # Срабатывает только если пользователь перешел в нужный голосовой канал
    if after.channel and configured_vc and after.channel.id == configured_vc:
        async with db_pool.acquire() as conn:
            # Получаем игроков активного матча
            active_players = await conn.fetch(
                "SELECT user_id FROM active_match_heroes WHERE guild_id = $1",
                guild_id
            )

            if active_players and len(active_players) >= 10:
                active_ids = {p["user_id"] for p in active_players}
                
                # Считаем, сколько участников активного матча сейчас находятся в голосовом канале сбора
                vc_members = after.channel.members
                returned_count = sum(1 for m in vc_members if m.id in active_ids)

                # Если вернулось 5 и более игроков
                if returned_count >= 5:
                    # Отправляем опрос в текстовый канал сбора (ready_channel)
                    text_channel = member.guild.get_channel(ready_ch_id) if ready_ch_id else None
                    if text_channel:
                        # Проверяем, не отправляли ли мы уже такой опрос недавно, чтобы не спамить
                        # Для этого проверяем историю сообщений
                        already_asked = False
                        async for msg in text_channel.history(limit=5):
                            if "Какая команда выиграла?" in msg.content and not msg.author.bot:
                                continue
                            if "Какая команда выиграла?" in msg.content and msg.author == bot.user:
                                # Если на кнопках еще не кликнули, значит опрос висит
                                if any(not btn.disabled for btn in msg.components[0].children if isinstance(btn, discord.ui.Button)):
                                    already_asked = True
                                    break

                        if not already_asked:
                            view = AutoWinPollView(guild_id)
                            await text_channel.send(
                                content="🎮 **Большинство игроков вернулись в лобби сбора!**\nВедущий, выберите, какая команда выиграла матч:",
                                view=view
                            )


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


# ───────────── /set_voice_team1 ─────────────
@bot.tree.command(
    name="set_voice_team1",
    description="[Админ] Назначить голосовой канал для Команды 1"
)
@app_commands.describe(channel="Голосовой канал для первой команды")
@app_commands.default_permissions(administrator=True)
async def cmd_set_voice_team1(interaction: discord.Interaction, channel: discord.VoiceChannel):
    await save_guild_setting(interaction.guild_id, "voice_team1_id", channel.id)
    await interaction.response.send_message(
        f"✅ Голосовой канал для **Команды 1** установлен: **{channel.name}** (`{channel.id}`)",
        ephemeral=True
    )


# ───────────── /set_voice_team2 ─────────────
@bot.tree.command(
    name="set_voice_team2",
    description="[Админ] Назначить голосовой канал для Команды 2"
)
@app_commands.describe(channel="Голосовой канал для второй команды")
@app_commands.default_permissions(administrator=True)
async def cmd_set_voice_team2(interaction: discord.Interaction, channel: discord.VoiceChannel):
    await save_guild_setting(interaction.guild_id, "voice_team2_id", channel.id)
    await interaction.response.send_message(
        f"✅ Голосовой канал для **Команды 2** установлен: **{channel.name}** (`{channel.id}`)",
        ephemeral=True
    )


# ───────────── /set_host_role ─────────────
@bot.tree.command(
    name="set_host_role",
    description="[Админ] Назначить роль Ведущего матчей (может делать перерандом и запускать матчи)"
)
@app_commands.describe(role="Роль, члены которой будут считаться Ведущими")
@app_commands.default_permissions(administrator=True)
async def cmd_set_host_role(interaction: discord.Interaction, role: discord.Role):
    await save_guild_setting(interaction.guild_id, "host_role_id", role.id)
    await interaction.response.send_message(
        f"✅ Роль ведущего матчей успешно установлена: **{role.name}** (`{role.id}`)",
        ephemeral=True
    )


# ───────────── /settings ──────────────
@bot.tree.command(
    name="settings",
    description="[Админ] Показать текущие настройки бота на сервере",
)
@app_commands.default_permissions(administrator=True)
async def cmd_settings(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    vc_id = get_guild_voice_channel(guild_id)
    rc_id = get_guild_ready_channel(guild_id)
    vt1_id = get_guild_voice_team1(guild_id)
    vt2_id = get_guild_voice_team2(guild_id)
    host_r_id = get_guild_host_role(guild_id)

    vc_text = f"<#{vc_id}>" if vc_id else "❌ _Не задан_ — используй `/set_voice`"
    rc_text = f"<#{rc_id}>" if rc_id else "❌ _Не задан_ — используй `/set_ready`"
    vt1_text = f"<#{vt1_id}>" if vt1_id else "❌ _Не задан_ — используй `/set_voice_team1`"
    vt2_text = f"<#{vt2_id}>" if vt2_id else "❌ _Не задан_ — используй `/set_voice_team2`"
    host_text = f"<@&{host_r_id}>" if host_r_id else "❌ _Не задана_ — используй `/set_host_role`"

    embed = discord.Embed(
        title="⚙️  Настройки бота",
        color=0x5865F2,
    )
    embed.add_field(name="🔊 Голосовой канал сбора", value=vc_text, inline=False)
    embed.add_field(name="📝 Текстовый канал (лобби)", value=rc_text, inline=False)
    embed.add_field(name="🔵 Войс для Команды 1", value=vt1_text, inline=True)
    embed.add_field(name="🔴 Войс для Команды 2", value=vt2_text, inline=True)
    embed.add_field(name="🎖️ Роль Ведущего матчей", value=host_text, inline=False)
    embed.set_footer(text="Используйте команды настройки для изменения каналов")

    await interaction.response.send_message(embed=embed, ephemeral=True)


# Общий хелпер для запуска сборов
async def run_lobby_init(interaction: discord.Interaction, match_type: str, label: str):
    guild_id = interaction.guild_id
    configured_vc = get_guild_voice_channel(guild_id)
    configured_rc = get_guild_ready_channel(guild_id)

    if configured_rc and interaction.channel_id != configured_rc:
        await interaction.response.send_message(
            f"❌ Эту команду можно использовать только в <#{configured_rc}>.",
            ephemeral=True,
        )
        return

    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "❌ Вы должны находиться в голосовом канале, чтобы начать сбор.",
            ephemeral=True,
        )
        return

    voice_channel = interaction.user.voice.channel

    if configured_vc and voice_channel.id != configured_vc:
        await interaction.response.send_message(
            f"❌ Сбор можно начать только из канала <#{configured_vc}>.",
            ephemeral=True,
        )
        return

    if voice_channel.id in active_lobbies:
        await interaction.response.send_message(
            "❌ Для этого голосового канала уже идёт сбор!",
            ephemeral=True,
        )
        return

    vc_members = voice_channel.members
    ready_ids: set[int] = set()

    embed = build_lobby_embed(vc_members, ready_ids, voice_channel.name, label)
    view = ReadyView()

    if configured_rc and interaction.channel_id == configured_rc:
        await interaction.response.send_message(embed=embed, view=view)
        msg = await interaction.original_response()
        lobby = LobbyState(voice_channel, interaction.channel, msg, match_type)
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
        lobby = LobbyState(voice_channel, target_ch, msg, match_type)
    else:
        await interaction.response.send_message(embed=embed, view=view)
        msg = await interaction.original_response()
        lobby = LobbyState(voice_channel, interaction.channel, msg, match_type)

    active_lobbies[voice_channel.id] = lobby


# ───────────── /start_ranked ─────────────
@bot.tree.command(
    name="start_ranked",
    description="Начать сбор на рейтинговый матч 5×5 (с балансом по ЭЛО и распределением по лайнам)"
)
async def cmd_start_ranked(interaction: discord.Interaction):
    await run_lobby_init(interaction, "ranked", "Рейтинговый")


# ───────────── /start_classic ────────────
@bot.tree.command(
    name="start_classic",
    description="Начать сбор на классический матч 5×5 (баланс по ЭЛО и лайнам, без начисления рейтинга)"
)
async def cmd_start_classic(interaction: discord.Interaction):
    await run_lobby_init(interaction, "classic", "Классический")


# ───────────── /start_dalbaeb ────────────
@bot.tree.command(
    name="start_dalbaeb",
    description="Начать сбор на хаос-матч 5×5 (полный хаос: случайные команды, рандомные лайны, без влияния на ЭЛО)"
)
async def cmd_start_dalbaeb(interaction: discord.Interaction):
    await run_lobby_init(interaction, "chaos", "Хаос")


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
        row = await conn.fetchrow(
            """
            SELECT hero_name, role, match_id, is_ranked 
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

        busy_rows = await conn.fetch(
            "SELECT hero_name FROM active_match_heroes WHERE match_id = $1",
            match_id
        )
        busy_heroes = {r["hero_name"] for r in busy_rows}

        heroes = load_heroes()
        role_heroes = [
            h for h in heroes 
            if HERO_ROLES.get(h, "exp") == role and h not in busy_heroes and h != current_hero
        ]

        if not role_heroes:
            role_heroes = [h for h in heroes if h not in busy_heroes and h != current_hero]

        if not role_heroes:
            await interaction.response.send_message(
                "❌ К сожалению, нет доступных героев для замены.",
                ephemeral=True
            )
            return

        new_hero = random.choice(role_heroes)

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


# ───────────── /win_team ──────────────
@bot.tree.command(
    name="win_team",
    description="[Админ] Начислить ЭЛО победившей команде (+50 ЭЛО каждому, только для Ranked)"
)
@app_commands.describe(team="Выберите победившую команду")
@app_commands.choices(team=[
    app_commands.Choice(name="🔵 КОМАНДА 1", value="Команда 1"),
    app_commands.Choice(name="🔴 КОМАНДА 2", value="Команда 2")
])
@app_commands.default_permissions(administrator=True)
async def cmd_win_team(interaction: discord.Interaction, team: app_commands.Choice[str]):
    guild_id = interaction.guild_id
    winner_team_val = team.value

    async with db_pool.acquire() as conn:
        players = await conn.fetch(
            """
            SELECT user_id, team_name, is_ranked 
            FROM active_match_heroes 
            WHERE guild_id = $1
            """,
            guild_id
        )

        if not players or len(players) < 10:
            await interaction.response.send_message(
                "❌ На сервере нет активных матчей или состав неполный.",
                ephemeral=True
            )
            return

        is_ranked_match = players[0]["is_ranked"]
        winners_ids = [p["user_id"] for p in players if p["team_name"] == winner_team_val]

        if not winners_ids:
            await interaction.response.send_message(
                "❌ Не удалось определить игроков победившей команды.",
                ephemeral=True
            )
            return

        if is_ranked_match:
            for uid in winners_ids:
                await conn.execute(
                    """
                    INSERT INTO players (user_id, elo) VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 50
                    """,
                    uid, DEFAULT_ELO + 50
                )
            
            await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", guild_id)

            mentions = " ".join(f"<@{uid}>" for uid in winners_ids)
            await interaction.response.send_message(
                f"🎉 **Команда {team.name} побеждает в рейтинговом матче!**\n"
                f"Все участники команды получают **+50 ЭЛО**:\n{mentions}"
            )
        else:
            await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", guild_id)
            await interaction.response.send_message(
                f"🎉 **Команда {team.name} побеждает!**\n*(Этот матч был не рейтинговым, ЭЛО начислено не было)*"
            )


# ───────────── /mvp_win ──────────────
@bot.tree.command(
    name="mvp_win",
    description="[Админ] Начислить MVP победившей команды (+75 ЭЛО)"
)
@app_commands.describe(player="Игрок, получивший MVP в победившей команде")
@app_commands.default_permissions(administrator=True)
async def cmd_mvp_win(interaction: discord.Interaction, player: discord.Member):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (user_id, elo) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 75
            """,
            player.id, DEFAULT_ELO + 75
        )
    await interaction.response.send_message(
        f"🌟 **MVP победителей!** Игрок {player.mention} получает **+75 ЭЛО**!"
    )


# ───────────── /mvp_loss ─────────────
@bot.tree.command(
    name="mvp_loss",
    description="[Админ] Начислить MVP проигравшей команды (+75 ЭЛО)"
)
@app_commands.describe(player="Игрок, получивший MVP в проигравшей команде")
@app_commands.default_permissions(administrator=True)
async def cmd_mvp_loss(interaction: discord.Interaction, player: discord.Member):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (user_id, elo) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 75
            """,
            player.id, DEFAULT_ELO + 75
        )
    await interaction.response.send_message(
        f"🌟 **MVP проигравших!** Игрок {player.mention} получает **+75 ЭЛО**!"
    )


# ───────────── /random ────────────────
@bot.tree.command(
    name="random",
    description="Выбрать случайного героя (можно указать роль)"
)
@app_commands.describe(role="Роль: gold, exp, mid, jungle, roam")
@app_commands.choices(role=[
    app_commands.Choice(name="🪙 Gold (Стрелок)", value="gold"),
    app_commands.Choice(name="🛡️ Exp (Боец)", value="exp"),
    app_commands.Choice(name="🔮 Mid (Маг)", value="mid"),
    app_commands.Choice(name="⚔️ Jungle (Лесник/Убийца)", value="jungle"),
    app_commands.Choice(name="👣 Roam (Танк/Поддержка)", value="roam")
])
async def cmd_random(interaction: discord.Interaction, role: app_commands.Choice[str] = None):
    heroes = load_heroes()
    
    if role:
        role_val = role.value
        role_heroes = [h for h in heroes if HERO_ROLES.get(h, "exp") == role_val]
        if not role_heroes:
            await interaction.response.send_message(
                f"❌ Героев для роли {role.name} не найдено.",
                ephemeral=True
            )
            return
        chosen_hero = random.choice(role_heroes)
        role_label = role.name
        await interaction.response.send_message(
            f"🎲 {interaction.user.mention} крутит рандомайзер на роль **{role_label}** и выбивает: **{chosen_hero}**!"
        )
    else:
        chosen_hero = random.choice(heroes)
        detected_role = HERO_ROLES.get(chosen_hero, "exp")
        role_labels = {
            "gold": "🪙 Gold",
            "exp": "🛡️ Exp",
            "mid": "🔮 Mid",
            "jungle": "⚔️ Jungle",
            "roam": "👣 Roam"
        }
        role_text = role_labels.get(detected_role, detected_role.capitalize())
        await interaction.response.send_message(
            f"🎲 {interaction.user.mention} крутит рандомайзер и выбивает случайного героя: **{chosen_hero}** ({role_text})!"
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
                f"    {role_label} · 🦸 : {hero}"
            )
        return "\n".join(lines), total

    lines_a, total_a = build_test_team_lines(team_a_idx, 0)
    lines_b, total_b = build_test_team_lines(team_b_idx, 1)

    # ── Embed: лобби (как выглядит сбор, когда все готовы) ──
    lobby_embed = discord.Embed(
        title="⚔️  MOBILE LEGENDS — РЕЙТИНГОВЫЙ МАТЧ 5×5",
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
        title="🏆 РЕЙТИНГОВЫЙ МАТЧ СФОРМИРОВАН!",
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
