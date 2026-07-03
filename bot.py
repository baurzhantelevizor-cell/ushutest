"""
Discord-бот для кастомных матчей 5x5 в Mobile Legends.
Хостинг: Railway + PostgreSQL.
Настройки хранятся в .env (локально) или в Variables (Railway).
"""

import os
import random
import asyncio
import io
from pathlib import Path
from difflib import SequenceMatcher

import discord
from discord import app_commands
from discord.ext import commands
import asyncpg
from dotenv import load_dotenv
import aiohttp
import numpy as np
from PIL import Image

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

# OCR-ридер (ленивая инициализация при первом вызове /scan)
ocr_reader = None

def get_ocr_reader():
    """Ленивая инициализация EasyOCR ридера."""
    global ocr_reader
    if ocr_reader is None:
        import easyocr
        ocr_reader = easyocr.Reader(["ru", "en"], gpu=False)
    return ocr_reader


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
            await conn.execute("ALTER TABLE active_match_heroes ADD COLUMN IF NOT EXISTS rerolls INTEGER NOT NULL DEFAULT 0;")
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
                is_ranked BOOLEAN NOT NULL DEFAULT TRUE,
                rerolls   INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Таблица для накопления общей статистики игроков
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_stats (
                user_id    BIGINT PRIMARY KEY,
                matches    INTEGER NOT NULL DEFAULT 0,
                wins       INTEGER NOT NULL DEFAULT 0,
                losses     INTEGER NOT NULL DEFAULT 0,
                mvps       INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Таблица для записи истории матчей (для вывода последних 5 героев)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS match_history (
                id          SERIAL PRIMARY KEY,
                user_id     BIGINT NOT NULL,
                match_id    VARCHAR(100) NOT NULL,
                hero_name   VARCHAR(100) NOT NULL,
                role        VARCHAR(50) NOT NULL,
                is_win      BOOLEAN NOT NULL,
                match_date  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # Таблица для связки Discord-аккаунта с игровым ником MLBB
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS linked_accounts (
                user_id       BIGINT PRIMARY KEY,
                game_nickname VARCHAR(100) NOT NULL,
                game_id       VARCHAR(50),
                linked_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    def __init__(self, lobby: LobbyState, players: list[discord.Member], team_a: list[tuple[discord.Member, int]], team_b: list[tuple[discord.Member, int]], rerolls_left: int = 1):
        super().__init__(timeout=600)  # Срок действия 10 минут
        self.lobby = lobby
        self.players = players
        self.team_a = team_a
        self.team_b = team_b
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
        await finalize_and_launch_match(self.lobby, self.players, self.team_a, self.team_b)

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

    view = MatchPreviewView(lobby, players, team_a, team_b, rerolls_left)

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
async def finalize_and_launch_match(lobby: LobbyState, players: list[discord.Member], team_a: list[tuple[discord.Member, int]], team_b: list[tuple[discord.Member, int]]):
    """Финальная раздача героев, запись в БД и перемещение игроков."""
    heroes = load_heroes()
    
    roles_order = ["gold", "exp", "mid", "jungle", "roam"]
    team_a_full = []
    team_b_full = []

    if lobby.match_type == "ranked":
        # В Ranked режиме НЕТ раздачи героев и лайнов. Просто сохраняем игроков в команды.
        for member, elo in team_a:
            team_a_full.append((member, elo, "Свои герои", "Любая"))
        for member, elo in team_b:
            team_b_full.append((member, elo, "Свои герои", "Любая"))
    elif lobby.match_type == "classic":
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
        # Chaos
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

    mentions = " ".join(m.mention for m in players)

    match_title = "🏆 РЕЙТИНГОВЫЙ МАТЧ СФОРМИРОВАН!" if lobby.match_type == "ranked" else (
        "⚔️ КЛАССИЧЕСКИЙ МАТЧ СФОРМИРОВАН!" if lobby.match_type == "classic" else "🌀 ХАОС МАТЧ СФОРМИРОВАН!"
    )
    
    # ─── АВТОПЕРЕНОС ИГРОКОВ В ГОЛОСОВЫЕ КАНАЛЫ КОМАНД ───
    move_notes = ""
    if lobby.match_type in ("ranked", "classic"):
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

    # ─── СОЗДАНИЕ ЕДИНОГО EMBED'А РЕЗУЛЬТАТА ───
    role_emojis = {
        "gold": "🪙 Gold",
        "exp": "🛡️ Exp",
        "mid": "🔮 Mid",
        "jungle": "⚔️ Jungle",
        "roam": "👣 Roam"
    }

    def build_embed_team_section(team_full_list: list) -> str:
        sec_lines = []
        for idx, (m, elo, hero, role) in enumerate(team_full_list, start=1):
            if lobby.match_type == "ranked":
                sec_lines.append(f"`{idx}.` {m.mention} — 🎖️ {elo} ЭЛО")
            else:
                role_label = role_emojis.get(role, role.capitalize())
                sec_lines.append(f"`{idx}.` {m.mention}\n    {role_label} · 🦸 : **{hero}**")
        return "\n".join(sec_lines)

    if lobby.match_type == "ranked":
        desc = (
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Игроки распределены на две сбалансированные команды. Удачи в игре! 🎮\n"
            f"{move_notes}\n\n"
            "**После игры администратор может выбрать победителя с помощью `/win_team`**"
        )
    else:
        desc = (
            f"Разница ЭЛО команд: **{abs(total_a - total_b)}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Герои распределены по ролям. Удачи на поле боя! 🎮\n"
            "Если у вас нет выпавшего героя, нажмите кнопку **Реролл 🎲** ниже"
            f"{move_notes}\n\n"
            "*(Этот матч не влияет на ЭЛО)*"
        )

    result_embed = discord.Embed(
        title=match_title,
        description=desc,
        color=0xFEE75C,
    )
    result_embed.add_field(
        name=f"🔵 КОМАНДА 1 (Суммарный ЭЛО: {total_a})",
        value=build_embed_team_section(team_a_full),
        inline=False
    )
    result_embed.add_field(
        name=f"🔴 КОМАНДА 2 (Суммарный ЭЛО: {total_b})",
        value=build_embed_team_section(team_b_full),
        inline=False
    )

    if lobby.match_type == "ranked":
        await lobby.text_channel.send(
            content=mentions,
            embed=result_embed
        )
    else:
        view = PlayerRerollView()
        await lobby.text_channel.send(
            content=mentions,
            embed=result_embed,
            view=view
        )

    active_lobbies.pop(lobby.voice_channel.id, None)


# ═══════════════════════ PLAYER REROLL BUTTON VIEW ═══════════════════════
class PlayerRerollButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Реролл 🎲",
            style=discord.ButtonStyle.secondary,
            custom_id="player_hero_reroll_button"
        )

    async def callback(self, interaction: discord.Interaction):
        user = interaction.user
        guild_id = interaction.guild_id

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT hero_name, role, match_id, is_ranked, rerolls 
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
            current_rerolls = row["rerolls"]

            if current_rerolls >= 2:
                await interaction.response.send_message(
                    "❌ Вы уже исчерпали лимит замен героев (максимум 2 замены на матч).",
                    ephemeral=True
                )
                return

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
            new_reroll_count = current_rerolls + 1

            await conn.execute(
                """
                UPDATE active_match_heroes 
                SET hero_name = $1, rerolls = $2
                WHERE user_id = $3 AND guild_id = $4
                """,
                new_hero, new_reroll_count, user.id, guild_id
            )

            # Получаем обновленный состав команд для обновления исходного сообщения
            all_match_players = await conn.fetch(
                """
                SELECT user_id, hero_name, role, team_name 
                FROM active_match_heroes 
                WHERE match_id = $1
                """,
                match_id
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
            f"Роль: **{role_label}** · Попытка: **{new_reroll_count}/2**\n"
            f"Старый герой: ~~{current_hero}~~\n"
            f"Новый герой: **{new_hero}**",
            ephemeral=True
        )

        # Перестраиваем Embed в исходном сообщении, чтобы показать измененного героя
        # Нам нужно достать старый Embed и обновить поля
        message = interaction.message
        if message and message.embeds:
            old_embed = message.embeds[0]
            
            # Разделяем игроков по командам
            team1_lines = []
            team2_lines = []
            idx1, idx2 = 1, 1

            for p in all_match_players:
                p_id = p["user_id"]
                p_hero = p["hero_name"]
                p_role = p["role"]
                p_team = p["team_name"]
                r_label = role_emojis.get(p_role, p_role.capitalize())

                if p_team == "Команда 1":
                    team1_lines.append(f"`{idx1}.` <@{p_id}>\n    {r_label} · 🦸 : **{p_hero}**")
                    idx1 += 1
                else:
                    team2_lines.append(f"`{idx2}.` <@{p_id}>\n    {r_label} · 🦸 : **{p_hero}**")
                    idx2 += 1

            new_embed = discord.Embed(
                title=old_embed.title,
                description=old_embed.description,
                color=old_embed.color
            )
            
            # Ищем названия полей с суммарным ELO
            t1_name = old_embed.fields[0].name if len(old_embed.fields) > 0 else "🔵 КОМАНДА 1"
            t2_name = old_embed.fields[1].name if len(old_embed.fields) > 1 else "🔴 КОМАНДА 2"

            new_embed.add_field(name=t1_name, value="\n".join(team1_lines) if team1_lines else "_Пусто_", inline=False)
            new_embed.add_field(name=t2_name, value="\n".join(team2_lines) if team2_lines else "_Пусто_", inline=False)

            try:
                await message.edit(embed=new_embed)
            except discord.HTTPException:
                pass


class PlayerRerollView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # Кнопка работает без таймаута
        self.add_item(PlayerRerollButton())


# ═══════════════════════════ EVENTS ══════════════════════════════
@bot.event
async def on_ready():
    global db_pool
    db_pool = await init_db()
    await load_all_guild_settings()
    print(f"[DB] PostgreSQL подключен, таблицы готовы.")
    print(f"[DB] Загружены настройки для {len(guild_settings_cache)} гильдий.")

    bot.add_view(ReadyView())
    bot.add_view(PlayerRerollView())

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
                SELECT user_id, team_name, is_ranked, hero_name, role, match_id 
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
                winners = [p for p in players if p["team_name"] == winner_team_val]
                losers = [p for p in players if p["team_name"] != winner_team_val]
                
                winners_ids = [p["user_id"] for p in winners]
                losers_ids = [p["user_id"] for p in losers]

                # Победителям +50 ЭЛО, +1 к матчам, +1 к победам
                for p in winners:
                    uid = p["user_id"]
                    hero = p["hero_name"]
                    role = p["role"]
                    m_id = p["match_id"]
                    await conn.execute(
                        """
                        INSERT INTO players (user_id, elo) VALUES ($1, $2)
                        ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 50
                        """,
                        uid, DEFAULT_ELO + 50
                    )
                    await conn.execute(
                        """
                        INSERT INTO player_stats (user_id, matches, wins, losses, mvps) VALUES ($1, 1, 1, 0, 0)
                        ON CONFLICT (user_id) DO UPDATE SET matches = player_stats.matches + 1, wins = player_stats.wins + 1
                        """,
                        uid
                    )
                    await conn.execute(
                        """
                        INSERT INTO match_history (user_id, match_id, hero_name, role, is_win)
                        VALUES ($1, $2, $3, $4, TRUE)
                        """,
                        uid, m_id, hero, role
                    )
                
                # Проигравшим -50 ЭЛО (но не ниже 0), +1 к матчам, +1 к поражениям
                for p in losers:
                    uid = p["user_id"]
                    hero = p["hero_name"]
                    role = p["role"]
                    m_id = p["match_id"]
                    await conn.execute(
                        """
                        INSERT INTO players (user_id, elo) VALUES ($1, $2)
                        ON CONFLICT (user_id) DO UPDATE SET elo = GREATEST(players.elo - 50, 0)
                        """,
                        uid, DEFAULT_ELO - 50
                    )
                    await conn.execute(
                        """
                        INSERT INTO player_stats (user_id, matches, wins, losses, mvps) VALUES ($1, 1, 0, 1, 0)
                        ON CONFLICT (user_id) DO UPDATE SET matches = player_stats.matches + 1, losses = player_stats.losses + 1
                        """,
                        uid
                    )
                    await conn.execute(
                        """
                        INSERT INTO match_history (user_id, match_id, hero_name, role, is_win)
                        VALUES ($1, $2, $3, $4, FALSE)
                        """,
                        uid, m_id, hero, role
                    )

                await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", self.guild_id)
                mentions_win = " ".join(f"<@{uid}>" for uid in winners_ids)
                mentions_loss = " ".join(f"<@{uid}>" for uid in losers_ids)
                await interaction.followup.send(
                    f"🎉 **Команда {winner_team_val} побеждает в рейтинговом матче!**\n"
                    f"🟢 Победители получает **+50 ЭЛО**:\n{mentions_win}\n"
                    f"🔴 Проигравшие теряют **-50 ЭЛО**:\n{mentions_loss}"
                )
            else:
                await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", self.guild_id)
                await interaction.followup.send(
                    f"🎉 **Команда {winner_team_val} побеждает!**\n*(Этот матч был не рейтинговым, ЭЛО изменено не было)*"
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
                "SELECT user_id, is_ranked FROM active_match_heroes WHERE guild_id = $1",
                guild_id
            )

            if active_players and len(active_players) >= 10:
                # В режиме хаос (is_ranked = False) опрос автоматически не шлём
                is_ranked_match = active_players[0]["is_ranked"]
                if not is_ranked_match:
                    return

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


# ───────────── /top ───────────────────
@bot.tree.command(name="top", description="Посмотреть топ-10 игроков сервера по ЭЛО")
async def cmd_top(interaction: discord.Interaction):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, elo FROM players ORDER BY elo DESC LIMIT 10"
        )

    if not rows:
        await interaction.response.send_message(
            "❌ В базе данных пока нет игроков с рейтингом.", ephemeral=True
        )
        return

    embed = discord.Embed(
        title="🏆 ТОП-10 ИГРОКОВ ПО ЭЛО",
        description="Рейтинг сильнейших игроков сервера:",
        color=0xFEE75C
    )

    medal_emojis = {1: "🥇", 2: "🥈", 3: "🥉"}

    leaderboard_lines = []
    for idx, row in enumerate(rows, start=1):
        u_id = row["user_id"]
        elo_val = row["elo"]
        
        # Получаем пользователя в кэше сервера или через API
        member = interaction.guild.get_member(u_id)
        name = member.mention if member else f"Участник <@{u_id}>"
        
        medal = medal_emojis.get(idx, f"`{idx:>2}.` ")
        leaderboard_lines.append(f"{medal} {name} — **{elo_val}** ЭЛО")

    embed.add_field(
        name="📋 Таблица лидеров",
        value="\n".join(leaderboard_lines),
        inline=False
    )
    embed.set_footer(text="Играйте рейтинговые матчи, чтобы подняться выше!")

    await interaction.response.send_message(embed=embed)


# ───────────── /profile ─────────────────
@bot.tree.command(name="profile", description="Посмотреть профиль, ЭЛО и подробную статистику игрока")
@app_commands.describe(player="Игрок, профиль которого нужно посмотреть (по умолчанию — ваш собственный)")
async def cmd_profile(interaction: discord.Interaction, player: discord.Member | None = None):
    target = player if player else interaction.user
    
    # 1. Получаем текущее ЭЛО
    elo = await get_elo(target.id)

    # Определяем лигу по ЭЛО
    if elo < 200:
        rank_name = "🟫 Воин"
    elif elo < 400:
        rank_name = "🥈 Элита"
    elif elo < 600:
        rank_name = "🥇 Мастер"
    elif elo < 800:
        rank_name = "🛡️ Грандмастер"
    elif elo < 1000:
        rank_name = "🔮 Эпик"
    elif elo < 1200:
        rank_name = "👑 Легенда"
    else:
        rank_name = "🌟 Мифическая Слава"

    # 2. Читаем общую статистику из player_stats
    async with db_pool.acquire() as conn:
        stats_row = await conn.fetchrow(
            "SELECT matches, wins, losses, mvps FROM player_stats WHERE user_id = $1",
            target.id
        )
        
        # 3. Читаем историю последних 5 героев
        history_rows = await conn.fetch(
            """
            SELECT hero_name, role, is_win 
            FROM match_history 
            WHERE user_id = $1 
            ORDER BY match_date DESC 
            LIMIT 5
            """,
            target.id
        )

        # 4. Читаем привязанный игровой ник
        linked_row = await conn.fetchrow(
            "SELECT game_nickname, game_id FROM linked_accounts WHERE user_id = $1",
            target.id
        )

    # Дефолтные значения
    matches = 0
    wins = 0
    losses = 0
    mvps = 0
    winrate = 0.0

    if stats_row:
        matches = stats_row["matches"]
        wins = stats_row["wins"]
        losses = stats_row["losses"]
        mvps = stats_row["mvps"]
        if matches > 0:
            winrate = (wins / matches) * 100

    role_emojis = {
        "gold": "🪙 Gold",
        "exp": "🛡️ Exp",
        "mid": "🔮 Mid",
        "jungle": "⚔️ Jungle",
        "roam": "👣 Roam"
    }

    # Строим список последних игр
    history_lines = []
    if history_rows:
        for r in history_rows:
            h_name = r["hero_name"]
            role_val = r["role"]
            is_win = r["is_win"]
            role_lbl = role_emojis.get(role_val, role_val.capitalize())
            status_emoji = "🟢 Win" if is_win else "🔴 Loss"
            
            history_lines.append(f"{status_emoji} · **{h_name}** ({role_lbl})")
    else:
        history_lines.append("_Игр пока нет_")

    embed = discord.Embed(
        title=f"👤 ПРОФИЛЬ ИГРОКА — {target.display_name}",
        color=0x5865F2
    )
    if target.avatar:
        embed.set_thumbnail(url=target.avatar.url)

    # Привязанный ник MLBB
    if linked_row:
        game_nick = linked_row["game_nickname"]
        game_id_val = linked_row["game_id"]
        link_text = f"**{game_nick}**"
        if game_id_val:
            link_text += f" (ID: `{game_id_val}`)"
        embed.add_field(name="🎮 Ник в MLBB", value=link_text, inline=False)
    else:
        embed.add_field(name="🎮 Ник в MLBB", value="_Не привязан · `/link`_", inline=False)

    embed.add_field(name="🎖️ ЭЛО Рейтинг", value=f"**{elo}**", inline=True)
    embed.add_field(name="🏆 Текущий Ранг", value=f"**{rank_name}**", inline=True)
    embed.add_field(name="🌟 Количество MVP", value=f"🏆 **{mvps}**", inline=True)

    stats_block = (
        f"🎮 Сыграно матчей: **{matches}**\n"
        f"📈 Процент побед: **{winrate:.1f}%**\n"
        f"🟢 Победы: **{wins}** · 🔴 Поражения: **{losses}**"
    )
    embed.add_field(name="📊 Игровая Статистика", value=stats_block, inline=False)
    embed.add_field(name="⏳ Последние 5 игр", value="\n".join(history_lines), inline=False)
    embed.set_footer(text=f"ID: {target.id} · USHU TEST MLBB")

    await interaction.response.send_message(embed=embed)


# ───────────── /link ──────────────────
@bot.tree.command(name="link", description="Привязать свой игровой ник MLBB к Discord-аккаунту")
@app_commands.describe(
    nickname="Ваш ник в Mobile Legends (как отображается в игре)",
    game_id="Ваш ID в игре (необязательно, например: 123456789)"
)
async def cmd_link(interaction: discord.Interaction, nickname: str, game_id: str | None = None):
    if len(nickname) > 100:
        await interaction.response.send_message(
            "❌ Ник слишком длинный (макс. 100 символов).", ephemeral=True
        )
        return

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT game_nickname FROM linked_accounts WHERE user_id = $1",
            interaction.user.id
        )
        
        await conn.execute(
            """
            INSERT INTO linked_accounts (user_id, game_nickname, game_id) 
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE 
            SET game_nickname = $2, game_id = $3, linked_at = CURRENT_TIMESTAMP
            """,
            interaction.user.id, nickname, game_id
        )

    id_text = f"\n🆔 ID в игре: **{game_id}**" if game_id else ""

    if existing:
        await interaction.response.send_message(
            f"✅ Игровой ник обновлён!\n"
            f"Старый ник: ~~{existing['game_nickname']}~~\n"
            f"Новый ник: **{nickname}**{id_text}",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"✅ Аккаунт успешно привязан!\n"
            f"🎮 Ник в MLBB: **{nickname}**{id_text}\n\n"
            f"Теперь бот сможет распознавать вас на скриншотах результатов матча.",
            ephemeral=True
        )


# ───────────── /unlink ────────────────
@bot.tree.command(name="unlink", description="Отвязать игровой ник MLBB от Discord-аккаунта")
async def cmd_unlink(interaction: discord.Interaction):
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM linked_accounts WHERE user_id = $1",
            interaction.user.id
        )
    
    if result == "DELETE 1":
        await interaction.response.send_message(
            "✅ Игровой ник успешно отвязан от вашего аккаунта.", ephemeral=True
        )
    else:
        await interaction.response.send_message(
            "❌ У вас нет привязанного игрового ника. Используйте `/link` для привязки.", ephemeral=True
        )


# ───────────── /link_admin ────────────
@bot.tree.command(name="link_admin", description="[Админ] Привязать игровой ник MLBB за другого игрока")
@app_commands.describe(
    player="Игрок Discord, которому нужно привязать ник",
    nickname="Ник игрока в Mobile Legends",
    game_id="ID игрока в MLBB (необязательно)"
)
@app_commands.default_permissions(administrator=True)
async def cmd_link_admin(interaction: discord.Interaction, player: discord.Member, nickname: str, game_id: str | None = None):
    if len(nickname) > 100:
        await interaction.response.send_message(
            "❌ Ник слишком длинный (макс. 100 символов).", ephemeral=True
        )
        return

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT game_nickname FROM linked_accounts WHERE user_id = $1",
            player.id
        )
        
        await conn.execute(
            """
            INSERT INTO linked_accounts (user_id, game_nickname, game_id) 
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE 
            SET game_nickname = $2, game_id = $3, linked_at = CURRENT_TIMESTAMP
            """,
            player.id, nickname, game_id
        )

    id_text = f"\n🆔 ID в игре: **{game_id}**" if game_id else ""

    if existing:
        await interaction.response.send_message(
            f"✅ Ник игрока {player.mention} обновлён!\n"
            f"Старый ник: ~~{existing['game_nickname']}~~\n"
            f"Новый ник: **{nickname}**{id_text}"
        )
    else:
        await interaction.response.send_message(
            f"✅ Аккаунт {player.mention} успешно привязан!\n"
            f"🎮 Ник в MLBB: **{nickname}**{id_text}"
        )



# ═══════════════════════════ OCR SCAN ════════════════════════════

def fuzzy_match(ocr_text: str, nickname: str) -> float:
    """Нечёткое сравнение OCR-текста с ником из базы."""
    ocr_clean = ocr_text.lower().strip()
    nick_clean = nickname.lower().strip()
    
    # Точное совпадение
    if ocr_clean == nick_clean:
        return 1.0
    
    # Проверяем, содержится ли ник в тексте или наоборот
    if nick_clean in ocr_clean or ocr_clean in nick_clean:
        return 0.9
    
    # Нечёткое сравнение
    return SequenceMatcher(None, ocr_clean, nick_clean).ratio()


async def analyze_screenshot(image_bytes: bytes) -> dict:
    """Анализирует скриншот MLBB и извлекает данные с помощью EasyOCR."""
    loop = asyncio.get_event_loop()
    
    # Открываем изображение
    original_img = Image.open(io.BytesIO(image_bytes))
    img_width, img_height = original_img.size
    
    # ПРЕДОБРАБОТКА ДЛЯ ЛУЧШЕГО РАСПОЗНАВАНИЯ МЕЛКОГО ШРИФТА:
    # 1. Увеличиваем изображение в 2 раза для четкости
    resized_img = original_img.resize((img_width * 2, img_height * 2), Image.Resampling.LANCZOS)
    # 2. Конвертируем в градации серого (убирает влияние цветных клан-тегов)
    processed_img = resized_img.convert("L")
    img_array = np.array(processed_img)
    
    # Запускаем EasyOCR в отдельном потоке
    reader = get_ocr_reader()
    results = await loop.run_in_executor(None, lambda: reader.readtext(img_array))
    
    all_texts = []
    for (bbox, text, conf) in results:
        # bbox = [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]
        # Координаты на увеличенном изображении, делим на 2 для возврата к оригиналу
        center_x = (sum(p[0] for p in bbox) / 4) / 2.0
        center_y = (sum(p[1] for p in bbox) / 4) / 2.0
        
        all_texts.append({
            "text": text,
            "confidence": conf,
            "center_x": center_x,
            "center_y": center_y,
            "side": "left" if center_x < img_width / 2.0 else "right"
        })
    
    # Определяем результат матча (VICTORY / DEFEAT / ПОБЕДА)
    match_result = None
    # Сначала проверяем классические ключевые слова
    for t in all_texts:
        text_upper = t["text"].upper()
        if "VICTORY" in text_upper or "ПОБЕДА" in text_upper or "VIC" in text_upper:
            match_result = "victory"
            break
        elif "DEFEAT" in text_upper or "ПОРАЖЕН" in text_upper or "DEF" in text_upper:
            match_result = "defeat"
            break
 
    # Дополнительная проверка счета сверху как запасной вариант, если текст не найден
    if not match_result:
        # Ищем два числа в самом верху экрана (y < 15% высоты)
        score_candidates = []
        for t in all_texts:
            if t["center_y"] < img_height * 0.15:
                # Пытаемся распарсить как число
                try:
                    val = int(t["text"].replace("o", "0").replace("O", "0").strip())
                    score_candidates.append((val, t["center_x"]))
                except ValueError:
                    continue
        
        # Если нашли два числа, делим на левое и правое
        if len(score_candidates) >= 2:
            # Сортируем по X
            score_candidates.sort(key=lambda item: item[1])
            left_score = score_candidates[0][0]
            right_score = score_candidates[-1][0]
            # Левый счет > правого счета -> Синие (левые) выиграли -> victory
            if left_score != right_score:
                match_result = "victory" if left_score > right_score else "defeat"
                print(f"[OCR Fallback] Результат по счету: Left {left_score} vs Right {right_score} -> {match_result}")
    
    # Определяем MVP
    mvp_detected = []
    for t in all_texts:
        text_upper = t["text"].upper()
        if "MVP" in text_upper or "МВП" in text_upper:
            mvp_detected.append(t)
            
    return {
        "all_texts": all_texts,
        "match_result": match_result,
        "mvp_positions": mvp_detected,
        "img_width": img_width,
        "img_height": img_height
    }


def match_players(ocr_data: dict, linked_accounts: list) -> dict:
    """Сопоставляет OCR-тексты с привязанными никами."""
    all_texts = ocr_data["all_texts"]
    match_result = ocr_data["match_result"]
    mvp_positions = ocr_data["mvp_positions"]
    
    # Фильтруем текст: исключаем числа, слишком короткие строки и служебные слова
    skip_words = {"VICTORY", "DEFEAT", "MVP", "NEW", "ДАННЫЕ", "ВЫЙТИ", 
                  "ЛАЙК", "ВСЕМ", "БЫСТРЫЙ", "ЧАТ", "БОЕВОЙ", "ДЛИТЕЛЬНОСТЬ",
                  "Ты", "замечательный", "противник", "Длительность"}
    
    candidate_texts = []
    for t in all_texts:
        text = t["text"].strip()
        # Пропускаем чистые числа, слишком короткие, и служебные слова
        if len(text) < 2:
            continue
        if text.replace(".", "").replace(",", "").replace(" ", "").isdigit():
            continue
        if text.upper() in skip_words:
            continue
        if any(sw.lower() in text.lower() for sw in skip_words):
            continue
        candidate_texts.append(t)
    
    # Сопоставляем каждый привязанный ник с найденными текстами
    matched_players = []
    unmatched_linked = []
    
    for acc in linked_accounts:
        nickname = acc["game_nickname"]
        user_id = acc["user_id"]
        
        best_match = None
        best_score = 0.0
        
        for t in candidate_texts:
            score = fuzzy_match(t["text"], nickname)
            if score > best_score:
                best_score = score
                best_match = t
        
        if best_score >= 0.55 and best_match:
            # Определяем сторону: left = "Команда 1", right = "Команда 2"
            side = best_match["side"]
            
            # Определяем, победитель или проигравший
            if match_result == "victory":
                is_winner = (side == "left")
            elif match_result == "defeat":
                is_winner = (side == "right")
            else:
                is_winner = None
            
            # Проверяем, рядом ли MVP
            is_mvp = False
            if mvp_positions:
                for mvp_pos in mvp_positions:
                    # MVP и ник на одной стороне и близко по Y
                    if mvp_pos["side"] == side:
                        y_diff = abs(mvp_pos["center_y"] - best_match["center_y"])
                        if y_diff < 80:
                            is_mvp = True
                            break
            
            matched_players.append({
                "user_id": user_id,
                "game_nickname": nickname,
                "ocr_text": best_match["text"],
                "match_score": best_score,
                "side": side,
                "is_winner": is_winner,
                "is_mvp": is_mvp
            })
        else:
            unmatched_linked.append({
                "user_id": user_id,
                "game_nickname": nickname,
                "best_score": best_score
            })
    
    return {
        "matched": matched_players,
        "unmatched": unmatched_linked,
        "match_result": match_result,
        "total_ocr_texts": len(candidate_texts)
    }


class ScanConfirmView(discord.ui.View):
    """Кнопки подтверждения/отмены результатов OCR-анализа."""
    
    def __init__(self, matched_players: list, guild_id: int):
        super().__init__(timeout=120)
        self.matched_players = matched_players
        self.guild_id = guild_id
    
    @discord.ui.button(label="✅ Подтвердить (Тестовый режим)", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Только админ может подтвердить.", ephemeral=True)
            return
        
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
        
        winners = [p for p in self.matched_players if p["is_winner"] is True]
        losers = [p for p in self.matched_players if p["is_winner"] is False]
        mvps = [p for p in self.matched_players if p.get("is_mvp")]
        
        # Временное отключение записей в БД для тестового режима
        # Никакие изменения ЭЛО или статистики не применяются.
        
        # Формируем итоговое сообщение
        result_lines = []
        if winners:
            w_mentions = " ".join(f"<@{p['user_id']}>" for p in winners)
            result_lines.append(f"🟢 **Победители (тест, +50 ЭЛО не начислено):**\n{w_mentions}")
        if losers:
            l_mentions = " ".join(f"<@{p['user_id']}>" for p in losers)
            result_lines.append(f"🔴 **Проигравшие (тест, -50 ЭЛО не списано):**\n{l_mentions}")
        if mvps:
            m_mentions = " ".join(f"<@{p['user_id']}>" for p in mvps)
            result_lines.append(f"🌟 **MVP (тест, +25 ЭЛО не начислено):**\n{m_mentions}")
        
        await interaction.followup.send(
            f"🧪 **[Тестовый режим] Результаты матча успешно проверены по скриншоту!**\n"
            f"*(Никакие изменения ЭЛО или статистики не записывались в базу данных)*\n\n" + "\n\n".join(result_lines)
        )
    
    @discord.ui.button(label="❌ Отмена", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Только админ может отменить.", ephemeral=True)
            return
        
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content="❌ **Анализ скриншота отменён.** Результаты не были применены.",
            view=self
        )


# ───────────── /scan ──────────────────
@bot.tree.command(name="scan", description="[Админ] Сканировать скриншот результатов матча MLBB и начислить ЭЛО")
@app_commands.describe(screenshot="Скриншот с результатами матча MLBB")
@app_commands.default_permissions(administrator=True)
async def cmd_scan(interaction: discord.Interaction, screenshot: discord.Attachment):
    # Проверяем, что это изображение
    if not screenshot.content_type or not screenshot.content_type.startswith("image/"):
        await interaction.response.send_message(
            "❌ Прикрепите изображение (скриншот результатов матча).", ephemeral=True
        )
        return
    
    await interaction.response.defer(thinking=True)
    
    try:
        # Скачиваем изображение
        image_bytes = await screenshot.read()
        
        # Анализируем скриншот
        ocr_data = await analyze_screenshot(image_bytes)
        
        # Загружаем все привязанные аккаунты с сервера
        async with db_pool.acquire() as conn:
            linked_accounts = await conn.fetch(
                """
                SELECT la.user_id, la.game_nickname 
                FROM linked_accounts la
                """
            )
        
        if not linked_accounts:
            await interaction.followup.send(
                "❌ В базе нет привязанных аккаунтов. Игроки должны использовать `/link` для привязки ников.",
                ephemeral=True
            )
            return
        
        # Выводим в лог все распознанные слова для отладки
        raw_words = [t["text"] for t in ocr_data["all_texts"]]
        print(f"[OCR DEBUG] Все распознанные слова: {raw_words}")
        
        # Сопоставляем
        results = match_players(ocr_data, linked_accounts)
        
        # Строим красивый Embed с результатами
        match_status = "🟢 ПОБЕДА (VICTORY)" if results["match_result"] == "victory" else (
            "🔴 ПОРАЖЕНИЕ (DEFEAT)" if results["match_result"] == "defeat" else "❓ Не удалось определить"
        )
        
        embed = discord.Embed(
            title="🔍 АНАЛИЗ СКРИНШОТА MLBB",
            description=f"**Результат матча:** {match_status}\n*(Тестовый режим: подтверждение не меняет ЭЛО)*",
            color=0x00FF00 if results["match_result"] == "victory" else (
                0xFF0000 if results["match_result"] == "defeat" else 0xFFFF00
            )
        )
        embed.set_thumbnail(url=screenshot.url)
        
        # Найденные игроки
        if results["matched"]:
            winners_lines = []
            losers_lines = []
            
            for p in results["matched"]:
                member = interaction.guild.get_member(p["user_id"])
                name = member.mention if member else f"<@{p['user_id']}>"
                accuracy = f"{p['match_score']*100:.0f}%"
                mvp_badge = " 🌟MVP" if p.get("is_mvp") else ""
                ocr_hint = f" _(OCR: «{p['ocr_text']}»)_" if p['match_score'] < 0.95 else ""
                
                line = f"{name} — `{p['game_nickname']}`{mvp_badge} ({accuracy}){ocr_hint}"
                
                if p["is_winner"] is True:
                    winners_lines.append(f"🟢 {line}")
                elif p["is_winner"] is False:
                    losers_lines.append(f"🔴 {line}")
                else:
                    winners_lines.append(f"❓ {line}")
            
            if winners_lines:
                embed.add_field(
                    name="🏆 Победители (+50 ЭЛО)",
                    value="\n".join(winners_lines),
                    inline=False
                )
            if losers_lines:
                embed.add_field(
                    name="💀 Проигравшие (-50 ЭЛО)",
                    value="\n".join(losers_lines),
                    inline=False
                )
        else:
            embed.add_field(
                name="⚠️ Игроки не найдены",
                value="Ни один привязанный ник не совпал с текстом на скриншоте.\n"
                      "Убедитесь, что игроки привязали ники через `/link`.",
                inline=False
            )
        embed.set_footer(text=f"Распознано {len(ocr_data['all_texts'])} текстовых элементов · EasyOCR")
        
        # Если есть совпадения — показываем кнопки подтверждения
        if results["matched"] and results["match_result"]:
            view = ScanConfirmView(results["matched"], interaction.guild_id)
            await interaction.followup.send(
                "📋 **Проверьте результаты анализа и подтвердите начисление ЭЛО:**",
                embed=embed,
                view=view
            )
        else:
            await interaction.followup.send(
                "⚠️ **Результаты анализа (автоматическое начисление невозможно):**",
                embed=embed
            )
    
    except Exception as e:
        await interaction.followup.send(
            f"❌ Ошибка при анализе скриншота: ```{str(e)[:500]}```",
            ephemeral=True
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


# ───────────── /cancel_match ────────────
@bot.tree.command(name="cancel_match", description="[Админ/Ведущий] Отменить текущий активный (запущенный) матч без начисления ЭЛО")
async def cmd_cancel_match(interaction: discord.Interaction):
    if not is_moderator(interaction.user):
        await interaction.response.send_message(
            "❌ У вас нет прав Ведущего или Администратора для отмены матча.",
            ephemeral=True
        )
        return

    guild_id = interaction.guild_id
    async with db_pool.acquire() as conn:
        players = await conn.fetch(
            "SELECT user_id FROM active_match_heroes WHERE guild_id = $1",
            guild_id
        )

        if not players:
            await interaction.response.send_message(
                "❌ Нет запущенного активного матча для отмены.",
                ephemeral=True
            )
            return

        await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", guild_id)
        await interaction.response.send_message(
            "🛑 **Текущий активный матч был отменён Ведущим/Администратором.** Данные матча стёрты, ЭЛО не изменено."
        )


# ───────────── /reroll ────────────────
@bot.tree.command(
    name="reroll",
    description="Перекрутить (заменить) выданного героя на другого для роли (Лимит: 2 раза)"
)
@app_commands.describe(player="[Ведущий/Админ] Игрок, которому нужно заменить героя (по умолчанию — вы сами)")
async def cmd_reroll(interaction: discord.Interaction, player: discord.Member | None = None):
    caller = interaction.user
    guild_id = interaction.guild_id

    # Определяем, чьего героя мы крутим
    target_user = player if player else caller

    # Если крутим за другого игрока, проверяем, является ли вызвавший ведущим/админом
    if target_user.id != caller.id:
        if not is_moderator(caller):
            await interaction.response.send_message(
                "❌ Вы можете заменять героев только себе. Заменять героев другим игрокам могут только Ведущий или Администраторы.",
                ephemeral=True
            )
            return

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT hero_name, role, match_id, is_ranked, rerolls 
            FROM active_match_heroes 
            WHERE user_id = $1 AND guild_id = $2
            """,
            target_user.id, guild_id
        )

        if not row:
            msg_target = "Вы не участвуете" if target_user.id == caller.id else f"Игрок **{target_user.display_name}** не участвует"
            await interaction.response.send_message(
                f"❌ {msg_target} в текущем сформированном матче.",
                ephemeral=True
            )
            return

        current_hero = row["hero_name"]
        role = row["role"]
        match_id = row["match_id"]
        current_rerolls = row["rerolls"]

        # Проверка лимита в 2 реролла
        if current_rerolls >= 2:
            msg_limit = "Вы исчерпали" if target_user.id == caller.id else f"Игрок **{target_user.display_name}** исчерпал"
            await interaction.response.send_message(
                f"❌ {msg_limit} лимит замен героев (максимум 2 замены на матч).",
                ephemeral=True
            )
            return

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
        new_reroll_count = current_rerolls + 1

        await conn.execute(
            """
            UPDATE active_match_heroes 
            SET hero_name = $1, rerolls = $2
            WHERE user_id = $3 AND guild_id = $4
            """,
            new_hero, new_reroll_count, target_user.id, guild_id
        )

        role_emojis = {
            "gold": "🪙 Gold",
            "exp": "🛡️ Exp",
            "mid": "🔮 Mid",
            "jungle": "⚔️ Jungle",
            "roam": "👣 Roam"
        }
        role_label = role_emojis.get(role, role.capitalize())

        actor_str = "" if target_user.id == caller.id else f" *(выполнено ведущим {caller.mention})*"
        await interaction.response.send_message(
            f"🔄 {target_user.mention}, герой был заменен!{actor_str}\n"
            f"Роль: **{role_label}** · Попытка: **{new_reroll_count}/2**\n"
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
            SELECT user_id, team_name, is_ranked, hero_name, role, match_id 
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
            winners = [p for p in players if p["team_name"] == winner_team_val]
            losers = [p for p in players if p["team_name"] != winner_team_val]
            
            winners_ids = [p["user_id"] for p in winners]
            losers_ids = [p["user_id"] for p in losers]

            # Победителям +50 ЭЛО, +1 к матчам, +1 к победам
            for p in winners:
                uid = p["user_id"]
                hero = p["hero_name"]
                role = p["role"]
                m_id = p["match_id"]
                await conn.execute(
                    """
                    INSERT INTO players (user_id, elo) VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 50
                    """,
                    uid, DEFAULT_ELO + 50
                )
                await conn.execute(
                    """
                    INSERT INTO player_stats (user_id, matches, wins, losses, mvps) VALUES ($1, 1, 1, 0, 0)
                    ON CONFLICT (user_id) DO UPDATE SET matches = player_stats.matches + 1, wins = player_stats.wins + 1
                    """,
                    uid
                )
                await conn.execute(
                    """
                    INSERT INTO match_history (user_id, match_id, hero_name, role, is_win)
                    VALUES ($1, $2, $3, $4, TRUE)
                    """,
                    uid, m_id, hero, role
                )
            
            # Проигравшим -50 ЭЛО (но не ниже 0), +1 к матчам, +1 к поражениям
            for p in losers:
                uid = p["user_id"]
                hero = p["hero_name"]
                role = p["role"]
                m_id = p["match_id"]
                await conn.execute(
                    """
                    INSERT INTO players (user_id, elo) VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET elo = GREATEST(players.elo - 50, 0)
                    """,
                    uid, DEFAULT_ELO - 50
                )
                await conn.execute(
                    """
                    INSERT INTO player_stats (user_id, matches, wins, losses, mvps) VALUES ($1, 1, 0, 1, 0)
                    ON CONFLICT (user_id) DO UPDATE SET matches = player_stats.matches + 1, losses = player_stats.losses + 1
                    """,
                    uid
                )
                await conn.execute(
                    """
                    INSERT INTO match_history (user_id, match_id, hero_name, role, is_win)
                    VALUES ($1, $2, $3, $4, FALSE)
                    """,
                    uid, m_id, hero, role
                )
            
            await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", guild_id)

            mentions_win = " ".join(f"<@{uid}>" for uid in winners_ids)
            mentions_loss = " ".join(f"<@{uid}>" for uid in losers_ids)
            await interaction.response.send_message(
                f"🎉 **Команда {team.name} побеждает в рейтинговом матче!**\n"
                f"🟢 Победители получают **+50 ЭЛО**:\n{mentions_win}\n"
                f"🔴 Проигравшие теряют **-50 ЭЛО**:\n{mentions_loss}"
            )
        else:
            await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", guild_id)
            await interaction.response.send_message(
                f"🎉 **Команда {team.name} побеждает!**\n*(Этот матч был не рейтинговым, ЭЛО изменено не было)*"
            )


# ───────────── /mvp_win ──────────────
@bot.tree.command(
    name="mvp_win",
    description="[Админ] Начислить MVP победившей команды (+25 ЭЛО)"
)
@app_commands.describe(player="Игрок, получивший MVP в победившей команде")
@app_commands.default_permissions(administrator=True)
async def cmd_mvp_win(interaction: discord.Interaction, player: discord.Member):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (user_id, elo) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 25
            """,
            player.id, DEFAULT_ELO + 25
        )
        await conn.execute(
            """
            INSERT INTO player_stats (user_id, mvps) VALUES ($1, 1)
            ON CONFLICT (user_id) DO UPDATE SET mvps = player_stats.mvps + 1
            """,
            player.id
        )
    await interaction.response.send_message(
        f"🌟 **MVP победителей!** Игрок {player.mention} получает **+25 ЭЛО**!"
    )


# ───────────── /mvp_loss ─────────────
@bot.tree.command(
    name="mvp_loss",
    description="[Админ] Начислить MVP проигравшей команды (+25 ЭЛО)"
)
@app_commands.describe(player="Игрок, получивший MVP в проигравшей команде")
@app_commands.default_permissions(administrator=True)
async def cmd_mvp_loss(interaction: discord.Interaction, player: discord.Member):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO players (user_id, elo) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET elo = players.elo + 25
            """,
            player.id, DEFAULT_ELO + 25
        )
        await conn.execute(
            """
            INSERT INTO player_stats (user_id, mvps) VALUES ($1, 1)
            ON CONFLICT (user_id) DO UPDATE SET mvps = player_stats.mvps + 1
            """,
            player.id
        )
    await interaction.response.send_message(
        f"🌟 **MVP проигравших!** Игрок {player.mention} получает **+25 ЭЛО**!"
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
    await interaction.response.defer()

    # Для полноценного теста сделаем так, чтобы ТЫ (тот, кто ввёл команду) был одним из игроков (Player 1)
    # Это позволит тебе нажимать кнопку «Реролл» и видеть, как меняется твой герой!
    caller = interaction.user
    fake_members = [caller] + [interaction.guild.me] * 9  # Симулируем 10 участников

    # Создадим фейковый LobbyState
    class FakeLobby:
        def __init__(self, channel, match_type="ranked"):
            self.voice_channel = channel
            self.text_channel = channel
            self.match_type = match_type
            self.finished = False

    # Записываем матч в БД так, как будто он запущен
    heroes = load_heroes()
    user_ids = [m.id for m in fake_members]
    
    # Распределяем героев для теста
    team_a_full = []
    team_b_full = []
    roles_order = ["gold", "exp", "mid", "jungle", "roam"]
    
    # Раздаем случайных героев
    role_picks = pick_unique_heroes_by_roles(heroes)
    role_to_heroes = {r: [] for r in roles_order}
    for h_name, role in role_picks:
        role_to_heroes[role].append(h_name)

    # Команда 1
    for i in range(5):
        m = fake_members[i]
        role = roles_order[i]
        hero = role_to_heroes[role][0]
        # Для фейков ЭЛО будет 1000, для тебя — твое реальное
        elo = 1000
        team_a_full.append((m, elo, hero, role))

    # Команда 2
    for i in range(5):
        m = fake_members[i + 5]
        role = roles_order[i]
        hero = role_to_heroes[role][1]
        elo = 1000
        team_b_full.append((m, elo, hero, role))

    # Пишем в БД под фейковым match_id
    match_id = f"test_{int(asyncio.get_event_loop().time())}"
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM active_match_heroes WHERE guild_id = $1", interaction.guild_id)
        for member, elo, hero, role in team_a_full:
            await conn.execute(
                """
                INSERT INTO active_match_heroes (user_id, guild_id, hero_name, role, match_id, team_name, is_ranked)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (user_id) DO UPDATE SET hero_name = $3, role = $4, match_id = $5, team_name = $6
                """,
                member.id, interaction.guild_id, hero, role, match_id, "Команда 1", True
            )
        for member, elo, hero, role in team_b_full:
            await conn.execute(
                """
                INSERT INTO active_match_heroes (user_id, guild_id, hero_name, role, match_id, team_name, is_ranked)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (user_id) DO UPDATE SET hero_name = $3, role = $4, match_id = $5, team_name = $6
                """,
                member.id, interaction.guild_id, hero, role, match_id, "Команда 2", True
            )

    # ─── СОЗДАНИЕ EMBED'А РЕЗУЛЬТАТА ───
    role_emojis = {
        "gold": "🪙 Gold",
        "exp": "🛡️ Exp",
        "mid": "🔮 Mid",
        "jungle": "⚔️ Jungle",
        "roam": "👣 Roam"
    }

    def build_embed_team_section(team_full_list: list) -> str:
        sec_lines = []
        for idx, (m, elo, hero, role) in enumerate(team_full_list, start=1):
            role_label = role_emojis.get(role, role.capitalize())
            # Выводим упоминание игрока
            sec_lines.append(f"`{idx}.` {m.mention}\n    {role_label} · 🦸 : **{hero}**")
        return "\n".join(sec_lines)

    result_embed = discord.Embed(
        title="🏆 [ТЕСТ] РЕЙТИНГОВЫЙ МАТЧ СФОРМИРОВАН!",
        description=(
            "Разница ЭЛО команд: **0**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Герои распределены по ролям. Это тестовый матч.\n"
            "Вы добавлены в **Команду 1**! Нажмите кнопку **Реролл 🎲** ниже, чтобы сменить своего героя."
        ),
        color=0xFEE75C,
    )
    result_embed.add_field(
        name="🔵 КОМАНДА 1",
        value=build_embed_team_section(team_a_full),
        inline=False
    )
    result_embed.add_field(
        name="🔴 КОМАНДА 2",
        value=build_embed_team_section(team_b_full),
        inline=False
    )

    view = PlayerRerollView()
    
    # Также выведем Ведущему панели Reroll/Start, чтобы показать, как они выглядят
    # (в качестве фейковых команд)
    preview_team_a = [(m, elo) for m, elo, _, _ in team_a_full]
    preview_team_b = [(m, elo) for m, elo, _, _ in team_b_full]
    
    preview_embed = discord.Embed(
        title="⚖️  [ТЕСТ] ПРЕДПРОСМОТР БАЛАНСА КОМАНД",
        description=(
            "Режим: **RANKED**\n"
            "Разница ЭЛО команд: **0**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Ведущий, проверьте состав. Вы можете сделать **один перерандом**."
        ),
        color=0xFEE75C
    )
    embed_a = discord.Embed(title="🔵 КОМАНДА 1", description="Суммарный ЭЛО: **1000**", color=EMBED_COLOR_TEAM_A)
    embed_a.add_field(name="Состав", value="\n".join(f"`{i+1}.` {m.mention} — 🎖️ 1000 ЭЛО" for i, (m, _) in enumerate(preview_team_a)), inline=False)
    
    embed_b = discord.Embed(title="🔴 КОМАНДА 2", description="Суммарный ЭЛО: **1000**", color=EMBED_COLOR_TEAM_B)
    embed_b.add_field(name="Состав", value="\n".join(f"`{i+1}.` {m.mention} — 🎖️ 1000 ЭЛО" for i, (m, _) in enumerate(preview_team_b)), inline=False)

    fake_lobby_obj = FakeLobby(interaction.channel)
    preview_view = MatchPreviewView(fake_lobby_obj, fake_members, preview_team_a, preview_team_b, 1)

    await interaction.followup.send(
        content="⬇️ **[ТЕСТ] Так выглядит панель Ведущего для перерандома и старта:**",
        embeds=[preview_embed, embed_a, embed_b],
        view=preview_view
    )

    await interaction.channel.send(
        content=f"⬇️ **[ТЕСТ] Так выглядит финальный Embed с героями и кнопкой замены:**",
        embed=result_embed,
        view=view
    )


# ═══════════════════════════ RUN ═════════════════════════════════
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
