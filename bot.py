import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from math import comb

import aiosqlite
import pytz
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    ChatMemberUpdated,
    LabeledPrice,
    PreCheckoutQuery,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from aiogram.filters import Command, ChatMemberUpdatedFilter, JOIN_TRANSITION
from cachetools import TTLCache
from dotenv import load_dotenv
import os

# =========================
# CONFIG
# =========================

load_dotenv()

TOKEN        = os.getenv("BOT_TOKEN", "")
MAIN_CHAT_ID = int(os.getenv("MAIN_CHAT_ID", "0"))
LOG_CHAT_ID  = int(os.getenv("LOG_CHAT_ID", "0"))
GAME_LOG_CHAT_ID = int(os.getenv("GAME_LOG_CHAT_ID", "0"))
ADMIN_ID     = int(os.getenv("ADMIN_ID", "0"))
# Обязательная подписка перед открытием меню бота.
REQUIRED_CHANNEL = "@d_coins_channel"
REQUIRED_CHANNEL_URL = "https://t.me/d_coins_channel"
# Можно переопределить отдельным ID/username в Railway, если промокоды нужны в другом канале.
PROMO_CHANNEL_ID = os.getenv("PROMO_CHANNEL_ID", REQUIRED_CHANNEL)

COOLDOWN_SECONDS   = 1
START_CHANCE       = 0.1
STEP               = 0.002
MAX_CHANCE         = 100.0
BONUS_COOLDOWN     = 43200
REF_BONUS          = 1.0
VALID_REF_MESSAGES = 10

# D-COINS
COINS_START        = 10
COINS_PER_MSG      = 1
COINS_VIP_PER_MSG  = 2
COINS_BONUS        = 20
COINS_VIP_BONUS    = 30
COINS_BONUS_CD     = 43200

# Казино
CASINO_MIN_BET     = 5
CASINO_TIMEOUT     = 300  # 5 минут
CASINO_BET_COOLDOWN = 10  # секунд между ставками одного пользователя
CASE_OPEN_COOLDOWN = 5    # секунд между открытиями кейса

# Обмен
EXCHANGE_CHANCE    = 5000   # 5 000 DC = +1% шанса
EXCHANGE_GIFT_15   = 15000
EXCHANGE_GIFT_25   = 25000
EXCHANGE_GIFT_50   = 50000
EXCHANGE_GIFT_100  = 100000
EXCHANGE_PREMIUM_1_MONTH = 500000

# Покупка DC за Telegram Stars: 100 000 DC = 200⭐, до 1 000 000 DC.
STAR_DC_PACKAGES = {amount: (amount // 100_000) * 200 for amount in range(100_000, 1_000_001, 100_000)}

# Кейсы
CASES = {
    "karapuz": {
        "title": "KARAPUZ",
        "price": 1000,
        "rewards": [
            (100, 5), (150, 7), (300, 12), (500, 15), (600, 16),
            (700, 15), (800, 12), (900, 8), (1000, 5), (1500, 3), (3000, 2),
        ],
    },
    "blood": {
        "title": "BLOOD",
        "price": 5000,
        "rewards": [
            ("coins", 300, 10), ("coins", 500, 12), ("coins", 700, 14),
            ("coins", 1000, 16), ("coins", 1500, 17), ("coins", 2500, 14),
            ("coins", 4000, 8), ("coins", 7000, 6),
            ("gift", 15, 2), ("gift", 25, 0.7), ("gift", 50, 0.3),
        ],
    },
    "pantera": {
        "title": "PANTERA",
        "price": 10000,
        "rewards": [
            ("coins", 1000, 16), ("coins", 5000, 21), ("coins", 6000, 21),
            ("coins", 8000, 18), ("coins", 9000, 13), ("coins", 12000, 6),
            ("coins", 15000, 3), ("gift", 15, 1), ("gift", 25, 0.5),
            ("gift", 50, 0.3), ("gift", 100, 0.2),
        ],
    },
    "spider_man": {
        "title": "SPIDER MAN",
        "price": 7500,
        "rewards": [
            ("coins", 500, 5), ("coins", 1000, 10), ("coins", 1500, 14),
            ("coins", 2500, 18), ("coins", 3500, 18), ("coins", 5000, 16),
            ("coins", 7500, 11), ("coins", 10000, 5), ("coins", 15000, 1),
            ("gift", 15, 2), ("gift", 25, 0.7), ("gift", 50, 0.3),
        ],
    },
}

BAN_MESSAGE = "🚫 Вы заблокированы и не можете участвовать в розыгрышах в боте."

POPOLNIT_AMOUNT = 50

REF_REWARDS = {
    5:  "15⭐",
    10: "25⭐",
    15: "50⭐",
    20: "100⭐",
}

REF_GIFT_IDS = {
    5:  ["5170145012310081615", "5170233102089322756"],
    10: ["5170250947678437525", "5168103777563050263"],
    15: ["5170144170496491616", "5170314324215857265",
         "5170564780938756245", "6028601630662853006"],
    20: ["5168043875654172773", "5170690322832818290",
         "5170521118301225164"],
}

WIN_GIFT_IDS = [
    "5170233102089322756",
    "5170233102089322756",  # <- вставь второй ID подарка
]

# Активные игры казино: user_id -> {"game": str, "bet": int, "data": dict, "expires": float}
active_games: dict = {}

logger = logging.getLogger(__name__)

# =========================
# DATABASE
# =========================

class Database:

    def __init__(self, path: str = "activity.db"):
        self.path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id    INTEGER,
                    chat_id    INTEGER,
                    user_name  TEXT,
                    chance     REAL    DEFAULT 0.1,
                    msg_count  INTEGER DEFAULT 0,
                    last_bonus REAL    DEFAULT 0,
                    PRIMARY KEY (user_id, chat_id)
                )
            """)
            # Username нужен для /transfer @username.
            # Добавляем колонку безопасно для уже существующей базы.
            async with db.execute("PRAGMA table_info(user_stats)") as cur:
                columns = [row[1] for row in await cur.fetchall()]
            if "username" not in columns:
                await db.execute("ALTER TABLE user_stats ADD COLUMN username TEXT")

            await db.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    invited_user_id INTEGER PRIMARY KEY,
                    inviter_user_id INTEGER NOT NULL,
                    valid           INTEGER DEFAULT 0,
                    msg_count       INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS invite_links (
                    user_id     INTEGER PRIMARY KEY,
                    invite_link TEXT    NOT NULL UNIQUE
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS wins (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    chat_id    INTEGER NOT NULL,
                    user_name  TEXT,
                    chance     REAL,
                    won_at     REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pending_gifts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    user_name   TEXT,
                    gift_id     TEXT NOT NULL,
                    reason      TEXT,
                    created_at  REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS premium_orders (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    user_name   TEXT,
                    cost        INTEGER NOT NULL,
                    created_at  REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS star_coin_purchases (
                    payment_charge_id TEXT PRIMARY KEY,
                    user_id           INTEGER NOT NULL,
                    dc_amount         INTEGER NOT NULL,
                    star_amount       INTEGER NOT NULL,
                    created_at        REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS vip_users (
                    user_id INTEGER PRIMARY KEY
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id   INTEGER PRIMARY KEY,
                    reason    TEXT,
                    banned_at REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    user_id   INTEGER,
                    chat_id   INTEGER,
                    user_name TEXT,
                    date      TEXT NOT NULL,
                    msg_count INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, chat_id, date)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS coins (
                    user_id         INTEGER PRIMARY KEY,
                    balance         INTEGER DEFAULT 10,
                    last_coin_bonus REAL    DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code       TEXT PRIMARY KEY COLLATE NOCASE,
                    reward     INTEGER NOT NULL,
                    reward_type TEXT NOT NULL DEFAULT 'coins',
                    case_id    TEXT,
                    case_count INTEGER,
                    max_uses   INTEGER,
                    uses       INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promo_activations (
                    code       TEXT NOT NULL COLLATE NOCASE,
                    user_id    INTEGER NOT NULL,
                    activated_at REAL NOT NULL,
                    PRIMARY KEY (code, user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS case_keys (
                    user_id  INTEGER NOT NULL,
                    case_id  TEXT NOT NULL,
                    amount   INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, case_id)
                )
            """)
            async with db.execute("PRAGMA table_info(promo_codes)") as cur:
                promo_columns = [row[1] for row in await cur.fetchall()]
            if "reward_type" not in promo_columns:
                await db.execute("ALTER TABLE promo_codes ADD COLUMN reward_type TEXT NOT NULL DEFAULT 'coins'")
            if "case_id" not in promo_columns:
                await db.execute("ALTER TABLE promo_codes ADD COLUMN case_id TEXT")
            if "case_count" not in promo_columns:
                await db.execute("ALTER TABLE promo_codes ADD COLUMN case_count INTEGER")
            await db.commit()

    # --------------------------------------------------
    # BAN
    # --------------------------------------------------

    async def ban_user(self, user_id: int, reason: str = "") -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO banned_users (user_id, reason, banned_at) VALUES (?, ?, ?)",
                (user_id, reason, time.time())
            )
            await db.commit()

    async def unban_user(self, user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
            await db.commit()

    async def is_banned(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)
            ) as cur:
                return await cur.fetchone() is not None

    async def get_ban_list(self) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT u.user_id, COALESCE(s.user_name, CAST(u.user_id AS TEXT)), u.reason "
                "FROM banned_users u "
                "LEFT JOIN user_stats s ON s.user_id = u.user_id AND s.chat_id = ?",
                (MAIN_CHAT_ID,)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # PENDING GIFTS
    # --------------------------------------------------

    async def add_pending_gift(self, user_id: int, user_name: str, gift_id: str, reason: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO pending_gifts (user_id, user_name, gift_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, user_name, gift_id, reason, time.time())
            )
            await db.commit()

    async def get_pending_gifts(self) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT id, user_id, user_name, gift_id, reason, created_at FROM pending_gifts ORDER BY created_at ASC"
            ) as cur:
                return await cur.fetchall()

    async def remove_pending_gift(self, gift_db_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM pending_gifts WHERE id=?", (gift_db_id,))
            await db.commit()

    # --------------------------------------------------
    # PREMIUM ORDERS
    # --------------------------------------------------

    async def add_premium_order(self, user_id: int, user_name: str, cost: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "INSERT INTO premium_orders (user_id, user_name, cost, created_at) VALUES (?, ?, ?, ?)",
                (user_id, user_name, cost, time.time()),
            )
            await db.commit()
            return cursor.lastrowid

    async def get_premium_orders(self) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT id, user_id, user_name, cost, created_at FROM premium_orders ORDER BY created_at ASC"
            ) as cur:
                return await cur.fetchall()

    async def remove_premium_order(self, order_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM premium_orders WHERE id=?", (order_id,))
            await db.commit()

    async def credit_star_coin_purchase(
        self, payment_charge_id: str, user_id: int, dc_amount: int, star_amount: int
    ) -> tuple[bool, int]:
        """Зачисляет покупку один раз; повторный платёж Telegram не дублирует DC."""
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "INSERT OR IGNORE INTO star_coin_purchases "
                    "(payment_charge_id, user_id, dc_amount, star_amount, created_at) VALUES (?, ?, ?, ?, ?)",
                    (payment_charge_id, user_id, dc_amount, star_amount, time.time()),
                )
                if cursor.rowcount != 1:
                    async with db.execute("SELECT balance FROM coins WHERE user_id=?", (user_id,)) as cur:
                        row = await cur.fetchone()
                    await db.rollback()
                    return False, row[0] if row else COINS_START
                await db.execute(
                    "INSERT INTO coins (user_id, balance) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?",
                    (user_id, COINS_START + dc_amount, dc_amount),
                )
                async with db.execute("SELECT balance FROM coins WHERE user_id=?", (user_id,)) as cur:
                    row = await cur.fetchone()
                await db.commit()
                return True, row[0]
            except Exception:
                await db.rollback()
                raise

    # --------------------------------------------------
    # USER STATS
    # --------------------------------------------------

    async def get_user(self, user_id: int, chat_id: int) -> tuple:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT chance, msg_count, last_bonus FROM user_stats WHERE user_id=? AND chat_id=?",
                (user_id, chat_id)
            ) as cur:
                row = await cur.fetchone()
        return row if row else (START_CHANCE, 0, 0.0)

    async def update_user(self, user_id: int, chat_id: int, user_name: str, chance: float, msg_count: int, last_bonus: float) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO user_stats (user_id, chat_id, user_name, chance, msg_count, last_bonus)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET
                    user_name  = excluded.user_name,
                    chance     = excluded.chance,
                    msg_count  = excluded.msg_count,
                    last_bonus = excluded.last_bonus
            """, (user_id, chat_id, user_name, chance, msg_count, last_bonus))
            await db.commit()

    async def get_user_name(self, user_id: int) -> str:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name FROM user_stats WHERE user_id=? AND chat_id=?",
                (user_id, MAIN_CHAT_ID)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else str(user_id)

    async def set_username(
        self,
        user_id: int,
        username: str | None,
        chat_id: int = MAIN_CHAT_ID,
        user_name: str | None = None,
    ) -> None:
        normalized = username.lower().lstrip("@") if username else None
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO user_stats (user_id, chat_id, user_name, username)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET
                    username = excluded.username,
                    user_name = COALESCE(excluded.user_name, user_stats.user_name)
                """,
                (user_id, chat_id, user_name or str(user_id), normalized)
            )
            await db.commit()

    async def find_user_by_username(self, username: str):
        username = username.lower().lstrip("@")
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_id, user_name FROM user_stats WHERE username=? AND chat_id=? LIMIT 1",
                (username, MAIN_CHAT_ID)
            ) as cur:
                return await cur.fetchone()

    async def transfer_coins(self, sender_id: int, recipient_id: int, amount: int):
        if amount <= 0 or sender_id == recipient_id:
            return False, None, None

        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                # Создаём баланс с начальным количеством, если пользователь ещё не получал монеты.
                await db.execute(
                    "INSERT OR IGNORE INTO coins (user_id, balance) VALUES (?, ?)",
                    (sender_id, COINS_START)
                )
                await db.execute(
                    "INSERT OR IGNORE INTO coins (user_id, balance) VALUES (?, ?)",
                    (recipient_id, COINS_START)
                )

                cur = await db.execute(
                    "UPDATE coins SET balance=balance-? WHERE user_id=? AND balance>=?",
                    (amount, sender_id, amount)
                )
                if cur.rowcount != 1:
                    await db.rollback()
                    return False, None, None

                await db.execute(
                    "UPDATE coins SET balance=balance+? WHERE user_id=?",
                    (amount, recipient_id)
                )
                await db.commit()

                async with db.execute("SELECT balance FROM coins WHERE user_id=?", (sender_id,)) as cur:
                    sender_row = await cur.fetchone()
                async with db.execute("SELECT balance FROM coins WHERE user_id=?", (recipient_id,)) as cur:
                    recipient_row = await cur.fetchone()
                return True, sender_row[0], recipient_row[0]
            except Exception:
                await db.rollback()
                raise

    async def get_top(self, chat_id: int, limit: int = 5) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name, chance, msg_count FROM user_stats WHERE chat_id=? ORDER BY chance DESC LIMIT ?",
                (chat_id, limit)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # INVITE LINKS
    # --------------------------------------------------

    async def get_invite_link(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT invite_link FROM invite_links WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else None

    async def save_invite_link(self, user_id: int, invite_link: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO invite_links (user_id, invite_link) VALUES (?, ?)",
                (user_id, invite_link)
            )
            await db.commit()

    async def get_owner_by_link(self, invite_link: str):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_id FROM invite_links WHERE invite_link=?", (invite_link,)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else None

    # --------------------------------------------------
    # REFERRALS
    # --------------------------------------------------

    async def add_referral(self, invited_user_id: int, inviter_user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO referrals (invited_user_id, inviter_user_id) VALUES (?, ?)",
                (invited_user_id, inviter_user_id)
            )
            await db.commit()

    async def get_referral(self, invited_user_id: int):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT inviter_user_id, valid, msg_count FROM referrals WHERE invited_user_id=?",
                (invited_user_id,)
            ) as cur:
                return await cur.fetchone()

    async def increment_ref_messages(self, invited_user_id: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE referrals SET msg_count = msg_count + 1 WHERE invited_user_id=? AND valid=0",
                (invited_user_id,)
            )
            await db.commit()
            async with db.execute(
                "SELECT msg_count FROM referrals WHERE invited_user_id=?", (invited_user_id,)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else 0

    async def validate_referral(self, invited_user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE referrals SET valid=1 WHERE invited_user_id=?", (invited_user_id,))
            await db.commit()

    async def count_valid_refs(self, inviter_user_id: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM referrals WHERE inviter_user_id=? AND valid=1", (inviter_user_id,)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else 0

    async def is_already_referred(self, invited_user_id: int) -> bool:
        return (await self.get_referral(invited_user_id)) is not None

    # --------------------------------------------------
    # WINS
    # --------------------------------------------------

    async def add_win(self, user_id: int, chat_id: int, user_name: str, chance: float) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO wins (user_id, chat_id, user_name, chance, won_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, chat_id, user_name, chance, time.time())
            )
            await db.commit()

    async def get_wins_count(self, user_id: int, chat_id: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM wins WHERE user_id=? AND chat_id=?", (user_id, chat_id)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else 0

    async def get_wins_top(self, chat_id: int, limit: int = 10) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name, COUNT(*) as cnt FROM wins WHERE chat_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?",
                (chat_id, limit)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # REF TOP
    # --------------------------------------------------

    async def get_refs_top(self, limit: int = 10) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT r.inviter_user_id, "
                "COALESCE(u.user_name, CAST(r.inviter_user_id AS TEXT)), "
                "COUNT(*) as cnt "
                "FROM referrals r "
                "LEFT JOIN user_stats u ON u.user_id = r.inviter_user_id AND u.chat_id = ? "
                "WHERE r.valid = 1 "
                "GROUP BY r.inviter_user_id ORDER BY cnt DESC LIMIT ?",
                (MAIN_CHAT_ID, limit)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # VIP
    # --------------------------------------------------

    async def set_vip(self, user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR IGNORE INTO vip_users (user_id) VALUES (?)", (user_id,))
            await db.commit()

    async def remove_vip(self, user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM vip_users WHERE user_id=?", (user_id,))
            await db.commit()

    async def is_vip(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT 1 FROM vip_users WHERE user_id=?", (user_id,)) as cur:
                return await cur.fetchone() is not None

    # --------------------------------------------------
    # DAILY STATS
    # --------------------------------------------------

    async def increment_daily(self, user_id: int, chat_id: int, user_name: str) -> None:
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO daily_stats (user_id, chat_id, user_name, date, msg_count)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(user_id, chat_id, date) DO UPDATE SET
                    user_name = excluded.user_name,
                    msg_count = msg_count + 1
            """, (user_id, chat_id, user_name, today))
            await db.commit()

    async def get_daily_top(self, chat_id: int, limit: int = 10) -> list:
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name, msg_count FROM daily_stats "
                "WHERE chat_id=? AND date=? ORDER BY msg_count DESC LIMIT ?",
                (chat_id, today, limit)
            ) as cur:
                return await cur.fetchall()

    async def clear_old_daily(self) -> None:
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM daily_stats WHERE date < ?", (today,))
            await db.commit()

    async def add_day_messages(self, user_id: int, chat_id: int, user_name: str, amount: int):
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO daily_stats (user_id, chat_id, user_name, date, msg_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id, date)
                DO UPDATE SET msg_count = msg_count + ?, user_name = excluded.user_name
            """, (user_id, chat_id, user_name, today, amount, amount))
            await db.commit()

    async def remove_day_messages(self, user_id: int, chat_id: int, amount: int):
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE daily_stats SET msg_count = MAX(msg_count - ?, 0)
                WHERE user_id=? AND chat_id=? AND date=?
            """, (amount, user_id, chat_id, today))
            await db.commit()

    # --------------------------------------------------
    # COINS
    # --------------------------------------------------

    async def get_coins(self, user_id: int) -> tuple:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT balance, last_coin_bonus FROM coins WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
        return row if row else (COINS_START, 0.0)

    async def add_coins(self, user_id: int, amount: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO coins (user_id, balance) VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?
            """, (user_id, COINS_START + amount, amount))
            await db.commit()
            async with db.execute("SELECT balance FROM coins WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
        return row[0] if row else COINS_START

    async def remove_coins(self, user_id: int, amount: int) -> bool:
        if amount <= 0:
            return False
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "INSERT OR IGNORE INTO coins (user_id, balance) VALUES (?, ?)",
                    (user_id, COINS_START),
                )
                cursor = await db.execute(
                    "UPDATE coins SET balance = balance - ? "
                    "WHERE user_id = ? AND balance >= ?",
                    (amount, user_id, amount),
                )
                if cursor.rowcount != 1:
                    await db.rollback()
                    return False
                await db.commit()
                return True
            except Exception:
                await db.rollback()
                raise

    async def set_coin_bonus_time(self, user_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO coins (user_id, last_coin_bonus) VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_coin_bonus = ?
            """, (user_id, time.time(), time.time()))
            await db.commit()

    async def get_coins_top(self, limit: int = 10) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("""
                SELECT c.user_id, COALESCE(u.user_name, CAST(c.user_id AS TEXT)), c.balance
                FROM coins c
                LEFT JOIN user_stats u ON u.user_id = c.user_id AND u.chat_id = ?
                ORDER BY c.balance DESC LIMIT ?
            """, (MAIN_CHAT_ID, limit)) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # PROMO CODES
    # --------------------------------------------------

    async def create_promo(self, code: str, reward: int, max_uses: int | None) -> bool:
        if reward <= 0 or (max_uses is not None and max_uses <= 0):
            return False
        async with aiosqlite.connect(self.path) as db:
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, reward, max_uses, created_at) VALUES (?, ?, ?, ?)",
                    (code, reward, max_uses, time.time()),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def create_case_promo(self, code: str, case_id: str, case_count: int, max_uses: int | None) -> bool:
        if case_id not in CASES or case_count <= 0 or (max_uses is not None and max_uses <= 0):
            return False
        async with aiosqlite.connect(self.path) as db:
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, reward, reward_type, case_id, case_count, max_uses, created_at) "
                    "VALUES (?, 0, 'case', ?, ?, ?, ?)",
                    (code, case_id, case_count, max_uses, time.time()),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def delete_promo(self, code: str) -> bool:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute("DELETE FROM promo_codes WHERE code=?", (code,))
            await db.commit()
        return cursor.rowcount == 1

    async def get_promos(self) -> list:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT code, reward, reward_type, case_id, case_count, max_uses, uses "
                "FROM promo_codes ORDER BY created_at DESC"
            ) as cur:
                return await cur.fetchall()

    async def get_case_keys(self, user_id: int, case_id: str) -> int:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT amount FROM case_keys WHERE user_id=? AND case_id=?", (user_id, case_id)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else 0

    async def open_case(self, user_id: int, case_id: str, price: int) -> str:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "UPDATE case_keys SET amount = amount - 1 "
                    "WHERE user_id=? AND case_id=? AND amount > 0",
                    (user_id, case_id),
                )
                if cursor.rowcount == 1:
                    await db.commit()
                    return "key"

                await db.execute(
                    "INSERT OR IGNORE INTO coins (user_id, balance) VALUES (?, ?)",
                    (user_id, COINS_START),
                )
                cursor = await db.execute(
                    "UPDATE coins SET balance = balance - ? WHERE user_id=? AND balance >= ?",
                    (price, user_id, price),
                )
                if cursor.rowcount != 1:
                    await db.rollback()
                    return "insufficient"
                await db.commit()
                return "coins"
            except Exception:
                await db.rollback()
                raise

    async def redeem_promo(self, code: str, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute(
                    "SELECT reward, reward_type, case_id, case_count, max_uses, uses "
                    "FROM promo_codes WHERE code=?", (code,)
                ) as cur:
                    promo = await cur.fetchone()
                if not promo:
                    await db.rollback()
                    return "not_found", None, None, None, None, None, None

                reward, reward_type, case_id, case_count, max_uses, uses = promo
                async with db.execute(
                    "SELECT 1 FROM promo_activations WHERE code=? AND user_id=?", (code, user_id)
                ) as cur:
                    already_used = await cur.fetchone()
                if already_used:
                    await db.rollback()
                    return "already_used", None, None, None, None, None, None
                if max_uses is not None and uses >= max_uses:
                    await db.rollback()
                    return "limit_reached", None, None, None, None, None, None

                await db.execute(
                    "INSERT INTO promo_activations (code, user_id, activated_at) VALUES (?, ?, ?)",
                    (code, user_id, time.time()),
                )
                if reward_type == "case":
                    await db.execute(
                        "INSERT INTO case_keys (user_id, case_id, amount) VALUES (?, ?, ?) "
                        "ON CONFLICT(user_id, case_id) DO UPDATE SET amount = amount + ?",
                        (user_id, case_id, case_count, case_count),
                    )
                else:
                    await db.execute(
                        "INSERT INTO coins (user_id, balance) VALUES (?, ?) "
                        "ON CONFLICT(user_id) DO UPDATE SET balance = balance + ?",
                        (user_id, COINS_START + reward, reward),
                    )
                await db.execute("UPDATE promo_codes SET uses = uses + 1 WHERE code=?", (code,))
                await db.commit()
                return "success", reward, reward_type, case_id, case_count, uses + 1, max_uses
            except Exception:
                await db.rollback()
                raise


db = Database(os.getenv("DB_PATH", "activity.db"))

# =========================
# HELPERS
# =========================

def display_name(user) -> str:
    return user.first_name


async def send_log(bot: Bot, text: str) -> None:
    try:
        await bot.send_message(LOG_CHAT_ID, text)
    except Exception as e:
        logger.warning("send_log failed: %s", e)

async def send_game_log(bot: Bot, text: str) -> None:
    if not GAME_LOG_CHAT_ID:
        return
    try:
        await bot.send_message(GAME_LOG_CHAT_ID, text)
    except Exception as e:
        logger.warning("send_game_log failed: %s", e)

async def publish_promo(bot: Bot, text: str) -> bool:
    try:
        await bot.send_message(PROMO_CHANNEL_ID, text)
        return True
    except Exception as e:
        logger.warning("publish_promo failed: %s", e)
        return False


async def reward_inviter(bot: Bot, inviter_id: int) -> None:
    inv_chance, inv_msgs, inv_bonus = await db.get_user(inviter_id, MAIN_CHAT_ID)
    new_chance = min(inv_chance + REF_BONUS, MAX_CHANCE)
    inv_name   = await db.get_user_name(inviter_id)
    await db.update_user(inviter_id, MAIN_CHAT_ID, inv_name, new_chance, inv_msgs, inv_bonus)
    valid_refs = await db.count_valid_refs(inviter_id)
    await send_log(bot,
        f"✅ Валидный реферал\n\n"
        f"👤 Пригласил: {inv_name} ({inviter_id})\n"
        f"👥 Всего валидных: {valid_refs}\n"
        f"📈 Новый шанс: {new_chance:.3f}%"
    )
    try:
        await bot.send_message(inviter_id,
            f"🎉 Твой реферал стал активным!\n"
            f"+{REF_BONUS}% к шансу\n"
            f"📈 Твой шанс: {new_chance:.3f}%"
        )
    except Exception as e:
        logger.warning("Notify inviter %s failed: %s", inviter_id, e)

    if valid_refs in REF_REWARDS:
        reward   = REF_REWARDS[valid_refs]
        gift_ids = REF_GIFT_IDS.get(valid_refs, [])
        gift_id  = random.choice(gift_ids) if gift_ids else None
        await send_log(bot,
            f"🎁 Реферальная награда\n\n"
            f"👤 {inv_name} ({inviter_id})\n"
            f"🏆 Награда: {reward}\n"
            f"👥 Рефералов: {valid_refs}"
        )
        await bot.send_message(ADMIN_ID,
            f"🎁 Реферальная награда\n\n"
            f"👤 {inv_name} ({inviter_id})\n"
            f"🏆 Награда: {reward}\n"
            f"👥 Рефералов: {valid_refs}"
        )
        if gift_id:
            try:
                star_balance = await bot.get_my_star_balance()
                cost = int(reward.replace("⭐", "").strip())
                if star_balance.amount < cost:
                    await db.add_pending_gift(inviter_id, inv_name, gift_id, f"реф. награда {reward}")
                    await bot.send_message(ADMIN_ID,
                        f"⚠️ Недостаточно звёзд!\n\n💫 Баланс: {star_balance.amount}⭐\n"
                        f"👤 {inv_name} ({inviter_id})\n🏆 {reward}\nДобавлен в /pending"
                    )
                else:
                    await bot.send_gift(user_id=inviter_id, gift_id=gift_id)
                    await send_log(bot, f"🎁 Реф. подарок отправлен\n\n{inv_name} ({inviter_id})\n{reward}")
            except Exception as e:
                logger.warning("send ref gift failed: %s", e)
                await db.add_pending_gift(inviter_id, inv_name, gift_id, f"реф. награда {reward} — ошибка: {e}")
                await bot.send_message(ADMIN_ID,
                    f"❌ Не удалось отправить реф. подарок\n\n"
                    f"👤 {inv_name} ({inviter_id})\n🏆 {reward}\n📛 {e}\nДобавлен в /pending"
                )


async def send_gift_safe(bot: Bot, user_id: int, user_name: str, gift_id: str, reason: str) -> None:
    try:
        star_balance = await bot.get_my_star_balance()
        cost_map = {
            EXCHANGE_GIFT_15:  15,
            EXCHANGE_GIFT_25:  25,
            EXCHANGE_GIFT_50:  50,
            EXCHANGE_GIFT_100: 100,
        }
        cost = 15
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
        await send_log(bot, f"🎁 Подарок отправлен\n\n{user_name} ({user_id})\n📝 {reason}\n💫 Баланс: {star_balance.amount}⭐")
    except Exception as e:
        await db.add_pending_gift(user_id, user_name, gift_id, f"{reason} — ошибка: {e}")
        await bot.send_message(ADMIN_ID, f"❌ Ошибка отправки подарка\n\n👤 {user_name} ({user_id})\n📝 {reason}\n📛 {e}\nДобавлен в /pending")


# =========================
# CASINO TIMEOUT CHECKER
# =========================

async def casino_timeout_checker(bot: Bot) -> None:
    while True:
        await asyncio.sleep(30)
        now = time.time()
        expired = [uid for uid, g in active_games.items() if g["expires"] < now]
        for uid in expired:
            game = active_games.pop(uid)
            bet  = game["bet"]
            chat_id = game.get("chat_id", MAIN_CHAT_ID)
            await db.add_coins(uid, bet)
            try:
                await bot.send_message(
                    chat_id,
                    f"⏰ Время вышло! Ставка {bet} D-COINS возвращена на твой баланс.",
                )
            except Exception:
                pass


# =========================
# ROUTER
# =========================

router    = Router()
cooldowns: TTLCache = TTLCache(maxsize=50_000, ttl=COOLDOWN_SECONDS)
casino_bet_cooldowns: TTLCache = TTLCache(maxsize=50_000, ttl=CASINO_BET_COOLDOWN)
case_open_cooldowns: TTLCache = TTLCache(maxsize=50_000, ttl=CASE_OPEN_COOLDOWN)

PLAIN_COMMANDS = {
    "start", "help", "ref", "refstats", "say", "vip", "unvip", "viplist",
    "ban", "unban", "banlist", "addmsgs", "removemsgs", "addday", "removeday",
    "addcoins", "removecoins", "createpromo", "deletepromo", "createcasepromo",
    "promos", "addrefs", "removerefs", "balance", "popolnit", "sendgift",
    "pending", "deliver", "deletepending", "premiumorders", "premiumdone", "premiumrefund",
    "stats", "top", "winstop", "reftop", "cointop",
    "coins", "promo", "transfer", "daytop", "bonus", "cases", "slots",
    "roulette", "dice", "mines", "exchange",
}

RUSSIAN_COMMANDS = {
    "старт": "start", "начать": "start", "помощь": "help",
    "реф": "ref", "реферал": "ref", "рефы": "refstats",
    "кейсы": "cases", "баланс": "coins", "монеты": "coins",
    "обмен": "exchange", "бонус": "bonus", "стата": "stats",
    "статистика": "stats", "топ": "top", "победители": "winstop",
    "топреф": "reftop", "топкоинов": "cointop", "дневнойтоп": "daytop",
    "промо": "promo", "перевод": "transfer", "слоты": "slots",
    "рулетка": "roulette", "кубик": "dice", "мины": "mines",
    "удалитьзаявку": "deletepending",
}

def parse_plain_command(text: str | None):
    if not text or text.startswith("/"):
        return None
    parts = text.strip().split()
    if not parts:
        return None
    command = RUSSIAN_COMMANDS.get(parts[0].lower(), parts[0].lower())
    if command not in PLAIN_COMMANDS:
        return None
    if command == "roulette" and len(parts) > 1:
        colors = {"красное": "red", "красный": "red", "черное": "black", "чёрное": "black", "черный": "black", "чёрный": "black"}
        parts[1] = colors.get(parts[1].lower(), parts[1])
    return command, parts[1:]

def is_plain_command(message: Message) -> bool:
    return parse_plain_command(message.text) is not None

def start_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="ref")],
        [InlineKeyboardButton(text="📊 Реферальная статистика", callback_data="refstats")],
        [InlineKeyboardButton(text="📦 Кейсы", callback_data="cases")],
        [InlineKeyboardButton(text="⭐ Купить D-COINS", callback_data="buy_dc_menu")],
        [InlineKeyboardButton(text="❓ Как играть", callback_data="help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def subscription_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=REQUIRED_CHANNEL_URL)],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")],
    ])

async def is_channel_subscriber(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        status = getattr(member.status, "value", member.status)
        return status not in {"left", "kicked"}
    except Exception as e:
        logger.warning("Subscription check failed for %s: %s", user_id, e)
        return False

async def send_subscription_prompt(message: Message) -> None:
    await message.answer(
        "📢 Чтобы пользоваться ботом, подпишись на наш канал.",
        reply_markup=subscription_keyboard(),
    )

def exchange_keyboard(balance: int):
    buttons = [
        [InlineKeyboardButton(text=f"📈 {EXCHANGE_CHANCE:,} DC → +1% шанса".replace(",", " "), callback_data="exch_chance")],
        [InlineKeyboardButton(text=f"🎁 {EXCHANGE_GIFT_15:,} DC → подарок 15⭐".replace(",", " "), callback_data="exch_gift_15")],
        [InlineKeyboardButton(text=f"🎁 {EXCHANGE_GIFT_25:,} DC → подарок 25⭐".replace(",", " "), callback_data="exch_gift_25")],
        [InlineKeyboardButton(text=f"🎁 {EXCHANGE_GIFT_50:,} DC → подарок 50⭐".replace(",", " "), callback_data="exch_gift_50")],
        [InlineKeyboardButton(text=f"🎁 {EXCHANGE_GIFT_100:,} DC → подарок 100⭐".replace(",", " "), callback_data="exch_gift_100")],
        [InlineKeyboardButton(text=f"💎 {EXCHANGE_PREMIUM_1_MONTH:,} DC → Premium на месяц".replace(",", " "), callback_data="exch_premium_1m")],
        [InlineKeyboardButton(text="⭐ Купить DC за звёзды", callback_data="buy_dc_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def buy_dc_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for dc_amount, star_amount in STAR_DC_PACKAGES.items():
        buttons.append([
            InlineKeyboardButton(
                text=f"🪙 {dc_amount:,} DC — {star_amount}⭐".replace(",", " "),
                callback_data=f"buy_dc_{dc_amount}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="◀️ К обмену", callback_data="buy_dc_back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def cases_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 KARAPUZ — 1 000 DC", callback_data="case_view_karapuz")],
        [InlineKeyboardButton(text="🩸 BLOOD — 5 000 DC", callback_data="case_view_blood")],
        [InlineKeyboardButton(text="🐆 PANTERA — 10 000 DC", callback_data="case_view_pantera")],
        [InlineKeyboardButton(text="🕷 SPIDER MAN — 7 500 DC", callback_data="case_view_spider_man")],
    ])

def case_detail_keyboard(case_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть кейс", callback_data=f"case_open_{case_id}")],
        [InlineKeyboardButton(text="⬅️ Все кейсы", callback_data="cases")],
    ])

# =========================
# PRIVATE — /start
# =========================

@router.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message, bot: Bot) -> None:
    if await db.is_banned(message.from_user.id):
        await message.answer(BAN_MESSAGE)
        return
    if not await is_channel_subscriber(bot, message.from_user.id):
        await send_subscription_prompt(message)
        return
    await message.answer(
        "👋 Добро пожаловать!\n\nВыберите действие:",
        reply_markup=start_keyboard()
    )

@router.callback_query(F.data == "check_subscription")
async def check_subscription(callback: CallbackQuery, bot: Bot) -> None:
    if not await is_channel_subscriber(bot, callback.from_user.id):
        await callback.answer("❌ Подписка пока не найдена.", show_alert=True)
        return
    await callback.message.edit_text(
        "👋 Добро пожаловать!\n\nВыберите действие:",
        reply_markup=start_keyboard(),
    )
    await callback.answer("✅ Подписка подтверждена")

@router.callback_query(F.data == "ref")
async def ref_callback(callback: CallbackQuery, bot: Bot):
    await cmd_ref(callback.message, bot)
    await callback.answer()

@router.callback_query(F.data == "refstats")
async def refstats_callback(callback: CallbackQuery):
    await cmd_refstats(callback.message)
    await callback.answer()

async def send_help(message: Message) -> None:
    await message.answer(
        "📖 Как играть\n\n"
        "1️⃣ Подпишись на канал и нажми «Проверить подписку».\n"
        "2️⃣ Пиши сообщения в основной группе — за них начисляются DC.\n"
        "3️⃣ Забирай ежедневный бонус: бонус.\n\n"
        "📦 Кейсы\n"
        "Напиши кейсы, выбери кейс кнопкой и открой его за DC или ключ.\n\n"
        "🎰 Игры — только в личке с ботом\n"
        "• слоты 50\n"
        "• рулетка красное 50\n"
        "• кубик 3 50\n"
        "• мины 2500\n"
        "В минах открывай клетки и забирай выигрыш до того, как попадёшь на бомбу.\n\n"
        "💱 Полезное\n"
        "• баланс — твои DC\n"
        "• обмен — обмен DC на шанс, подарки или Premium\n"
        "• промо КОД — активировать промокод\n"
        "• перевод @username сумма — отправить DC игроку\n"
        "• реферал — получить ссылку\n"
        "• стата — статистика в основном чате\n\n"
        "Команды пишутся без /"
    )

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if message.chat.type != "private" and message.chat.id != MAIN_CHAT_ID:
        return
    if message.from_user and await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    await send_help(message)

@router.callback_query(F.data == "help")
async def help_callback(callback: CallbackQuery) -> None:
    await send_help(callback.message)
    await callback.answer()

# =========================
# PRIVATE — /ref
# =========================

@router.message(Command("ref"), F.chat.type == "private")
async def cmd_ref(message: Message, bot: Bot) -> None:
    user_id  = message.from_user.id
    existing = await db.get_invite_link(user_id)
    if existing:
        await message.answer(
            f"👥 Твоя реферальная ссылка:\n\n{existing}\n\n"
            f"Поделись ей — за каждого активного реферала получишь +{REF_BONUS}% к шансу!"
        )
        return
    try:
        link = await bot.create_chat_invite_link(chat_id=MAIN_CHAT_ID, name=f"ref_{user_id}", creates_join_request=False)
    except Exception as e:
        logger.error("create_chat_invite_link error for %s: %s", user_id, e)
        await message.answer("❌ Не удалось создать ссылку. Попробуй позже.")
        return
    await db.save_invite_link(user_id, link.invite_link)
    await message.answer(
        f"👥 Твоя реферальная ссылка:\n\n{link.invite_link}\n\n"
        f"Поделись ей — за каждого активного реферала получишь +{REF_BONUS}% к шансу!"
    )

# =========================
# PRIVATE — /refstats
# =========================

@router.message(Command("refstats"), F.chat.type == "private")
async def cmd_refstats(message: Message) -> None:
    user_id    = message.from_user.id
    valid_refs = await db.count_valid_refs(user_id)
    chance, _, _ = await db.get_user(user_id, MAIN_CHAT_ID)
    next_reward = "🏅 Максимальная награда получена"
    for level, reward in REF_REWARDS.items():
        if valid_refs < level:
            next_reward = f"{level - valid_refs} чел. → {reward}"
            break
    await message.answer(
        f"📊 Реферальная статистика\n\n"
        f"👥 Валидных рефералов: {valid_refs}\n"
        f"🎁 Следующая награда: {next_reward}\n\n"
        f"📈 Твой шанс: {chance:.3f}%"
    )

# =========================
# PRIVATE — ADMIN COMMANDS
# =========================

@router.message(Command("say"), F.chat.type == "private")
async def cmd_say(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("Использование: /say текст сообщения")
        return
    try:
        await bot.send_message(MAIN_CHAT_ID, args[1].strip())
        await message.answer("✅ Сообщение отправлено в чат.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить.\n\n📛 {e}")

@router.message(Command("vip"), F.chat.type == "private")
async def cmd_vip(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /vip user_id")
        return
    try:
        user_id = int(args[1].lstrip("@"))
    except ValueError:
        await message.answer("❌ Укажи числовой ID.")
        return
    await db.set_vip(user_id)
    await message.answer(f"✅ Пользователь {user_id} назначен VIP.")

@router.message(Command("unvip"), F.chat.type == "private")
async def cmd_unvip(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unvip user_id")
        return
    try:
        user_id = int(args[1].lstrip("@"))
    except ValueError:
        await message.answer("❌ Укажи числовой ID.")
        return
    await db.remove_vip(user_id)
    await message.answer(f"✅ VIP статус снят с {user_id}.")

@router.message(Command("viplist"), F.chat.type == "private")
async def cmd_viplist(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(db.path) as conn:
        async with conn.execute(
            "SELECT v.user_id, COALESCE(u.user_name, CAST(v.user_id AS TEXT)), COALESCE(u.msg_count, 0) "
            "FROM vip_users v LEFT JOIN user_stats u ON u.user_id = v.user_id AND u.chat_id = ?",
            (MAIN_CHAT_ID,)
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await message.answer("👑 VIP пользователей нет.")
        return
    text = "👑 Список VIP:\n\n"
    for uid, name, msg_count in rows:
        text += f"• {name} ({uid}) — {msg_count} сообщ.\n"
    await message.answer(text)

@router.message(Command("ban"), F.chat.type == "private")
async def cmd_ban(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer("Использование: /ban user_id [причина]")
        return
    try:
        user_id = int(args[1])
    except ValueError:
        await message.answer("❌ Укажи числовой ID.")
        return
    reason = args[2] if len(args) > 2 else ""
    await db.ban_user(user_id, reason)
    await message.answer(f"✅ Пользователь {user_id} заблокирован.")

@router.message(Command("unban"), F.chat.type == "private")
async def cmd_unban(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unban user_id")
        return
    try:
        user_id = int(args[1])
    except ValueError:
        await message.answer("❌ Укажи числовой ID.")
        return
    await db.unban_user(user_id)
    await message.answer(f"✅ Пользователь {user_id} разблокирован.")

@router.message(Command("banlist"), F.chat.type == "private")
async def cmd_banlist(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    ban_list = await db.get_ban_list()
    if not ban_list:
        await message.answer("✅ Список заблокированных пуст.")
        return
    text = "🚫 Заблокированные:\n\n"
    for uid, name, reason in ban_list:
        text += f"• {name} ({uid})"
        if reason:
            text += f" — {reason}"
        text += "\n"
    await message.answer(text)

@router.message(Command("addmsgs"), F.chat.type == "private")
async def cmd_addmsgs(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /addmsgs user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    chance, msg_count, last_bonus = await db.get_user(user_id, MAIN_CHAT_ID)
    inv_name  = await db.get_user_name(user_id)
    new_count  = msg_count + amount
    new_chance = min(round(chance + STEP * amount, 3), MAX_CHANCE)
    await db.update_user(user_id, MAIN_CHAT_ID, inv_name, new_chance, new_count, last_bonus)
    await message.answer(
        f"✅ Добавлено {amount} сообщений\n"
        f"👤 {inv_name} ({user_id})\n"
        f"💬 Сообщений: {new_count}\n"
        f"📈 Шанс: {new_chance:.3f}%"
    )

@router.message(Command("removemsgs"), F.chat.type == "private")
async def cmd_removemsgs(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /removemsgs user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    chance, msg_count, last_bonus = await db.get_user(user_id, MAIN_CHAT_ID)
    inv_name  = await db.get_user_name(user_id)
    new_count  = max(0, msg_count - amount)
    new_chance = max(round(chance - STEP * amount, 3), START_CHANCE)
    await db.update_user(user_id, MAIN_CHAT_ID, inv_name, new_chance, new_count, last_bonus)
    await message.answer(
        f"✅ Убрано {amount} сообщений\n"
        f"👤 {inv_name} ({user_id})\n"
        f"💬 Сообщений: {new_count}\n"
        f"📈 Шанс: {new_chance:.3f}%"
    )

@router.message(Command("addday"), F.chat.type == "private")
async def cmd_addday(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /addday user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи ID и количество.")
        return
    name = await db.get_user_name(user_id)
    await db.add_day_messages(user_id, MAIN_CHAT_ID, name, amount)
    await message.answer(f"✅ Добавлено {amount} сообщений в daytop\n👤 {name} ({user_id})")

@router.message(Command("removeday"), F.chat.type == "private")
async def cmd_removeday(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /removeday user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи ID и количество.")
        return
    await db.remove_day_messages(user_id, MAIN_CHAT_ID, amount)
    await message.answer(f"✅ Убрано {amount} сообщений из daytop\n👤 {user_id}")

@router.message(Command("addcoins"), F.chat.type == "private")
async def cmd_addcoins(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /addcoins user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    if amount <= 0:
        await message.answer("❌ Количество должно быть положительным.")
        return
    new_balance = await db.add_coins(user_id, amount)
    name = await db.get_user_name(user_id)
    await message.answer(f"✅ Добавлено {amount} DC\n👤 {name} ({user_id})\n🪙 Баланс: {new_balance} D-COINS")

@router.message(Command("removecoins"), F.chat.type == "private")
async def cmd_removecoins(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /removecoins user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    if amount <= 0:
        await message.answer("❌ Количество должно быть положительным.")
        return
    ok = await db.remove_coins(user_id, amount)
    name = await db.get_user_name(user_id)
    balance, _ = await db.get_coins(user_id)
    if ok:
        await message.answer(f"✅ Убрано {amount} DC\n👤 {name} ({user_id})\n🪙 Баланс: {balance} D-COINS")
    else:
        await message.answer(f"❌ Недостаточно монет\n👤 {name} ({user_id})\n🪙 Баланс: {balance} D-COINS")

@router.message(Command("createpromo"), F.chat.type == "private")
async def cmd_createpromo(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) not in (3, 4):
        await message.answer("Использование: /createpromo КОД награда_DC [лимит]\nЛимит не указывай для безлимитного промокода.")
        return
    code = args[1].upper()
    if not 3 <= len(code) <= 32 or not all(char.isalnum() or char in "_-" for char in code):
        await message.answer("❌ Код: от 3 до 32 символов; разрешены латинские буквы, цифры, _ и -.")
        return
    try:
        reward = int(args[2])
        max_uses = int(args[3]) if len(args) == 4 else None
    except ValueError:
        await message.answer("❌ Награда и лимит должны быть целыми числами.")
        return
    if not await db.create_promo(code, reward, max_uses):
        await message.answer("❌ Не удалось создать промокод: проверь значения или выбери другой код.")
        return
    limit_text = str(max_uses) if max_uses is not None else "без лимита"
    await message.answer(f"✅ Промокод {code} создан.\n🎁 Награда: {reward} DC\n👥 Активаций: {limit_text}")
    if not await publish_promo(
        bot,
        f"🎁 Новый промокод!\n\n"
        f"🔑 Код: <code>{code}</code>\n"
        f"🪙 Награда: {reward:,} DC\n"
        f"👥 Активаций: {limit_text}\n\n"
        f"Активировать в боте: промо {code}".replace(",", " "),
    ):
        await message.answer("⚠️ Промокод создан, но не отправлен в канал. Проверь, что бот — администратор канала.")

@router.message(Command("deletepromo"), F.chat.type == "private")
async def cmd_deletepromo(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Использование: /deletepromo КОД")
        return
    if await db.delete_promo(args[1].upper()):
        await message.answer(f"✅ Промокод {args[1].upper()} удалён.")
    else:
        await message.answer("❌ Промокод не найден.")

@router.message(Command("createcasepromo"), F.chat.type == "private")
async def cmd_createcasepromo(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) not in (3, 4, 5):
        await message.answer("Использование: /createcasepromo КОД КЕЙС [ключей] [лимит]\nПример: /createcasepromo KARAPUZFREE karapuz 1 100")
        return
    code = args[1].upper()
    case_id = args[2].lower()
    if not 3 <= len(code) <= 32 or not all(char.isalnum() or char in "_-" for char in code):
        await message.answer("❌ Код: от 3 до 32 символов; разрешены латинские буквы, цифры, _ и -.")
        return
    try:
        case_count = int(args[3]) if len(args) >= 4 else 1
        max_uses = int(args[4]) if len(args) == 5 else None
    except ValueError:
        await message.answer("❌ Количество ключей и лимит должны быть целыми числами.")
        return
    if not await db.create_case_promo(code, case_id, case_count, max_uses):
        await message.answer("❌ Не удалось создать промокод: проверь кейс, значения или код.")
        return
    limit_text = str(max_uses) if max_uses is not None else "без лимита"
    await message.answer(f"✅ Промокод {code} создан.\n🎟 Кейс: {CASES[case_id]['title']} × {case_count}\n👥 Активаций: {limit_text}")
    if not await publish_promo(
        bot,
        f"🎁 Новый промокод!\n\n"
        f"🔑 Код: <code>{code}</code>\n"
        f"🎟 Награда: {CASES[case_id]['title']} × {case_count}\n"
        f"👥 Активаций: {limit_text}\n\n"
        f"Активировать в боте: промо {code}",
    ):
        await message.answer("⚠️ Промокод создан, но не отправлен в канал. Проверь, что бот — администратор канала.")

@router.message(Command("promos"), F.chat.type == "private")
async def cmd_promos(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    promos = await db.get_promos()
    if not promos:
        await message.answer("📭 Активных промокодов нет.")
        return
    text = "🎟 Активные промокоды:\n\n"
    for code, reward, reward_type, case_id, case_count, max_uses, uses in promos:
        limit_text = f"{uses}/{max_uses}" if max_uses is not None else f"{uses}/∞"
        reward_text = f"{CASES[case_id]['title']} × {case_count}" if reward_type == "case" else f"{reward} DC"
        text += f"{code} — {reward_text} ({limit_text})\n"
    await message.answer(text)

@router.message(Command("addrefs"), F.chat.type == "private")
async def cmd_addrefs(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /addrefs user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    prev_refs = await db.count_valid_refs(user_id)
    async with aiosqlite.connect(db.path) as conn:
        for i in range(amount):
            fake_id = -(user_id * 1000 + i)
            await conn.execute(
                "INSERT OR IGNORE INTO referrals (invited_user_id, inviter_user_id, valid) VALUES (?, ?, 1)",
                (fake_id, user_id)
            )
        await conn.commit()
    valid_refs = await db.count_valid_refs(user_id)
    inv_name   = await db.get_user_name(user_id)
    added = valid_refs - prev_refs
    new_chance = None
    if added > 0:
        chance, msg_count, last_bonus = await db.get_user(user_id, MAIN_CHAT_ID)
        new_chance = min(round(chance + REF_BONUS * added, 3), MAX_CHANCE)
        await db.update_user(user_id, MAIN_CHAT_ID, inv_name, new_chance, msg_count, last_bonus)
    await message.answer(
        f"✅ Добавлено {amount} рефералов\n"
        f"👤 {inv_name} ({user_id})\n"
        f"👥 Всего валидных: {valid_refs}"
        + (f"\n📈 Шанс: {new_chance:.3f}%" if new_chance else "")
    )
    for level, reward in REF_REWARDS.items():
        if valid_refs >= level and prev_refs < level:
            gift_ids = REF_GIFT_IDS.get(level, [])
            gift_id  = random.choice(gift_ids) if gift_ids else None
            await send_log(bot, f"🎁 Реф. награда\n\n👤 {inv_name} ({user_id})\n🏆 {reward}")
            await bot.send_message(ADMIN_ID, f"🎁 Реф. награда\n\n👤 {inv_name} ({user_id})\n🏆 {reward}")
            if gift_id:
                try:
                    await bot.send_gift(user_id=user_id, gift_id=gift_id)
                except Exception as e:
                    await db.add_pending_gift(user_id, inv_name, gift_id, f"реф. награда {reward} — ошибка: {e}")

@router.message(Command("removerefs"), F.chat.type == "private")
async def cmd_removerefs(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /removerefs user_id количество")
        return
    try:
        user_id = int(args[1])
        amount  = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи числовой ID и количество.")
        return
    async with aiosqlite.connect(db.path) as conn:
        async with conn.execute(
            "SELECT invited_user_id FROM referrals WHERE inviter_user_id=? AND invited_user_id < 0 LIMIT ?",
            (user_id, amount)
        ) as cur:
            rows = await cur.fetchall()
        for row in rows:
            await conn.execute("DELETE FROM referrals WHERE invited_user_id=?", (row[0],))
        await conn.commit()
    valid_refs = await db.count_valid_refs(user_id)
    inv_name   = await db.get_user_name(user_id)
    await message.answer(
        f"✅ Убрано {len(rows)} рефералов\n"
        f"👤 {inv_name} ({user_id})\n"
        f"👥 Осталось: {valid_refs}"
    )

@router.message(Command("balance"), F.chat.type == "private")
async def cmd_balance(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    try:
        star_balance = await bot.get_my_star_balance()
        await message.answer(f"💫 Баланс бота: {star_balance.amount} звёзд")
    except Exception as e:
        await message.answer("❌ Не удалось получить баланс.")

@router.message(Command("popolnit"), F.chat.type == "private")
async def cmd_popolnit(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    await bot.send_invoice(
        chat_id=message.from_user.id,
        title="💫 Пополнение бота",
        description=f"Пополнение баланса бота на {POPOLNIT_AMOUNT} звёзд",
        payload="admin_topup",
        currency="XTR",
        prices=[LabeledPrice(label="Звёзды", amount=POPOLNIT_AMOUNT)],
    )

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery) -> None:
    await query.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment_handler(message: Message, bot: Bot) -> None:
    payment = message.successful_payment
    if payment.invoice_payload == "admin_topup":
        await message.answer(f"✅ Оплата прошла!\n💫 Зачислено: {payment.total_amount} звёзд.")
        await send_log(bot, f"💫 Пополнение\n\n👤 Админ: {message.from_user.id}\n⭐ {payment.total_amount} звёзд")
        return

    if not payment.invoice_payload.startswith("buy_dc_"):
        return
    try:
        dc_amount = int(payment.invoice_payload.removeprefix("buy_dc_"))
    except ValueError:
        logger.warning("Unknown Stars payment payload: %s", payment.invoice_payload)
        return
    star_amount = STAR_DC_PACKAGES.get(dc_amount)
    if star_amount is None or payment.total_amount != star_amount or payment.currency != "XTR":
        logger.warning("Invalid Stars payment: payload=%s amount=%s", payment.invoice_payload, payment.total_amount)
        return
    credited, new_balance = await db.credit_star_coin_purchase(
        payment.telegram_payment_charge_id,
        message.from_user.id,
        dc_amount,
        star_amount,
    )
    if not credited:
        await message.answer("ℹ️ Эта оплата уже была зачислена ранее.")
        return
    await message.answer(
        f"✅ Оплата прошла!\n"
        f"🪙 Зачислено: {dc_amount:,} DC\n"
        f"⭐ Оплачено: {star_amount}⭐\n"
        f"🪙 Баланс: {new_balance:,} DC".replace(",", " ")
    )
    await send_log(
        bot,
        f"⭐ Покупка DC\n\n👤 {display_name(message.from_user)} ({message.from_user.id})\n"
        f"⭐ {star_amount} → 🪙 {dc_amount:,} DC".replace(",", " "),
    )

@router.message(Command("sendgift"), F.chat.type == "private")
async def cmd_sendgift(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /sendgift user_id gift_id")
        return
    try:
        user_id = int(args[1])
        gift_id = args[2]
    except ValueError:
        await message.answer("❌ Укажи числовой ID и gift_id.")
        return
    try:
        star_balance = await bot.get_my_star_balance()
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
        await send_log(bot, f"🎁 Ручная выдача\n\n👤 {user_id}\n📦 {gift_id}\n💫 {star_balance.amount}⭐")
        await message.answer(f"✅ Подарок отправлен!\n👤 {user_id}\n📦 {gift_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@router.message(Command("pending"), F.chat.type == "private")
async def cmd_pending(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    gifts = await db.get_pending_gifts()
    if not gifts:
        await message.answer("✅ Список невыданных подарков пуст!")
        return
    import datetime as dt
    star_balance = await bot.get_my_star_balance()
    text = f"📋 Невыданные подарки ({len(gifts)}):\n💫 Баланс: {star_balance.amount}⭐\n\n"
    for g_id, uid, user_name, gift_id, reason, created_at in gifts:
        date = dt.datetime.fromtimestamp(created_at).strftime("%d.%m %H:%M")
        text += (
            f"#{g_id} | {user_name} ({uid})\n📦 {gift_id}\n📝 {reason} | {date}\n"
            f"👉 deliver {g_id} — выдать\n"
            f"🗑 deletepending {g_id} — удалить\n\n"
        )
    await message.answer(text)

@router.message(Command("deliver"), F.chat.type == "private")
async def cmd_deliver(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /deliver id")
        return
    try:
        gift_db_id = int(args[1])
    except ValueError:
        await message.answer("❌ Укажи числовой ID из /pending")
        return
    gifts = await db.get_pending_gifts()
    gift  = next((g for g in gifts if g[0] == gift_db_id), None)
    if not gift:
        await message.answer("❌ Подарок не найден.")
        return
    _, user_id, user_name, gift_id, reason, _ = gift
    try:
        star_balance = await bot.get_my_star_balance()
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
        await db.remove_pending_gift(gift_db_id)
        await send_log(bot, f"🎁 Отложенный подарок выдан\n\n{user_name} ({user_id})\n💫 {star_balance.amount}⭐")
        await message.answer(f"✅ Подарок выдан!\n👤 {user_name} ({user_id})\n💫 Баланс: {star_balance.amount}⭐")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}\n\nПополни баланс через /popolnit")

@router.message(Command("deletepending"), F.chat.type == "private")
async def cmd_deletepending(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: deletepending ID")
        return
    gift_db_id = int(parts[1])
    gift = next((item for item in await db.get_pending_gifts() if item[0] == gift_db_id), None)
    if not gift:
        await message.answer("❌ Заявка не найдена.")
        return
    _, user_id, user_name, gift_id, _, _ = gift
    await db.remove_pending_gift(gift_db_id)
    await message.answer(f"🗑 Заявка #{gift_db_id} удалена: {user_name} ({user_id}), подарок {gift_id}.")

@router.message(Command("premiumorders"), F.chat.type == "private")
async def cmd_premiumorders(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    orders = await db.get_premium_orders()
    if not orders:
        await message.answer("✅ Заявок на Premium нет.")
        return
    import datetime as dt
    text = f"💎 Заявки Premium на месяц ({len(orders)}):\n\n"
    for order_id, user_id, user_name, cost, created_at in orders:
        date = dt.datetime.fromtimestamp(created_at).strftime("%d.%m %H:%M")
        text += (
            f"#{order_id} | {user_name} ({user_id})\n"
            f"🪙 {cost:,} DC | {date}\n"
            f"После выдачи: premiumdone {order_id}\n"
            f"Возврат: premiumrefund {order_id}\n\n"
        ).replace(",", " ")
    await message.answer(text)

@router.message(Command("premiumdone"), F.chat.type == "private")
async def cmd_premiumdone(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: premiumdone ID")
        return
    order_id = int(parts[1])
    order = next((item for item in await db.get_premium_orders() if item[0] == order_id), None)
    if not order:
        await message.answer("❌ Заявка не найдена.")
        return
    _, user_id, user_name, _, _ = order
    await db.remove_premium_order(order_id)
    await message.answer(f"✅ Premium отмечен как выданный: {user_name} ({user_id}).")
    try:
        await bot.send_message(user_id, "💎 Premium на месяц выдан. Спасибо за обмен!")
    except Exception as e:
        logger.warning("Could not notify Premium recipient: %s", e)

@router.message(Command("premiumrefund"), F.chat.type == "private")
async def cmd_premiumrefund(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: premiumrefund ID")
        return
    order_id = int(parts[1])
    order = next((item for item in await db.get_premium_orders() if item[0] == order_id), None)
    if not order:
        await message.answer("❌ Заявка не найдена.")
        return
    _, user_id, user_name, cost, _ = order
    new_balance = await db.add_coins(user_id, cost)
    await db.remove_premium_order(order_id)
    await message.answer(f"↩️ Возвращено {cost:,} DC игроку {user_name} ({user_id}).".replace(",", " "))
    try:
        await bot.send_message(
            user_id,
            f"↩️ Premium пока недоступен — тебе вернули {cost:,} DC.\n🪙 Баланс: {new_balance:,} DC".replace(",", " "),
        )
    except Exception as e:
        logger.warning("Could not notify Premium refund recipient: %s", e)

# =========================
# GROUP — /stats
# =========================

@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    user_id = message.from_user.id
    chance, msg_count, _ = await db.get_user(user_id, message.chat.id)
    wins = await db.get_wins_count(user_id, message.chat.id)
    valid_refs = await db.count_valid_refs(user_id)
    balance, _ = await db.get_coins(user_id)
    await message.reply(
        f"📊 Статистика:\n\n"
        f"📈 Шанс: {chance:.3f}%\n"
        f"💬 Сообщений: {msg_count}\n"
        f"🏆 Побед: {wins}\n"
        f"👥 Рефералов: {valid_refs}\n"
        f"🪙 D-COINS: {balance}"
    )

@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    top  = await db.get_top(message.chat.id)
    text = "🏆 Топ участников:\n\n"
    for i, (name, chance, count) in enumerate(top, start=1):
        text += f"{i}. {name} — {chance:.3f}% ({count} сообщ.)\n"
    await message.reply(text)

@router.message(Command("winstop"))
async def cmd_winstop(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    top = await db.get_wins_top(message.chat.id)
    if not top:
        await message.reply("🏆 Побед пока нет.")
        return
    text = "🏆 Топ победителей:\n\n"
    for i, (name, cnt) in enumerate(top, start=1):
        text += f"{i}. {name} — {cnt} поб.\n"
    await message.reply(text)

@router.message(Command("reftop"))
async def cmd_reftop(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    top = await db.get_refs_top()
    if not top:
        await message.reply("👥 Рефералов пока нет.")
        return
    text = "👥 Топ по рефералам:\n\n"
    for i, (_, name, cnt) in enumerate(top, start=1):
        text += f"{i}. {name} — {cnt} реф.\n"
    await message.reply(text)

@router.message(Command("cointop"))
async def cmd_cointop(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    top = await db.get_coins_top(10)
    if not top:
        await message.reply("🪙 D-COINS ни у кого нет.")
        return
    text = "🪙 Топ по D-COINS:\n\n"
    for i, (_, name, bal) in enumerate(top, start=1):
        text += f"{i}. {name} — {bal} DC\n"
    await message.reply(text)

@router.message(Command("coins"))
async def cmd_coins(message: Message) -> None:
    if message.chat.type != "private" and message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    balance, _ = await db.get_coins(message.from_user.id)
    await message.reply(f"🪙 Твой баланс: {balance} D-COINS")

@router.message(Command("promo"), F.chat.type == "private")
async def cmd_promo(message: Message) -> None:
    if await db.is_banned(message.from_user.id):
        await message.answer(BAN_MESSAGE)
        return
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Использование: /promo КОД")
        return
    code = args[1].upper()
    status, reward, reward_type, case_id, case_count, uses, max_uses = await db.redeem_promo(code, message.from_user.id)
    if status == "success":
        limit_text = f"{uses}/{max_uses}" if max_uses is not None else f"{uses}/∞"
        if reward_type == "case":
            keys = await db.get_case_keys(message.from_user.id, case_id)
            await message.answer(
                f"✅ Промокод активирован!\n🎟 Получено: {CASES[case_id]['title']} × {case_count}\n"
                f"🔑 Ключей: {keys}\n👥 Активаций: {limit_text}"
            )
        else:
            balance, _ = await db.get_coins(message.from_user.id)
            await message.answer(f"✅ Промокод активирован!\n🎁 Получено: {reward} DC\n🪙 Баланс: {balance} DC\n👥 Активаций: {limit_text}")
    elif status == "already_used":
        await message.answer("❌ Ты уже активировал этот промокод.")
    elif status == "limit_reached":
        await message.answer("❌ Лимит активаций этого промокода исчерпан.")
    else:
        await message.answer("❌ Промокод не найден или уже отключён.")


# =========================
# /transfer — GROUP + PRIVATE
# =========================

@router.message(Command("transfer"))
async def cmd_transfer(message: Message, bot: Bot) -> None:
    sender_id = message.from_user.id

    if await db.is_banned(sender_id):
        await message.reply(BAN_MESSAGE)
        return

    # Запоминаем username отправителя, если он уже есть в user_stats.
    if message.chat.id == MAIN_CHAT_ID and message.from_user.username:
        await db.set_username(sender_id, message.from_user.username, MAIN_CHAT_ID, display_name(message.from_user))

    args = message.text.split()
    target_id = None
    target_name = None

    # Вариант 1: /transfer @username 100
    if len(args) == 3 and args[1].startswith("@"):
        username = args[1][1:].strip()
        try:
            amount = int(args[2])
        except ValueError:
            await message.reply("❌ Сумма должна быть целым числом.")
            return

        target = await db.find_user_by_username(username)
        if not target:
            await message.reply(
                "❌ Пользователь с таким username не найден.\n"
                "Пользователь должен хотя бы один раз написать в основной группе, "
                "чтобы бот знал его ID."
            )
            return
        target_id, target_name = target

    # Вариант 2: ответ на сообщение — /transfer 100
    elif len(args) == 2 and message.reply_to_message and message.reply_to_message.from_user:
        try:
            amount = int(args[1])
        except ValueError:
            await message.reply("❌ Сумма должна быть целым числом.")
            return

        target_user = message.reply_to_message.from_user
        target_id = target_user.id
        target_name = display_name(target_user)

        if target_user.username:
            await db.set_username(target_id, target_user.username, MAIN_CHAT_ID, target_name)

    else:
        await message.reply(
            "❌ Использование:\n"
            "/transfer @username сумма\n"
            "или ответь на сообщение командой /transfer сумма"
        )
        return

    if amount <= 0:
        await message.reply("❌ Сумма должна быть больше 0 D-COINS.")
        return

    if target_id == sender_id:
        await message.reply("❌ Нельзя переводить D-COINS самому себе.")
        return

    if await db.is_banned(target_id):
        await message.reply("❌ Нельзя переводить D-COINS заблокированному пользователю.")
        return

    ok, sender_balance, recipient_balance = await db.transfer_coins(sender_id, target_id, amount)
    if not ok:
        balance, _ = await db.get_coins(sender_id)
        await message.reply(
            f"❌ Недостаточно D-COINS.\n🪙 Твой баланс: {balance} DC"
        )
        return

    sender_name = display_name(message.from_user)
    if not target_name:
        target_name = await db.get_user_name(target_id)

    await message.reply(
        f"✅ Перевод выполнен!\n\n"
        f"👤 Получатель: {target_name}\n"
        f"💸 Переведено: {amount} DC\n"
        f"🪙 Твой баланс: {sender_balance} DC"
    )

    try:
        await bot.send_message(
            target_id,
            f"💰 Тебе перевели {amount} D-COINS!\n\n"
            f"👤 От: {sender_name}\n"
            f"🪙 Твой баланс: {recipient_balance} DC"
        )
    except Exception as e:
        logger.info("Не удалось уведомить получателя %s: %s", target_id, e)


@router.message(Command("daytop"))
async def cmd_daytop(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    top = await db.get_daily_top(message.chat.id)
    if not top:
        await message.reply("📊 Сегодня сообщений ещё нет.")
        return
    today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%d.%m.%Y")
    text = f"📊 Топ активных за {today}:\n\n"
    for i, (name, count) in enumerate(top, start=1):
        text += f"{i}. {name} — {count} сообщ.\n"
    await message.reply(text)

@router.message(Command("bonus"))
async def cmd_bonus(message: Message) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    user_id = message.from_user.id
    name    = display_name(message.from_user)
    chance, msg_count, last_bonus = await db.get_user(user_id, message.chat.id)
    balance, last_coin_bonus = await db.get_coins(user_id)
    now = time.time()
    is_vip = await db.is_vip(user_id)

    chance_text = ""
    coin_text   = ""

    if now - last_bonus >= BONUS_COOLDOWN:
        bonus_amount = round(random.uniform(0.05, 0.20), 3)
        new_chance   = min(round(chance + bonus_amount, 3), MAX_CHANCE)
        await db.update_user(user_id, message.chat.id, name, new_chance, msg_count, now)
        chance_text = f"📈 Шанс: +{bonus_amount:.3f}% → {new_chance:.3f}%"
    else:
        left = BONUS_COOLDOWN - (now - last_bonus)
        h, m = int(left // 3600), int(left % 3600 // 60)
        chance_text = f"⏳ Шанс-бонус через: {h} ч. {m} мин."

    if now - last_coin_bonus >= COINS_BONUS_CD:
        coins_amount = COINS_VIP_BONUS if is_vip else COINS_BONUS
        new_balance  = await db.add_coins(user_id, coins_amount)
        await db.set_coin_bonus_time(user_id)
        coin_text = f"🪙 D-COINS: +{coins_amount} → {new_balance} DC"
    else:
        left = COINS_BONUS_CD - (now - last_coin_bonus)
        h, m = int(left // 3600), int(left % 3600 // 60)
        coin_text = f"⏳ Бонус монет через: {h} ч. {m} мин."

    await message.reply(f"🎁 Ежедневный бонус:\n\n{chance_text}\n{coin_text}")

# =========================
# PRIVATE — CASES
# =========================

@router.message(Command("cases"), F.chat.type == "private")
async def cmd_cases(message: Message) -> None:
    if await db.is_banned(message.from_user.id):
        await message.answer(BAN_MESSAGE)
        return
    await send_cases_menu(message, message.from_user.id)

async def send_cases_menu(message: Message, user_id: int) -> None:
    await message.answer(
        "📦 Выбери кейс:",
        reply_markup=cases_keyboard(),
    )

async def show_case(callback: CallbackQuery, case_id: str) -> None:
    case = CASES[case_id]
    keys = await db.get_case_keys(callback.from_user.id, case_id)
    rewards = []
    for reward in case["rewards"]:
        if isinstance(reward[0], str):
            kind, value, _ = reward
            label = f"🎁 Подарок {value}⭐" if kind == "gift" else f"{value:,} DC".replace(",", " ")
        else:
            value, _ = reward
            label = f"{value:,} DC".replace(",", " ")
        rewards.append(f"• {label}")
    await callback.message.edit_text(
        f"📦 Кейс {case['title']}\n\n"
        f"💰 Цена: {case['price']:,} DC\n"
        f"🔑 Твоих ключей: {keys}\n\n"
        f"🎁 Возможные награды:\n" + "\n".join(rewards),
        reply_markup=case_detail_keyboard(case_id),
    )

@router.callback_query(F.data.startswith("case_view_"))
async def case_view_callback(callback: CallbackQuery) -> None:
    case_id = callback.data.removeprefix("case_view_")
    if case_id not in CASES:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    await show_case(callback, case_id)
    await callback.answer()

@router.callback_query(F.data == "cases")
async def cases_callback(callback: CallbackQuery) -> None:
    if await db.is_banned(callback.from_user.id):
        await callback.answer(BAN_MESSAGE, show_alert=True)
        return
    await callback.message.edit_text("📦 Выбери кейс:", reply_markup=cases_keyboard())
    await callback.answer()

async def open_case(callback: CallbackQuery, bot: Bot, case_id: str) -> None:
    user_id = callback.from_user.id
    if await db.is_banned(user_id):
        await callback.answer(BAN_MESSAGE, show_alert=True)
        return
    if user_id in case_open_cooldowns:
        await callback.answer(f"⏳ Следующее открытие через {CASE_OPEN_COOLDOWN} сек.", show_alert=True)
        return

    case = CASES[case_id]
    case_open_cooldowns[user_id] = True
    payment = await db.open_case(user_id, case_id, case["price"])
    if payment == "insufficient":
        case_open_cooldowns.pop(user_id, None)
        balance, _ = await db.get_coins(user_id)
        await callback.answer(f"❌ Нужно {case['price']} DC, у тебя {balance}", show_alert=True)
        return

    if isinstance(case["rewards"][0][0], str):
        kind, reward, _ = random.choices(case["rewards"], weights=[item[2] for item in case["rewards"]], k=1)[0]
    else:
        reward, _ = random.choices(case["rewards"], weights=[item[1] for item in case["rewards"]], k=1)[0]
        kind = "coins"
    if kind == "coins":
        new_balance = await db.add_coins(user_id, reward)
        prize_text = f"🎉 Выпало: {reward:,} DC".replace(",", " ")
    else:
        gift_key = {15: 5, 25: 10, 50: 15, 100: 20}[reward]
        gift_id = random.choice(REF_GIFT_IDS[gift_key])
        try:
            await bot.send_gift(user_id=user_id, gift_id=gift_id)
            prize_text = f"🎁 Выпал подарок {reward}⭐\n✅ Подарок отправлен в личку!"
        except Exception as e:
            await db.add_pending_gift(user_id, await db.get_user_name(user_id), gift_id, f"кейс {case['title']}: {e}")
            prize_text = f"🎁 Выпал подарок {reward}⭐\n⏳ Добавлен в очередь выдачи."
        new_balance, _ = await db.get_coins(user_id)
    keys = await db.get_case_keys(user_id, case_id)
    payment_text = "🔑 Использован ключ кейса" if payment == "key" else f"💸 Списано: {case['price']:,} DC".replace(",", " ")
    result_text = (
        f"📦 {case['title']} открыт!\n\n"
        f"{prize_text}\n"
        f"{payment_text}\n"
        f"🪙 Баланс: {new_balance:,} DC\n"
        f"🔑 Ключей: {keys}"
    ).replace(",", " ")
    await callback.message.edit_text(
        result_text,
        reply_markup=case_detail_keyboard(case_id),
    )
    await send_game_log(
        bot,
        f"📦 Открыт кейс {case['title']}\n"
        f"👤 {display_name(callback.from_user)} ({user_id})\n"
        f"{payment_text}\n{prize_text}\n"
        f"🪙 Баланс: {new_balance:,} DC".replace(",", " "),
    )
    await callback.answer()

@router.callback_query(F.data == "case_open_karapuz")
async def open_karapuz_case(callback: CallbackQuery, bot: Bot) -> None:
    await open_case(callback, bot, "karapuz")

@router.callback_query(F.data == "case_open_blood")
async def open_blood_case(callback: CallbackQuery, bot: Bot) -> None:
    await open_case(callback, bot, "blood")

@router.callback_query(F.data == "case_open_pantera")
async def open_pantera_case(callback: CallbackQuery, bot: Bot) -> None:
    await open_case(callback, bot, "pantera")

@router.callback_query(F.data == "case_open_spider_man")
async def open_spider_man_case(callback: CallbackQuery, bot: Bot) -> None:
    await open_case(callback, bot, "spider_man")

# =========================
# GROUP — CASINO
# =========================

@router.message(Command("slots"))
async def cmd_slots(message: Message, bot: Bot) -> None:
    if message.chat.type != "private":
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    user_id = message.from_user.id

    if user_id in active_games:
        await message.reply("🎰 У тебя уже есть активная игра! Сначала заверши её.")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /slots [ставка]\nПример: /slots 50")
        return
    try:
        bet = int(args[1])
    except ValueError:
        await message.reply("❌ Ставка должна быть числом.")
        return
    if bet < CASINO_MIN_BET:
        await message.reply(f"❌ Минимальная ставка: {CASINO_MIN_BET} DC")
        return

    balance, _ = await db.get_coins(user_id)
    if balance < bet:
        await message.reply(f"❌ Недостаточно D-COINS!\n🪙 Твой баланс: {balance} DC")
        return

    if user_id in casino_bet_cooldowns:
        await message.reply(f"⏳ Следующая ставка будет доступна через {CASINO_BET_COOLDOWN} сек.")
        return
    casino_bet_cooldowns[user_id] = True
    if not await db.remove_coins(user_id, bet):
        casino_bet_cooldowns.pop(user_id, None)
        await message.reply("❌ Недостаточно D-COINS!")
        return
    balance_after, _ = await db.get_coins(user_id)

    SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎"]
    s1 = random.choice(SYMBOLS)
    s2 = random.choice(SYMBOLS)
    s3 = random.choice(SYMBOLS)

    if s1 == s2 == s3:
        win = bet * 2
        await db.add_coins(user_id, win)
        new_balance, _ = await db.get_coins(user_id)
        await message.reply(
            f"🎰 {s1} {s2} {s3}\n\n"
            f"✅ Ты выиграл!\n"
            f"💸 Ставка: {bet} DC\n"
            f"🏆 Выигрыш: {win} DC\n"
            f"🪙 Баланс: {new_balance} DC"
        )
        await send_game_log(bot, f"🎰 Слоты\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC\n✅ Выигрыш: {win} DC\n🪙 Баланс: {new_balance} DC")
    else:
        await message.reply(
            f"🎰 {s1} {s2} {s3}\n\n"
            f"❌ Не повезло!\n"
            f"💸 Ставка: {bet} DC\n"
            f"🪙 Баланс: {balance_after} DC"
        )
        await send_game_log(bot, f"🎰 Слоты\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC\n❌ Проигрыш\n🪙 Баланс: {balance_after} DC")


@router.message(Command("roulette"))
async def cmd_roulette(message: Message, bot: Bot) -> None:
    if message.chat.type != "private":
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    user_id = message.from_user.id

    if user_id in active_games:
        await message.reply("🎰 У тебя уже есть активная игра! Сначала заверши её.")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /roulette [red/black] [ставка]\nПример: /roulette red 50")
        return

    color = args[1].lower()
    if color not in ("red", "black"):
        await message.reply("❌ Выбери цвет: red или black\nПример: /roulette red 50")
        return

    try:
        bet = int(args[2])
    except ValueError:
        await message.reply("❌ Ставка должна быть числом.")
        return
    if bet < CASINO_MIN_BET:
        await message.reply(f"❌ Минимальная ставка: {CASINO_MIN_BET} DC")
        return

    balance, _ = await db.get_coins(user_id)
    if balance < bet:
        await message.reply(f"❌ Недостаточно D-COINS!\n🪙 Твой баланс: {balance} DC")
        return

    if user_id in casino_bet_cooldowns:
        await message.reply(f"⏳ Следующая ставка будет доступна через {CASINO_BET_COOLDOWN} сек.")
        return
    casino_bet_cooldowns[user_id] = True
    if not await db.remove_coins(user_id, bet):
        casino_bet_cooldowns.pop(user_id, None)
        await message.reply("❌ Недостаточно D-COINS!")
        return

    result_color = random.choice(["red"] * 18 + ["black"] * 18 + ["green"])
    emoji_map = {"red": "🔴", "black": "⚫", "green": "🟢"}
    result_emoji = emoji_map[result_color]
    chosen_emoji = "🔴" if color == "red" else "⚫"

    if result_color == color:
        win = bet * 2
        await db.add_coins(user_id, win)
        new_balance, _ = await db.get_coins(user_id)
        await message.reply(
            f"🎡 Выпало: {result_emoji}\n\n"
            f"✅ Ты выиграл!\n"
            f"💸 Ставка: {bet} DC на {chosen_emoji}\n"
            f"🏆 Выигрыш: {win} DC\n"
            f"🪙 Баланс: {new_balance} DC"
        )
        await send_game_log(bot, f"🎡 Рулетка\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC на {chosen_emoji}\nВыпало: {result_emoji}\n✅ Выигрыш: {win} DC\n🪙 Баланс: {new_balance} DC")
    else:
        new_balance, _ = await db.get_coins(user_id)
        await message.reply(
            f"🎡 Выпало: {result_emoji}\n\n"
            f"❌ Не повезло!\n"
            f"💸 Ставка: {bet} DC на {chosen_emoji}\n"
            f"🪙 Баланс: {new_balance} DC"
        )
        await send_game_log(bot, f"🎡 Рулетка\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC на {chosen_emoji}\nВыпало: {result_emoji}\n❌ Проигрыш\n🪙 Баланс: {new_balance} DC")


@router.message(Command("dice"))
async def cmd_dice(message: Message, bot: Bot) -> None:
    if message.chat.type != "private":
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    user_id = message.from_user.id

    if user_id in active_games:
        await message.reply("🎰 У тебя уже есть активная игра! Сначала заверши её.")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /dice [число 1-6] [ставка]\nПример: /dice 3 50")
        return

    try:
        number = int(args[1])
        bet    = int(args[2])
    except ValueError:
        await message.reply("❌ Число и ставка должны быть числами.")
        return

    if number < 1 or number > 6:
        await message.reply("❌ Число должно быть от 1 до 6.")
        return
    if bet < CASINO_MIN_BET:
        await message.reply(f"❌ Минимальная ставка: {CASINO_MIN_BET} DC")
        return

    balance, _ = await db.get_coins(user_id)
    if balance < bet:
        await message.reply(f"❌ Недостаточно D-COINS!\n🪙 Твой баланс: {balance} DC")
        return

    if user_id in casino_bet_cooldowns:
        await message.reply(f"⏳ Следующая ставка будет доступна через {CASINO_BET_COOLDOWN} сек.")
        return
    casino_bet_cooldowns[user_id] = True
    if not await db.remove_coins(user_id, bet):
        casino_bet_cooldowns.pop(user_id, None)
        await message.reply("❌ Недостаточно D-COINS!")
        return

    rolled = random.randint(1, 6)

    if rolled == number:
        win = bet * 2
        await db.add_coins(user_id, win)
        new_balance, _ = await db.get_coins(user_id)
        await message.reply(
            f"🎲 Выпало: {rolled}\n\n"
            f"✅ Угадал!\n"
            f"💸 Ставка: {bet} DC на {number}\n"
            f"🏆 Выигрыш: {win} DC\n"
            f"🪙 Баланс: {new_balance} DC"
        )
        await send_game_log(bot, f"🎲 Кубик\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC на {number}\nВыпало: {rolled}\n✅ Выигрыш: {win} DC\n🪙 Баланс: {new_balance} DC")
    else:
        new_balance, _ = await db.get_coins(user_id)
        await message.reply(
            f"🎲 Выпало: {rolled}\n\n"
            f"❌ Не угадал! (ты выбрал {number})\n"
            f"💸 Ставка: {bet} DC\n"
            f"🪙 Баланс: {new_balance} DC"
        )
        await send_game_log(bot, f"🎲 Кубик\n👤 {display_name(message.from_user)} ({user_id})\n💸 Ставка: {bet} DC на {number}\nВыпало: {rolled}\n❌ Проигрыш\n🪙 Баланс: {new_balance} DC")


# =========================
# PRIVATE — MINES
# =========================

MINES_GRID_SIZE = 25
MINES_COUNT = 6

def mines_multiplier(safe_opened: int) -> float:
    """Коэффициент для поля 5×5 с шестью минами."""
    if safe_opened <= 0:
        return 0.0
    # Первые два коэффициента совпадают с привычной механикой игры.
    if safe_opened == 1:
        return 1.28
    if safe_opened == 2:
        return 1.65
    fair_multiplier = comb(MINES_GRID_SIZE, safe_opened) / comb(MINES_GRID_SIZE - MINES_COUNT, safe_opened)
    return round(fair_multiplier * 0.94, 2)

def mines_prize(bet: int, safe_opened: int) -> int:
    return int(bet * mines_multiplier(safe_opened))

def mines_keyboard(game: dict, reveal: bool = False) -> InlineKeyboardMarkup:
    opened = game["opened"]
    mines = game["mines"]
    buttons = []
    for row in range(5):
        line = []
        for column in range(5):
            cell = row * 5 + column
            if reveal and cell in mines:
                text = "💣"
                callback_data = "mines_done"
            elif cell in opened:
                text = " "
                callback_data = "mines_done"
            elif reveal:
                text = " "
                callback_data = "mines_done"
            else:
                text = "❓"
                callback_data = f"mines_cell_{cell}"
            line.append(InlineKeyboardButton(text=text, callback_data=callback_data))
        buttons.append(line)
    if not reveal:
        if opened:
            prize = mines_prize(game["bet"], len(opened))
            buttons.append([InlineKeyboardButton(text=f"💸 Забрать {prize:,} DC".replace(",", " "), callback_data="mines_cashout")])
        else:
            buttons.append([InlineKeyboardButton(text="❌", callback_data="mines_cashout")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def mines_text(game: dict) -> str:
    safe_opened = len(game["opened"])
    text = (
        "💣 Ты начал игру «Минное поле»!\n"
        f"💰 Ставка: {game['bet']:,} DC\n"
        f"💣 Мин на поле: {MINES_COUNT}"
    ).replace(",", " ")
    if safe_opened:
        multiplier = mines_multiplier(safe_opened)
        prize = mines_prize(game["bet"], safe_opened)
        text += f"\n💵 Выигрыш: x{multiplier:.2f} | {prize:,} DC".replace(",", " ")
    return text

async def start_mines_game(message: Message) -> None:
    user_id = message.from_user.id
    if await db.is_banned(user_id):
        await message.reply(BAN_MESSAGE)
        return
    if user_id in active_games:
        await message.reply("🎰 У тебя уже есть активная игра! Сначала заверши её.")
        return

    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /mines ставка\nПример: /mines 2500")
        return
    try:
        bet = int(args[1])
    except ValueError:
        await message.reply("❌ Ставка должна быть числом.")
        return
    if bet < CASINO_MIN_BET:
        await message.reply(f"❌ Минимальная ставка: {CASINO_MIN_BET} DC")
        return
    if user_id in casino_bet_cooldowns:
        await message.reply(f"⏳ Следующая ставка будет доступна через {CASINO_BET_COOLDOWN} сек.")
        return
    if not await db.remove_coins(user_id, bet):
        balance, _ = await db.get_coins(user_id)
        await message.reply(f"❌ Недостаточно D-COINS!\n🪙 Твой баланс: {balance} DC")
        return

    casino_bet_cooldowns[user_id] = True
    game = {
        "game": "mines",
        "bet": bet,
        "mines": set(random.sample(range(MINES_GRID_SIZE), MINES_COUNT)),
        "opened": set(),
        "chat_id": message.chat.id,
        "expires": time.time() + CASINO_TIMEOUT,
    }
    active_games[user_id] = game
    await message.reply(mines_text(game), reply_markup=mines_keyboard(game))

@router.message(Command("mines"))
async def cmd_mines(message: Message) -> None:
    if message.chat.type != "private":
        return
    await start_mines_game(message)

@router.message(F.chat.type == "private", F.text.regexp(r"(?i)^мины\s+\d+\s*$"))
async def cmd_mines_text(message: Message) -> None:
    await start_mines_game(message)

@router.callback_query(F.data.startswith("mines_cell_"))
async def mines_open_cell(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    game = active_games.get(user_id)
    if not game or game.get("game") != "mines":
        await callback.answer("Игра уже завершена.", show_alert=True)
        return
    try:
        cell = int(callback.data.removeprefix("mines_cell_"))
    except (ValueError, AttributeError):
        await callback.answer("Некорректная клетка.", show_alert=True)
        return
    if cell < 0 or cell >= MINES_GRID_SIZE or cell in game["opened"]:
        await callback.answer("Эта клетка уже открыта.", show_alert=True)
        return

    if cell in game["mines"]:
        active_games.pop(user_id, None)
        balance, _ = await db.get_coins(user_id)
        await callback.message.edit_text(
            "💣 Игра завершена!\n💵 Вы проиграли.",
            reply_markup=mines_keyboard(game, reveal=True),
        )
        await send_game_log(
            bot,
            f"💣 Мины\n👤 {display_name(callback.from_user)} ({user_id})\n"
            f"💸 Ставка: {game['bet']} DC\n❌ Проигрыш\n🪙 Баланс: {balance} DC",
        )
        await callback.answer("💥 Мина!")
        return

    game["opened"].add(cell)
    if len(game["opened"]) == MINES_GRID_SIZE - MINES_COUNT:
        prize = mines_prize(game["bet"], len(game["opened"]))
        active_games.pop(user_id, None)
        balance = await db.add_coins(user_id, prize)
        await callback.message.edit_text(
            f"🏆 Поле очищено!\n💵 Выигрыш: x{mines_multiplier(len(game['opened'])):.2f} | {prize:,} DC\n🪙 Баланс: {balance:,} DC".replace(",", " "),
            reply_markup=mines_keyboard(game, reveal=True),
        )
        await send_game_log(bot, f"💣 Мины\n👤 {display_name(callback.from_user)} ({user_id})\n💸 Ставка: {game['bet']} DC\n🏆 Поле очищено: +{prize} DC\n🪙 Баланс: {balance} DC")
    else:
        await callback.message.edit_text(mines_text(game), reply_markup=mines_keyboard(game))
    await callback.answer()

@router.callback_query(F.data == "mines_cashout")
async def mines_cashout(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    game = active_games.get(user_id)
    if not game or game.get("game") != "mines":
        await callback.answer("Игра уже завершена.", show_alert=True)
        return
    safe_opened = len(game["opened"])
    if not safe_opened:
        await callback.answer("Открой хотя бы одну клетку.", show_alert=True)
        return
    prize = mines_prize(game["bet"], safe_opened)
    active_games.pop(user_id, None)
    balance = await db.add_coins(user_id, prize)
    await callback.message.edit_text(
        f"✅ Вы забрали выигрыш!\n💵 Выигрыш: x{mines_multiplier(safe_opened):.2f} | {prize:,} DC\n🪙 Баланс: {balance:,} DC".replace(",", " "),
        reply_markup=mines_keyboard(game, reveal=True),
    )
    await send_game_log(
        bot,
        f"💣 Мины\n👤 {display_name(callback.from_user)} ({user_id})\n"
        f"💸 Ставка: {game['bet']} DC\n✅ Забрал: {prize} DC (x{mines_multiplier(safe_opened):.2f})\n"
        f"🪙 Баланс: {balance} DC",
    )
    await callback.answer("✅ Выигрыш зачислен")

@router.callback_query(F.data == "mines_done")
async def mines_done(callback: CallbackQuery) -> None:
    await callback.answer("Игра уже завершена.")


# =========================
# GROUP — /exchange
# =========================

@router.message(Command("exchange"))
async def cmd_exchange(message: Message) -> None:
    if message.chat.type != "private" and message.chat.id != MAIN_CHAT_ID:
        return
    if await db.is_banned(message.from_user.id):
        await message.reply(BAN_MESSAGE)
        return
    balance, _ = await db.get_coins(message.from_user.id)
    await message.reply(
        f"💱 Обмен D-COINS\n\n"
        f"🪙 Твой баланс: {balance} DC\n\n"
        f"Выбери что хочешь получить:",
        reply_markup=exchange_keyboard(balance)
    )

@router.callback_query(F.data == "buy_dc_menu")
async def buy_dc_menu(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "⭐ Покупка D-COINS за звёзды\n\n"
        "Курс: 500 DC = 1⭐\n"
        "Выбери пакет:",
        reply_markup=buy_dc_keyboard(),
    )
    await callback.answer()

@router.callback_query(F.data == "buy_dc_back")
async def buy_dc_back(callback: CallbackQuery) -> None:
    balance, _ = await db.get_coins(callback.from_user.id)
    await callback.message.edit_text(
        f"💱 Обмен D-COINS\n\n🪙 Твой баланс: {balance:,} DC\n\nВыбери что хочешь получить:".replace(",", " "),
        reply_markup=exchange_keyboard(balance),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("buy_dc_"))
async def buy_dc_package(callback: CallbackQuery, bot: Bot) -> None:
    try:
        dc_amount = int(callback.data.removeprefix("buy_dc_"))
    except (AttributeError, ValueError):
        await callback.answer("❌ Пакет не найден.", show_alert=True)
        return
    star_amount = STAR_DC_PACKAGES.get(dc_amount)
    if star_amount is None:
        await callback.answer("❌ Пакет не найден.", show_alert=True)
        return
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"{dc_amount:,} D-COINS".replace(",", " "),
            description=f"Покупка {dc_amount:,} D-COINS за {star_amount}⭐".replace(",", " "),
            payload=f"buy_dc_{dc_amount}",
            currency="XTR",
            prices=[LabeledPrice(label=f"{dc_amount:,} DC".replace(",", " "), amount=star_amount)],
        )
    except Exception as e:
        logger.warning("Could not create Stars invoice: %s", e)
        await callback.answer("❌ Не удалось создать счёт. Попробуй позже.", show_alert=True)
        return
    await callback.answer("⭐ Счёт отправлен в личные сообщения")

@router.callback_query(F.data == "exch_chance")
async def exch_chance(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    if await db.is_banned(user_id):
        await callback.answer(BAN_MESSAGE, show_alert=True)
        return
    balance, _ = await db.get_coins(user_id)
    if balance < EXCHANGE_CHANCE:
        await callback.answer(f"❌ Нужно {EXCHANGE_CHANCE} DC, у тебя {balance}", show_alert=True)
        return
    if not await db.remove_coins(user_id, EXCHANGE_CHANCE):
        balance, _ = await db.get_coins(user_id)
        await callback.answer(f"❌ Нужно {EXCHANGE_CHANCE} DC, у тебя {balance}", show_alert=True)
        return
    chance, msg_count, last_bonus = await db.get_user(user_id, MAIN_CHAT_ID)
    new_chance = min(round(chance + 1.0, 3), MAX_CHANCE)
    name = await db.get_user_name(user_id)
    await db.update_user(user_id, MAIN_CHAT_ID, name, new_chance, msg_count, last_bonus)
    new_balance, _ = await db.get_coins(user_id)
    await callback.message.edit_text(
        f"✅ Обменял {EXCHANGE_CHANCE} DC на +1% шанса\n"
        f"🪙 Баланс: {new_balance} DC\n"
        f"📈 Новый шанс: {new_chance:.3f}%",
        reply_markup=exchange_keyboard(new_balance)
    )
    await callback.answer()

async def process_exchange_gift(callback: CallbackQuery, cost: int, gift_key: int, reward_label: str, bot: Bot) -> None:
    user_id = callback.from_user.id
    if await db.is_banned(user_id):
        await callback.answer(BAN_MESSAGE, show_alert=True)
        return
    balance, _ = await db.get_coins(user_id)
    if balance < cost:
        await callback.answer(f"❌ Нужно {cost} DC, у тебя {balance}", show_alert=True)
        return
    gift_ids = REF_GIFT_IDS.get(gift_key, [])
    gift_id  = random.choice(gift_ids) if gift_ids else None
    if not gift_id:
        logger.error("No gift IDs configured for exchange gift key %s", gift_key)
        await callback.answer("❌ Этот подарок временно недоступен.", show_alert=True)
        return

    if not await db.remove_coins(user_id, cost):
        balance, _ = await db.get_coins(user_id)
        await callback.answer(f"❌ Нужно {cost} DC, у тебя {balance}", show_alert=True)
        return

    name = await db.get_user_name(user_id)
    new_balance, _ = await db.get_coins(user_id)
    pending_reason = f"обмен {cost} DC → {reward_label}"
    try:
        star_balance = await bot.get_my_star_balance()
        stars_needed = int(reward_label.replace("⭐", "").strip())
    except Exception as e:
        logger.warning("Could not check star balance: %s", e)
        await db.add_pending_gift(user_id, name, gift_id, f"{pending_reason} — не удалось проверить баланс")
        await callback.message.edit_text(
            f"✅ Обменял {cost} DC на подарок {reward_label}\n"
            f"🪙 Баланс: {new_balance} DC\n"
            f"⏳ Подарок будет отправлен позже.",
            reply_markup=exchange_keyboard(new_balance),
        )
        await callback.answer()
        return

    if star_balance.amount < stars_needed:
        await db.add_pending_gift(user_id, name, gift_id, pending_reason)
        try:
            await bot.send_message(
                ADMIN_ID,
                f"⚠️ Недостаточно звёзд!\n\n👤 {name} ({user_id})\n💫 {star_balance.amount}⭐\nДобавлен в /pending",
            )
        except Exception as e:
            logger.warning("Could not notify admin about pending gift: %s", e)
        await callback.message.edit_text(
            f"✅ Обменял {cost} DC на подарок {reward_label}\n"
            f"🪙 Баланс: {new_balance} DC\n"
            f"⏳ Подарок будет отправлен как только пополним баланс.",
            reply_markup=exchange_keyboard(new_balance),
        )
        await callback.answer()
        return

    try:
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
    except Exception as e:
        logger.warning("Exchange gift failed: %s", e)
        await db.add_pending_gift(user_id, name, gift_id, f"{pending_reason} — ошибка: {e}")
        await callback.message.edit_text(
            f"✅ Обменял {cost} DC на подарок {reward_label}\n"
            f"🪙 Баланс: {new_balance} DC\n"
            f"⏳ Подарок будет отправлен позже.",
            reply_markup=exchange_keyboard(new_balance),
        )
        await callback.answer()
        return

    try:
        await send_log(bot, f"🎁 Обмен монет\n\n{name} ({user_id})\n{cost} DC → {reward_label}")
    except Exception as e:
        logger.warning("Could not log exchange gift: %s", e)
    await callback.message.edit_text(
        f"✅ Обменял {cost} DC на подарок {reward_label}\n"
        f"🪙 Баланс: {new_balance} DC\n"
        f"🎁 Подарок отправлен в личку!",
        reply_markup=exchange_keyboard(new_balance),
    )
    await callback.answer()

@router.callback_query(F.data == "exch_gift_15")
async def exch_gift_15(callback: CallbackQuery, bot: Bot) -> None:
    await process_exchange_gift(callback, EXCHANGE_GIFT_15, 5, "15⭐", bot)

@router.callback_query(F.data == "exch_gift_25")
async def exch_gift_25(callback: CallbackQuery, bot: Bot) -> None:
    await process_exchange_gift(callback, EXCHANGE_GIFT_25, 10, "25⭐", bot)

@router.callback_query(F.data == "exch_gift_50")
async def exch_gift_50(callback: CallbackQuery, bot: Bot) -> None:
    await process_exchange_gift(callback, EXCHANGE_GIFT_50, 15, "50⭐", bot)

@router.callback_query(F.data == "exch_gift_100")
async def exch_gift_100(callback: CallbackQuery, bot: Bot) -> None:
    await process_exchange_gift(callback, EXCHANGE_GIFT_100, 20, "100⭐", bot)

@router.callback_query(F.data == "exch_premium_1m")
async def exch_premium_1m(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if await db.is_banned(user_id):
        await callback.answer(BAN_MESSAGE, show_alert=True)
        return
    if not await db.remove_coins(user_id, EXCHANGE_PREMIUM_1_MONTH):
        balance, _ = await db.get_coins(user_id)
        await callback.answer(
            f"❌ Нужно {EXCHANGE_PREMIUM_1_MONTH:,} DC, у тебя {balance:,}".replace(",", " "),
            show_alert=True,
        )
        return

    name = await db.get_user_name(user_id)
    try:
        order_id = await db.add_premium_order(user_id, name, EXCHANGE_PREMIUM_1_MONTH)
    except Exception:
        await db.add_coins(user_id, EXCHANGE_PREMIUM_1_MONTH)
        logger.exception("Could not create Premium order")
        await callback.answer("❌ Не удалось создать заявку. DC возвращены.", show_alert=True)
        return

    new_balance, _ = await db.get_coins(user_id)
    await callback.message.edit_text(
        f"✅ Заявка #{order_id} на Premium на месяц создана\n"
        f"🪙 Списано: {EXCHANGE_PREMIUM_1_MONTH:,} DC\n"
        f"🪙 Баланс: {new_balance:,} DC\n\n"
        "💎 Premium будет выдан вручную в ближайшее время.".replace(",", " "),
        reply_markup=exchange_keyboard(new_balance),
    )
    try:
        await bot.send_message(
            ADMIN_ID,
            f"💎 Новая заявка Premium на месяц\n\n"
            f"#{order_id} | {name} ({user_id})\n"
            f"🪙 {EXCHANGE_PREMIUM_1_MONTH:,} DC\n\n"
            f"После выдачи: premiumdone {order_id}".replace(",", " "),
        )
        await send_log(bot, f"💎 Обмен на Premium\n\n#{order_id} | {name} ({user_id})\n{EXCHANGE_PREMIUM_1_MONTH:,} DC".replace(",", " "))
    except Exception as e:
        logger.warning("Could not notify about Premium order: %s", e)
    await callback.answer("✅ Заявка создана")

# =========================
# NEW MEMBERS
# =========================

@router.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_user_join(event: ChatMemberUpdated, bot: Bot) -> None:
    if event.chat.id != MAIN_CHAT_ID:
        return
    member = event.new_chat_member.user
    if member.is_bot:
        return
    if await db.is_already_referred(member.id):
        return
    invite_link = event.invite_link
    if not invite_link:
        return
    link_str   = invite_link.invite_link
    inviter_id = await db.get_owner_by_link(link_str)
    # Все ссылки создаёт бот, поэтому invite_link.creator не является
    # пригласившим пользователем. Для старых ссылок восстанавливаем ID
    # из имени ref_<user_id>, иначе реферала не засчитываем никому.
    if not inviter_id and invite_link.name and invite_link.name.startswith("ref_"):
        try:
            inviter_id = int(invite_link.name.removeprefix("ref_"))
        except ValueError:
            inviter_id = None
    if not inviter_id:
        logger.warning("Unknown referral invite link: %s", link_str)
        return
    if inviter_id == member.id:
        return
    await db.add_referral(member.id, inviter_id)
    await send_log(bot,
        f"🔗 Новый реферал\n\n"
        f"👤 Пришёл: {display_name(member)} ({member.id})\n"
        f"👥 Пригласил: {inviter_id}\n"
        f"⏳ Нужно сообщений: {VALID_REF_MESSAGES}"
    )

# Обычные слова вместо команд со слешем. Этот обработчик расположен до
# group_handler, поэтому команды не засчитываются как обычные сообщения.
PRIVATE_PLAIN_COMMANDS = {
    "start", "ref", "refstats", "say", "vip", "unvip", "viplist", "ban",
    "unban", "banlist", "addmsgs", "removemsgs", "addday", "removeday",
    "addcoins", "removecoins", "createpromo", "deletepromo", "createcasepromo",
    "promos", "addrefs", "removerefs", "balance", "popolnit", "sendgift",
    "pending", "deliver", "deletepending", "premiumorders", "premiumdone", "premiumrefund",
    "promo", "cases", "slots", "roulette", "dice", "mines",
}
GROUP_PLAIN_COMMANDS = {"stats", "top", "winstop", "reftop", "cointop", "daytop", "bonus"}
BOT_ARGUMENT_COMMANDS = {
    "start", "ref", "say", "addrefs", "balance", "popolnit", "sendgift",
    "createpromo", "createcasepromo",
    "pending", "deliver", "premiumdone", "premiumrefund", "transfer", "slots", "roulette", "dice",
}
PLAIN_COMMAND_HANDLERS = {
    "start": cmd_start, "help": cmd_help, "ref": cmd_ref, "refstats": cmd_refstats,
    "say": cmd_say, "vip": cmd_vip, "unvip": cmd_unvip, "viplist": cmd_viplist,
    "ban": cmd_ban, "unban": cmd_unban, "banlist": cmd_banlist,
    "addmsgs": cmd_addmsgs, "removemsgs": cmd_removemsgs,
    "addday": cmd_addday, "removeday": cmd_removeday,
    "addcoins": cmd_addcoins, "removecoins": cmd_removecoins,
    "createpromo": cmd_createpromo, "deletepromo": cmd_deletepromo,
    "createcasepromo": cmd_createcasepromo, "promos": cmd_promos,
    "addrefs": cmd_addrefs, "removerefs": cmd_removerefs,
    "balance": cmd_balance, "popolnit": cmd_popolnit, "sendgift": cmd_sendgift,
    "pending": cmd_pending, "deliver": cmd_deliver, "deletepending": cmd_deletepending,
    "premiumorders": cmd_premiumorders,
    "premiumdone": cmd_premiumdone, "premiumrefund": cmd_premiumrefund,
    "stats": cmd_stats, "top": cmd_top, "winstop": cmd_winstop,
    "reftop": cmd_reftop, "cointop": cmd_cointop, "coins": cmd_coins,
    "promo": cmd_promo, "transfer": cmd_transfer, "daytop": cmd_daytop,
    "bonus": cmd_bonus, "cases": cmd_cases, "slots": cmd_slots,
    "roulette": cmd_roulette, "dice": cmd_dice, "mines": cmd_mines,
    "exchange": cmd_exchange,
}

@router.message(is_plain_command)
async def plain_command_handler(message: Message, bot: Bot) -> None:
    parsed = parse_plain_command(message.text)
    if not parsed:
        return
    command, args = parsed
    if command in PRIVATE_PLAIN_COMMANDS and message.chat.type != "private":
        return
    if command in GROUP_PLAIN_COMMANDS and message.chat.id != MAIN_CHAT_ID:
        return

    command_message = message.model_copy(update={"text": "/" + command + (" " + " ".join(args) if args else "")})
    handler = PLAIN_COMMAND_HANDLERS[command]
    if command in BOT_ARGUMENT_COMMANDS:
        await handler(command_message, bot)
    else:
        await handler(command_message)

# =========================
# MAIN GROUP HANDLER
# =========================

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def group_handler(message: Message, bot: Bot) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        if message.chat.id not in {LOG_CHAT_ID, GAME_LOG_CHAT_ID}:
            try:
                await bot.leave_chat(message.chat.id)
            except Exception as e:
                logger.warning("leave_chat failed: %s", e)
        return

    msg_text = message.text or message.caption
    if not msg_text or msg_text.startswith("/"):
        return

    # Сообщение, отправленное от имени канала, должно учитываться за канал.
    # Проверяем sender_chat первым: Telegram может одновременно передать from_user.
    if message.sender_chat:
        user_id = message.sender_chat.id
        name = message.sender_chat.title or str(message.sender_chat.id)
    elif message.from_user and not message.from_user.is_bot:
        user_id = message.from_user.id
        name = display_name(message.from_user)
    else:
        return

    if message.from_user and message.from_user.username:
        await db.set_username(user_id, message.from_user.username, message.chat.id, name)

    cache_key = (user_id, message.chat.id)

    if await db.is_banned(user_id):
        return

    if cache_key in cooldowns:
        return
    cooldowns[cache_key] = True

    await db.increment_daily(user_id, message.chat.id, name)

    chance, msg_count, last_bonus = await db.get_user(user_id, message.chat.id)
    is_vip = await db.is_vip(user_id)

    # Монеты за сообщение
    coins_earned = COINS_VIP_PER_MSG if is_vip else COINS_PER_MSG
    await db.add_coins(user_id, coins_earned)

    # REF VALIDATION
    ref_data = await db.get_referral(user_id)
    if ref_data:
        inviter_id, valid, _ = ref_data
        if not valid:
            new_ref_count = await db.increment_ref_messages(user_id)
            if new_ref_count % 5 == 0 and new_ref_count < VALID_REF_MESSAGES:
                try:
                    await bot.send_message(inviter_id, f"⏳ Твой реферал написал {new_ref_count}/{VALID_REF_MESSAGES} сообщений")
                except Exception:
                    pass
            if new_ref_count >= VALID_REF_MESSAGES:
                await db.validate_referral(user_id)
                await reward_inviter(bot, inviter_id)

    # WIN SYSTEM
    msg_step = 1
    chance_step = 0.003 if is_vip else 0.002  # VIP x1.5

    if msg_count < 150:
        is_win = False
    else:
        is_win = chance >= MAX_CHANCE or random.uniform(0, 1000) <= chance

    if is_win:
        await db.add_win(user_id, message.chat.id, name, chance)
        await message.reply(
            f"🏆 Поздравляем, {name}!\n\n"
            f"📈 Шанс был: {chance:.3f}%\n"
            f"🎁 Подарок уже отправлен тебе в личку!"
        )
        await send_log(bot, f"🏆 Победитель\n\n{name} ({user_id})\nШанс: {chance:.3f}%")
        await bot.send_message(ADMIN_ID, f"🏆 Новый победитель\n\n{name} ({user_id})\nШанс: {chance:.3f}%")

        try:
            star_balance = await bot.get_my_star_balance()
            if star_balance.amount < 15:
                await db.add_pending_gift(user_id, name, WIN_GIFT_IDS[0], "победа")
                await send_log(bot, f"⚠️ Недостаточно звёзд!\n\nБаланс: {star_balance.amount}⭐\n{name} ({user_id})\nДобавлен в /pending")
                await bot.send_message(ADMIN_ID,
                    f"⚠️ Недостаточно звёзд!\n\n💫 {star_balance.amount}⭐\n👤 {name} ({user_id})\nДобавлен в /pending"
                )
            else:
                try:
                    await bot.send_gift(user_id=user_id, gift_id=random.choice(WIN_GIFT_IDS))
                    await send_log(bot, f"🎁 Подарок отправлен\n\n{name} ({user_id})\n💫 {star_balance.amount - 15}⭐")
                    await bot.send_message(ADMIN_ID, f"✅ Подарок отправлен!\n\n👤 {name} ({user_id})\n💫 {star_balance.amount - 15}⭐")
                except Exception as e:
                    error_text = str(e)
                    await db.add_pending_gift(user_id, name, WIN_GIFT_IDS[0], f"ошибка: {error_text}")
                    await send_log(bot, f"❌ Ошибка подарка\n\n{name} ({user_id})\n{error_text}")
                    await bot.send_message(ADMIN_ID, f"❌ Ошибка подарка\n\n👤 {name} ({user_id})\n📛 {error_text}\nДобавлен в /pending")
        except Exception as e:
            logger.warning("get_my_star_balance failed: %s", e)
            await db.add_pending_gift(user_id, name, WIN_GIFT_IDS[0], "не удалось проверить баланс")
            await bot.send_message(ADMIN_ID, f"⚠️ Не удалось проверить баланс\n\n👤 {name} ({user_id})\nДобавлен в /pending")

        await db.update_user(user_id, message.chat.id, name, START_CHANCE, 0, last_bonus)
    else:
        new_chance = min(round(chance + chance_step, 3), MAX_CHANCE)
        await db.update_user(user_id, message.chat.id, name, new_chance, msg_count + 1, last_bonus)

# =========================
# DAILY RESET TASK
# =========================

async def daily_reset_task(bot: Bot) -> None:
    tz = pytz.timezone("Europe/Moscow")
    while True:
        now = datetime.now(tz)
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())
        await db.clear_old_daily()
        await send_log(bot, "🗑 Дневная статистика сброшена (00:00 МСК)")

# =========================
# MAIN
# =========================

async def main() -> None:
    if not TOKEN:
        raise ValueError("BOT_TOKEN не задан в .env")
    await db.init()
    bot = Bot(token=TOKEN)
    dp  = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(daily_reset_task(bot))
    asyncio.create_task(casino_timeout_checker(bot))
    logger.info("Бот запущен")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    asyncio.run(main())
