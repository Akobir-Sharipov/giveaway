import asyncio
import logging
import random
import time
from datetime import datetime, timedelta

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
ADMIN_ID     = int(os.getenv("ADMIN_ID", "0"))

COOLDOWN_SECONDS   = 1
START_CHANCE       = 0.1
STEP               = 0.002
MAX_CHANCE         = 100.0
BONUS_COOLDOWN     = 43200
REF_BONUS          = 1.0
VALID_REF_MESSAGES = 10

BAN_MESSAGE = "🚫егор иди нахуй"

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
    "5170233102089322756",
]

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

    async def get_ban_list(self) -> list[tuple]:
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

    async def get_pending_gifts(self) -> list[tuple]:
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
    # USER STATS
    # --------------------------------------------------

    async def get_user(self, user_id: int, chat_id: int) -> tuple[float, int, float]:
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

    async def get_top(self, chat_id: int, limit: int = 5) -> list[tuple]:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name, chance, msg_count FROM user_stats WHERE chat_id=? ORDER BY chance DESC LIMIT ?",
                (chat_id, limit)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # INVITE LINKS
    # --------------------------------------------------

    async def get_invite_link(self, user_id: int) -> str | None:
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

    async def get_owner_by_link(self, invite_link: str) -> int | None:
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

    async def get_referral(self, invited_user_id: int) -> tuple | None:
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

    async def get_wins_top(self, chat_id: int, limit: int = 10) -> list[tuple]:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT user_name, COUNT(*) as cnt FROM wins WHERE chat_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?",
                (chat_id, limit)
            ) as cur:
                return await cur.fetchall()

    # --------------------------------------------------
    # REF TOP
    # --------------------------------------------------

    async def get_refs_top(self, limit: int = 10) -> list[tuple]:
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

    async def get_daily_top(self, chat_id: int, limit: int = 10) -> list[tuple]:
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
                DO UPDATE SET
                    msg_count = msg_count + ?,
                    user_name = excluded.user_name
            """, (user_id, chat_id, user_name, today, amount, amount))
            await db.commit()

    async def remove_day_messages(self, user_id: int, chat_id: int, amount: int):
        today = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE daily_stats
                SET msg_count = MAX(msg_count - ?, 0)
                WHERE user_id=? AND chat_id=? AND date=?
            """, (amount, user_id, chat_id, today))
            await db.commit()

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
                        f"⚠️ Недостаточно звёзд для реф. награды!\n\n"
                        f"💫 Баланс: {star_balance.amount}⭐\n"
                        f"👤 {inv_name} ({inviter_id})\n"
                        f"🏆 {reward}\nДобавлен в /pending"
                    )
                else:
                    await bot.send_gift(user_id=inviter_id, gift_id=gift_id)
                    await send_log(bot, f"🎁 Реф. подарок отправлен\n\n{inv_name} ({inviter_id})\n{reward}")
            except Exception as e:
                logger.warning("send ref gift failed: %s", e)
                await db.add_pending_gift(inviter_id, inv_name, gift_id, f"реф. награда {reward} — ошибка: {e}")
                await bot.send_message(ADMIN_ID,
                    f"❌ Не удалось отправить реф. подарок\n\n"
                    f"👤 {inv_name} ({inviter_id})\n"
                    f"🏆 {reward}\n📛 {e}\nДобавлен в /pending"
                )

# =========================
# ROUTER
# =========================

router    = Router()
cooldowns: TTLCache = TTLCache(maxsize=50_000, ttl=COOLDOWN_SECONDS)

def start_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="ref")],
        [InlineKeyboardButton(text="📊 Реферальная статистика", callback_data="refstats")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# =========================
# PRIVATE — /start
# =========================

@router.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message) -> None:
    if await db.is_banned(message.from_user.id):
        await message.answer(BAN_MESSAGE)
        return

    await message.answer(
        "👋 Добро пожаловать!\n\n"
        "Выберите действие:",
        reply_markup=start_keyboard()
    )


@router.callback_query(F.data == "ref")
async def ref_callback(callback: CallbackQuery, bot: Bot):
    await cmd_ref(callback.message, bot)
    await callback.answer()


@router.callback_query(F.data == "refstats")
async def refstats_callback(callback: CallbackQuery):
    await cmd_refstats(callback.message)
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
# PRIVATE — /say (только для админа)
# =========================

@router.message(Command("say"), F.chat.type == "private")
async def cmd_say(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("Использование: /say текст сообщения")
        return
    text = args[1].strip()
    try:
        await bot.send_message(MAIN_CHAT_ID, text)
        await message.answer("✅ Сообщение отправлено в чат.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить сообщение.\n\n📛 {e}")

# =========================
# PRIVATE — /vip (только для админа)
# =========================

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
        await message.answer("❌ Укажи числовой ID пользователя.")
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
        await message.answer("❌ Укажи числовой ID пользователя.")
        return
    await db.remove_vip(user_id)
    await message.answer(f"✅ VIP статус снят с пользователя {user_id}.")

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

# =========================
# PRIVATE — /ban (только для админа)
# =========================

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
        await message.answer("❌ Укажи числовой ID пользователя.")
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
        await message.answer("❌ Укажи числовой ID пользователя.")
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
    text = "🚫 Заблокированные пользователи:\n\n"
    for uid, name, reason in ban_list:
        text += f"• {name} ({uid})"
        if reason:
            text += f" — {reason}"
        text += "\n"
    await message.answer(text)

# =========================
# PRIVATE — /addmsgs (только для админа)
# =========================

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
    inv_name   = await db.get_user_name(user_id)
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
    inv_name   = await db.get_user_name(user_id)
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
        amount = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи ID и количество.")
        return
    name = await db.get_user_name(user_id)
    await db.add_day_messages(user_id, MAIN_CHAT_ID, name, amount)
    await message.answer(
        f"✅ Добавлено {amount} сообщений в daytop\n"
        f"👤 {name} ({user_id})"
    )

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
        amount = int(args[2])
    except ValueError:
        await message.answer("❌ Укажи ID и количество.")
        return
    await db.remove_day_messages(user_id, MAIN_CHAT_ID, amount)
    await message.answer(
        f"✅ Убрано {amount} сообщений из daytop\n"
        f"👤 {user_id}"
    )

# =========================
# PRIVATE — /addrefs (только для админа)
# =========================

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
        + (f"\n📈 Шанс: {new_chance:.3f}% (+{REF_BONUS * added:.1f}%)" if new_chance else "")
    )

    for level, reward in REF_REWARDS.items():
        if valid_refs >= level and prev_refs < level:
            gift_ids = REF_GIFT_IDS.get(level, [])
            gift_id  = random.choice(gift_ids) if gift_ids else None
            await send_log(bot, f"🎁 Реферальная награда\n\n👤 {inv_name} ({user_id})\n🏆 {reward}\n👥 Рефералов: {valid_refs}")
            await bot.send_message(ADMIN_ID, f"🎁 Реферальная награда\n\n👤 {inv_name} ({user_id})\n🏆 {reward}\n👥 Рефералов: {valid_refs}")
            try:
                await bot.send_message(user_id, f"🎉 Ты достиг {level} рефералов!\n🏆 Награда: {reward}\nПодарок уже отправлен тебе в личку!")
            except Exception:
                pass
            if gift_id:
                try:
                    star_balance = await bot.get_my_star_balance()
                    cost = int(reward.replace("⭐", "").strip())
                    if star_balance.amount < cost:
                        await db.add_pending_gift(user_id, inv_name, gift_id, f"реф. награда {reward}")
                        await bot.send_message(ADMIN_ID, f"⚠️ Недостаточно звёзд!\n💫 Баланс: {star_balance.amount}⭐\n👤 {inv_name}\n🏆 {reward}\nДобавлен в /pending")
                    else:
                        await bot.send_gift(user_id=user_id, gift_id=gift_id)
                        await send_log(bot, f"🎁 Реф. подарок отправлен\n\n{inv_name} ({user_id})\n{reward}")
                except Exception as e:
                    await db.add_pending_gift(user_id, inv_name, gift_id, f"реф. награда {reward} — ошибка: {e}")
                    await bot.send_message(ADMIN_ID, f"❌ Ошибка реф. подарка\n\n👤 {inv_name}\n🏆 {reward}\n📛 {e}\nДобавлен в /pending")

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
        f"👥 Осталось валидных: {valid_refs}"
    )

# =========================
# PRIVATE — /balance (только для админа)
# =========================

@router.message(Command("balance"), F.chat.type == "private")
async def cmd_balance(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    try:
        star_balance = await bot.get_my_star_balance()
        await message.answer(f"💫 Баланс бота: {star_balance.amount} звёзд")
    except Exception as e:
        logger.warning("get_my_star_balance failed: %s", e)
        await message.answer("❌ Не удалось получить баланс.")

# =========================
# PRIVATE — /popolnit (только для админа)
# =========================

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
        stars = payment.total_amount
        await message.answer(f"✅ Оплата прошла успешно!\n💫 Зачислено: {stars} звёзд на баланс бота.")
        await send_log(bot, f"💫 Пополнение баланса\n\n👤 Админ: {message.from_user.id}\n⭐ Сумма: {stars} звёзд")

# =========================
# PRIVATE — /sendgift (только для админа)
# =========================

@router.message(Command("sendgift"), F.chat.type == "private")
async def cmd_sendgift(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /sendgift user_id gift_id\n\nПример: /sendgift 123456789 5170233102089322756")
        return
    try:
        user_id = int(args[1])
        gift_id = args[2]
    except ValueError:
        await message.answer("❌ Укажи числовой ID пользователя и ID подарка.")
        return
    try:
        star_balance = await bot.get_my_star_balance()
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
        await send_log(bot, f"🎁 Ручная выдача подарка\n\n👤 {user_id}\n📦 {gift_id}\n💫 Баланс: {star_balance.amount}⭐")
        await message.answer(f"✅ Подарок успешно отправлен!\n\n👤 ID: {user_id}\n📦 Gift ID: {gift_id}\n💫 Баланс: {star_balance.amount}⭐")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить подарок!\n\n📛 Причина: {e}")

# =========================
# PRIVATE — /pending (только для админа)
# =========================

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
        text += f"#{g_id} | {user_name} ({uid})\n📦 {gift_id}\n📝 {reason} | {date}\n👉 /deliver {g_id}\n\n"
    await message.answer(text)

# =========================
# PRIVATE — /deliver (только для админа)
# =========================

@router.message(Command("deliver"), F.chat.type == "private")
async def cmd_deliver(message: Message, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /deliver id\nID берёшь из /pending")
        return
    try:
        gift_db_id = int(args[1])
    except ValueError:
        await message.answer("❌ Укажи числовой ID из /pending")
        return
    gifts = await db.get_pending_gifts()
    gift  = next((g for g in gifts if g[0] == gift_db_id), None)
    if not gift:
        await message.answer("❌ Подарок не найден. Возможно уже выдан.")
        return
    _, user_id, user_name, gift_id, reason, _ = gift
    try:
        star_balance = await bot.get_my_star_balance()
        await bot.send_gift(user_id=user_id, gift_id=gift_id)
        await db.remove_pending_gift(gift_db_id)
        await send_log(bot, f"🎁 Отложенный подарок выдан\n\n{user_name} ({user_id})\n💫 Остаток: {star_balance.amount}⭐")
        await message.answer(f"✅ Подарок успешно выдан!\n\n👤 {user_name} ({user_id})\n💫 Баланс: {star_balance.amount}⭐")
    except Exception as e:
        await message.answer(f"❌ Не удалось выдать подарок!\n\n📛 Причина: {e}\n\nПопробуй позже или пополни баланс через /popolnit")

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
    await message.reply(
        f"📊 Статистика:\n\n"
        f"📈 Шанс: {chance:.3f}%\n"
        f"💬 Сообщений: {msg_count}\n"
        f"🏆 Побед за всё время: {wins}\n"
        f"👥 Валидных рефералов: {valid_refs}"
    )

# =========================
# GROUP — /top
# =========================

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

# =========================
# GROUP — /winstop
# =========================

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
    text = "🏆 Топ победителей за всё время:\n\n"
    for i, (name, cnt) in enumerate(top, start=1):
        text += f"{i}. {name} — {cnt} поб.\n"
    await message.reply(text)

# =========================
# GROUP — /reftop
# =========================

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
    text = "👥 Топ по рефералам за всё время:\n\n"
    for i, (_, name, cnt) in enumerate(top, start=1):
        text += f"{i}. {name} — {cnt} реф.\n"
    await message.reply(text)

# =========================
# GROUP — /bonus
# =========================

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
    now = time.time()
    if now - last_bonus < BONUS_COOLDOWN:
        hours_left = int((BONUS_COOLDOWN - (now - last_bonus)) // 3600)
        mins_left  = int((BONUS_COOLDOWN - (now - last_bonus)) % 3600 // 60)
        await message.reply(f"⏳ Бонус уже получен.\nСледующий через: {hours_left} ч. {mins_left} мин.")
        return
    bonus_amount = round(random.uniform(0.05, 0.20), 3)
    new_chance   = min(round(chance + bonus_amount, 3), MAX_CHANCE)
    await db.update_user(user_id, message.chat.id, name, new_chance, msg_count, now)
    await message.reply(f"🎁 Бонус: +{bonus_amount:.3f}%\n📈 Новый шанс: {new_chance:.3f}%")

# =========================
# GROUP — /daytop
# =========================

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
    if not inviter_id:
        if invite_link.creator and invite_link.creator.id != member.id:
            inviter_id = invite_link.creator.id
        else:
            return
    if inviter_id == member.id:
        return
    await db.add_referral(member.id, inviter_id)
    logger.info("Новый реферал: %s пришёл по ссылке %s (владелец %s)", member.id, link_str, inviter_id)
    await send_log(bot,
        f"🔗 Новый реферал зафиксирован\n\n"
        f"👤 Пришёл: {display_name(member)} ({member.id})\n"
        f"👥 Пригласил: {inviter_id}\n"
        f"⏳ Нужно сообщений: {VALID_REF_MESSAGES}"
    )

# =========================
# MAIN GROUP HANDLER
# =========================

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def group_handler(message: Message, bot: Bot) -> None:
    if message.chat.id != MAIN_CHAT_ID:
        if message.chat.id != LOG_CHAT_ID:
            try:
                await bot.leave_chat(message.chat.id)
            except Exception as e:
                logger.warning("leave_chat failed: %s", e)
        return

    if not message.from_user or message.from_user.is_bot:
        return

    msg_text = message.text or message.caption
    if not msg_text or msg_text.startswith("/"):
        return

    user_id   = message.from_user.id
    name      = display_name(message.from_user)
    cache_key = (user_id, message.chat.id)

    if await db.is_banned(user_id):
        return

    if cache_key in cooldowns:
        return
    cooldowns[cache_key] = True

    # Дневная статистика
    await db.increment_daily(user_id, message.chat.id, name)

    chance, msg_count, last_bonus = await db.get_user(user_id, message.chat.id)

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
    is_vip   = await db.is_vip(user_id)
    msg_step = 2 if is_vip else 1

    if msg_count < 150:
        is_win = False
    else:
        is_win = chance >= MAX_CHANCE or random.uniform(0, 500) <= chance

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
                await db.add_pending_gift(user_id, name, "5170233102089322756", "победа")
                await send_log(bot, f"⚠️ Недостаточно звёзд!\n\nБаланс: {star_balance.amount}⭐\nПобедитель: {name} ({user_id})\nДобавлен в /pending")
                await bot.send_message(ADMIN_ID,
                    f"⚠️ Недостаточно звёзд!\n\n💫 Баланс: {star_balance.amount}⭐\n👤 {name} ({user_id})\n\nПодарок добавлен в /pending"
                )
            else:
                try:
                    await bot.send_gift(user_id=user_id, random.choice(WIN_GIFT_IDS))
                    await send_log(bot, f"🎁 Подарок отправлен\n\n{name} ({user_id})\n💫 Остаток: {star_balance.amount - 15}⭐")
                    await bot.send_message(ADMIN_ID,
                        f"✅ Подарок успешно отправлен!\n\n👤 {name} ({user_id})\n💫 Остаток: {star_balance.amount - 15}⭐"
                    )
                except Exception as e:
                    error_text = str(e)
                    await db.add_pending_gift(user_id, name, "5170233102089322756", f"ошибка: {error_text}")
                    await send_log(bot, f"❌ Ошибка отправки подарка\n\n{name} ({user_id})\n{error_text}\nДобавлен в /pending")
                    await bot.send_message(ADMIN_ID,
                        f"❌ Не удалось отправить подарок!\n\n👤 {name} ({user_id})\n📛 {error_text}\n\nДобавлен в /pending"
                    )
        except Exception as e:
            logger.warning("get_my_star_balance failed: %s", e)
            await db.add_pending_gift(user_id, name, "5170233102089322756", "не удалось проверить баланс")
            await bot.send_message(ADMIN_ID,
                f"⚠️ Не удалось проверить баланс звёзд\n\n👤 {name} ({user_id})\nДобавлен в /pending"
            )

        await db.update_user(user_id, message.chat.id, name, START_CHANCE, 0, last_bonus)
    else:
        new_chance = min(round(chance + (STEP * msg_step), 3), MAX_CHANCE)
        await db.update_user(user_id, message.chat.id, name, new_chance, msg_count + msg_step, last_bonus)

# =========================
# DAILY RESET TASK
# =========================

async def daily_reset_task(bot: Bot) -> None:
    tz = pytz.timezone("Europe/Moscow")
    while True:
        now = datetime.now(tz)
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = (next_midnight - now).total_seconds()
        await asyncio.sleep(wait_seconds)
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
    logger.info("Бот запущен")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    asyncio.run(main())
