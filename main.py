# main.py
# EasyGo MVP: локальная служба доставки (пока только Dunpo)
# Стек: python-telegram-bot v20+ (async), Google Sheets (orders/couriers/events)
#
# ENV:
#   BOT_TOKEN=...
#   SHEET_ID=...
#   ADMIN_IDS=123,456
#   GOOGLE_SERVICE_ACCOUNT_FILE=C:\path\to\service_account.json
# Optional:
#   PORT=8080
#
# MVP:
# - Старт -> выбор города (Asan/Dunpo/Sinchang), но работает только Dunpo
# - Затем выбор роли (клиент/курьер)
# - Клиент: пошаговый заказ, адреса только текстом и только на корейском
# - Перед созданием заказа: выбор тарифа
#     1) доставка в районе Dunpo - фикс 4000
#     2) доставка в другие районы - клиент вводит свою цену
# - Курьер: "Стать курьером" + ручное одобрение админом
# - Оповещения: одобренные курьеры получают новые заказы, заказ доступен до взятия
# - Курьер после взятия нажимает "Выезжаю/в пути" -> клиент и админ видят статус
# - Завершение: кнопка "Заказ выполнен" -> обязательно скриншот -> отправка клиенту и админу
# - Клиент: "Статус доставки" (по активному заказу) + "Мои заказы" (с фильтром)
# - Клиент может отозвать заказ только если он NEW (никто не взял)

import os
import re
import json
import asyncio
import logging
import requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from urllib.parse import quote
import asyncio
from telegram.error import Conflict

from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("easygo_delivery")


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not SHEET_ID:
    raise RuntimeError("SHEET_ID is not set")
if not ADMIN_IDS_RAW:
    raise RuntimeError("ADMIN_IDS is not set (comma separated ids)")

ADMIN_IDS = set()
for part in ADMIN_IDS_RAW.split(","):
    part = part.strip()
    if part:
        ADMIN_IDS.add(int(part))

DEFAULT_PRICE_KRW = 4000
PRICE_PER_KM_KRW = 1200
GOOGLE_PRICE_PER_KM = 1200
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")


LOC_DUNPO = "Dunpo"
LOC_ASAN = "Asan"
LOC_SINCHANG = "Sinchang"


# =========================
# ROLE + STATES
# =========================
USER_ROLE_KEY = "role"
USER_LOCATION_KEY = "location"

ROLE_UNKNOWN = "ROLE_UNKNOWN"
ROLE_CLIENT = "ROLE_CLIENT"
ROLE_COURIER = "ROLE_COURIER"

CLIENT_STATE_KEY = "client_state"
COURIER_STATE_KEY = "courier_state"

# client states
C_NONE = "C_NONE"
C_PRICE_RECOMMEND = "C_PRICE_RECOMMEND"   # показ рекомендованной цены + выбор
C_PRICE_FINAL = "C_PRICE_FINAL"           # ручной ввод цены
C_PICKUP = "C_PICKUP"
C_DROP = "C_DROP"
C_PRICE_ZONE = "C_PRICE_ZONE"
C_DOOR = "C_DOOR"
C_TYPE = "C_TYPE"
C_TYPE_OTHER = "C_TYPE_OTHER"
C_TIME = "C_TIME"
C_TIME_CUSTOM = "C_TIME_CUSTOM"
C_CONTACT = "C_CONTACT"
C_CONFIRM = "C_CONFIRM"

# courier states
K_NONE = "K_NONE"
K_APPLY_NAME = "K_APPLY_NAME"
K_APPLY_PHONE = "K_APPLY_PHONE"
K_APPLY_TRANSPORT = "K_APPLY_TRANSPORT"
K_AWAITING_PROOF = "K_AWAITING_PROOF"

# order status
ORDER_NEW = "NEW"
ORDER_TAKEN = "TAKEN"
ORDER_DONE_PENDING = "DONE_PENDING_PROOF"
ORDER_DONE = "DONE"
ORDER_CANCELED = "CANCELED"
ORDER_PROBLEM = "PROBLEM"  # адрес некорректен (проблемный заказ)

# courier status
COURIER_PENDING = "PENDING"
COURIER_APPROVED = "APPROVED"
COURIER_REJECTED = "REJECTED"

ORDER_EN_ROUTE = "EN_ROUTE"
ORDER_PICKED_UP = "PICKED_UP"

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def role_for_log(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get(USER_ROLE_KEY, ROLE_UNKNOWN)


# =========================
# TG RETRY
# =========================
async def tg_retry(call, tries: int = 6, base_sleep: float = 0.7):
    last_exc = None
    for attempt in range(tries):
        try:
            return await call()
        except RetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 0.2)
        except (TimedOut, NetworkError) as e:
            last_exc = e
            await asyncio.sleep(base_sleep * (2 ** attempt))
        except BadRequest:
            raise
    if last_exc:
        raise last_exc

# =========================
# ONE-MESSAGE UI CORE
# =========================
UI_MSG_ID_KEY = "ui_msg_id"
UI_RESET_KEY = "ui_reset_in_progress"

from telegram.error import BadRequest

async def ui_render(context, chat_id: int, text: str, reply_markup=None, **kwargs):
    
    if context.user_data.get(UI_RESET_KEY) and text != HOME_TEXT:
        log.info("UI_RENDER SKIPPED (reset in progress)")
        return

    last_text = context.user_data.get("_ui_last_text")
    if last_text == text:
        log.info("UI_RENDER SKIP (same text)")
        return
    context.user_data["_ui_last_text"] = text
    
    # 🔒 если идет reset — НИКТО не рисует UI
    if context.user_data.get(UI_RESET_KEY):
        log.info("UI_RENDER SKIPPED (reset in progress)")
        return

    msg_id = context.user_data.get(UI_MSG_ID_KEY)

    if msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=reply_markup,
                **kwargs
            )
            return
        except BadRequest:
            context.user_data.pop(UI_MSG_ID_KEY, None)
        except Exception:
            log.exception("Unexpected UI edit error")
            context.user_data.pop(UI_MSG_ID_KEY, None)

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        **kwargs
    )
    context.user_data[UI_MSG_ID_KEY] = msg.message_id


async def ui_clear_buttons(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """
    Иногда полезно убрать старые кнопки у текущего UI-сообщения.
    """
    msg_id = context.user_data.get(UI_MSG_ID_KEY)
    if not msg_id:
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=None
        )
    except Exception:
        pass


# =========================
# ADDRESS VALIDATION
# Only Korean text (Hangul required, no latin/cyrillic)
# =========================
_re_hangul = re.compile(r"[가-힣]")
_re_latin = re.compile(r"[A-Za-z]")
_re_cyr = re.compile(r"[А-Яа-яЁё]")


def is_korean_address(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if not t:
        return False
    if _re_latin.search(t) or _re_cyr.search(t):
        return False
    if not _re_hangul.search(t):
        return False
    return True


def parse_price_krw(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.strip().replace(" ", "")
    if not t.isdigit():
        return None
    try:
        v = int(t)
    except Exception:
        return None
    if v < 1000 or v > 300000:
        return None
    return v


def naver_map_search_url(addr_ko: str) -> str:
    q = quote((addr_ko or "").strip())
    # Telegram кнопки принимают только http/https, поэтому используем Naver Map web search
    return f"https://map.naver.com/v5/search/{q}"


# =========================
# GOOGLE SHEETS
# =========================
ORDERS_SHEET = "orders"
COURIERS_SHEET = "couriers"
EVENTS_SHEET = "events"
VISITS_SHEET = "visits"

VISITS_HEADERS = [
    "ts",
    "user_tg_id",
    "username",
    "role",
    "location",
    "event",
    "last_seen",
]

ORDERS_HEADERS = [
    "order_id",                   # A
    "created_at",                 # B
    "location",                   # C
    "price_krw",                  # D
    "status",                     # E
    "client_tg_id",               # F
    "client_username",            # G
    "recipient_contact_text",     # H
    "pickup_address_ko",          # I
    "drop_address_ko",            # J
    "door_code",                  # K
    "delivery_type",              # L
    "delivery_time_type",         # M
    "delivery_time_text",         # N
    "taken_at",                   # O
    "courier_tg_id",              # P
    "courier_name",               # Q
    "courier_phone",              # R
    "in_progress_at",             # S
    "done_requested_at",          # T
    "completed_at",               # U
    "proof_image_file_id",        # V
    "proof_image_message_id",     # W
    "canceled_at",                # X
    "canceled_by",                # Y
]

COURIERS_HEADERS = [
    "courier_tg_id",
    "username",
    "name",
    "phone",
    "transport",
    "status",
    "applied_at",
    "approved_at",
    "rejected_at",
]

EVENTS_HEADERS = [
    "ts",
    "user_tg_id",
    "role",
    "event_type",
    "order_id",
    "meta",
]


def build_sheets_service():
    json_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    if json_str:
        info = json.loads(json_str)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif json_file:
        creds = service_account.Credentials.from_service_account_file(json_file, scopes=scopes)
    else:
        raise RuntimeError("Set GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON")

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


class SheetsStore:

    def log_visit(
        self,
        user_tg_id: int,
        username: str,
        role: str,
        location: str,
        event: str,
    ):
        ts = now_ts()
        row = [
            ts,
            str(user_tg_id),
            username or "",
            role,
            location,
            event,
            ts,  # last_seen = текущий ts
        ]
        try:
            self.append_row(VISITS_SHEET, row)
        except HttpError as e:
            log.warning("Failed to log visit: %s", e)


    def __init__(self, service, sheet_id: str):
        self.service = service
        self.sheet_id = sheet_id
        self.order_row: Dict[str, int] = {}
        self.courier_row: Dict[str, int] = {}
        self.last_order_num = 0

    def _get_spreadsheet(self) -> Dict[str, Any]:
        return self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()

    def _sheet_exists(self, spreadsheet: Dict[str, Any], title: str) -> bool:
        for sh in spreadsheet.get("sheets", []):
            if sh.get("properties", {}).get("title") == title:
                return True
        return False

    def _add_sheet(self, title: str):
        req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body=req
        ).execute()

    def _write_headers_if_empty(self, title: str, headers: List[str]):
        rng = f"{title}!A1:Z1"
        resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheet_id,
            range=rng
        ).execute()
        values = resp.get("values", [])
        if not values:
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=f"{title}!A1",
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()

    def ensure_structure(self):
        ss = self._get_spreadsheet()
        if not self._sheet_exists(ss, ORDERS_SHEET):
            self._add_sheet(ORDERS_SHEET)
        if not self._sheet_exists(ss, COURIERS_SHEET):
            self._add_sheet(COURIERS_SHEET)
        if not self._sheet_exists(ss, EVENTS_SHEET):
            self._add_sheet(EVENTS_SHEET)
        if not self._sheet_exists(ss, VISITS_SHEET):
            self._add_sheet(VISITS_SHEET)

        self._write_headers_if_empty(ORDERS_SHEET, ORDERS_HEADERS)
        self._write_headers_if_empty(COURIERS_SHEET, COURIERS_HEADERS)
        self._write_headers_if_empty(EVENTS_SHEET, EVENTS_HEADERS)
        self._write_headers_if_empty(VISITS_SHEET, VISITS_HEADERS)

    def _read_range(self, rng: str) -> List[List[str]]:
        resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheet_id,
            range=rng
        ).execute()
        return resp.get("values", [])

    def warm_cache(self):
        ids = self._read_range(f"{ORDERS_SHEET}!A2:A")
        for idx, row in enumerate(ids, start=2):
            oid = (row[0] if row else "").strip()
            if not oid:
                continue
            self.order_row[oid] = idx
            try:
                n = int(oid)
                if n > self.last_order_num:
                    self.last_order_num = n
            except Exception:
                pass

        cids = self._read_range(f"{COURIERS_SHEET}!A2:A")
        for idx, row in enumerate(cids, start=2):
            cid = (row[0] if row else "").strip()
            if not cid:
                continue
            self.courier_row[cid] = idx

    def append_row(self, title: str, row: List[Any]):
        self.service.spreadsheets().values().append(
            spreadsheetId=self.sheet_id,
            range=f"{title}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()

    def update_row(self, title: str, row_index: int, row: List[Any]):
        rng = f"{title}!A{row_index}"
        self.service.spreadsheets().values().update(
            spreadsheetId=self.sheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [row]},
        ).execute()

    def log_event(self, user_tg_id: int, role: str, event_type: str, order_id: str = "", meta: str = ""):
        row = [now_ts(), str(user_tg_id), role, event_type, str(order_id), meta]
        try:
            self.append_row(EVENTS_SHEET, row)
        except HttpError as e:
            log.warning("Failed to log event: %s", e)

    def next_order_id(self) -> str:
        self.last_order_num += 1
        return str(self.last_order_num)

    def upsert_courier(self, courier: Dict[str, Any]):
        cid = str(courier["courier_tg_id"])
        row = [
            cid,
            courier.get("username", ""),
            courier.get("name", ""),
            courier.get("phone", ""),
            courier.get("transport", ""),
            courier.get("status", ""),
            courier.get("applied_at", ""),
            courier.get("approved_at", ""),
            courier.get("rejected_at", ""),
        ]
        if cid in self.courier_row:
            self.update_row(COURIERS_SHEET, self.courier_row[cid], row)
        else:
            self.append_row(COURIERS_SHEET, row)
            self.courier_row = {}
            self.warm_cache()

    def insert_order(self, order: Dict[str, Any]):
        oid = str(order["order_id"])
        row = [
            oid,
            order.get("created_at", ""),
            order.get("location", ""),
            str(order.get("price_krw", "")),
            order.get("status", ""),
            str(order.get("client_tg_id", "")),
            order.get("client_username", ""),
            order.get("recipient_contact_text", ""),
            order.get("pickup_address_ko", ""),
            order.get("drop_address_ko", ""),
            order.get("door_code", ""),
            order.get("delivery_type", ""),
            order.get("delivery_time_type", ""),
            order.get("delivery_time_text", ""),
            order.get("taken_at", ""),
            str(order.get("courier_tg_id", "")),
            order.get("courier_name", ""),
            order.get("courier_phone", ""),
            order.get("in_progress_at", ""),
            order.get("done_requested_at", ""),
            order.get("completed_at", ""),
            order.get("proof_image_file_id", ""),
            order.get("proof_image_message_id", ""),
            order.get("canceled_at", ""),
            order.get("canceled_by", ""),
        ]
        self.append_row(ORDERS_SHEET, row)
        self.order_row = {}
        self.warm_cache()

    def update_order(self, order: Dict[str, Any]):
        oid = str(order["order_id"])
        if oid not in self.order_row:
            self.order_row = {}
            self.warm_cache()

        if oid not in self.order_row:
            self.insert_order(order)
            return

        row_index = self.order_row[oid]
        row = [
            oid,
            order.get("created_at", ""),
            order.get("location", ""),
            str(order.get("price_krw", "")),
            order.get("status", ""),
            str(order.get("client_tg_id", "")),
            order.get("client_username", ""),
            order.get("recipient_contact_text", ""),
            order.get("pickup_address_ko", ""),
            order.get("drop_address_ko", ""),
            order.get("door_code", ""),
            order.get("delivery_type", ""),
            order.get("delivery_time_type", ""),
            order.get("delivery_time_text", ""),
            order.get("taken_at", ""),
            str(order.get("courier_tg_id", "")),
            order.get("courier_name", ""),
            order.get("courier_phone", ""),
            order.get("in_progress_at", ""),
            order.get("done_requested_at", ""),
            order.get("completed_at", ""),
            order.get("proof_image_file_id", ""),
            order.get("proof_image_message_id", ""),
            order.get("canceled_at", ""),
            order.get("canceled_by", ""),
        ]
        self.update_row(ORDERS_SHEET, row_index, row)

    def load_all_couriers(self) -> List[Dict[str, str]]:
        values = self._read_range(f"{COURIERS_SHEET}!A2:I")
        out: List[Dict[str, str]] = []
        for r in values:
            rr = r + [""] * (9 - len(r))
            cid = rr[0].strip()
            if not cid:
                continue
            out.append({
                "courier_tg_id": cid,
                "username": rr[1],
                "name": rr[2],
                "phone": rr[3],
                "transport": rr[4],
                "status": rr[5],
                "applied_at": rr[6],
                "approved_at": rr[7],
                "rejected_at": rr[8],
            })
        return out

    def load_all_orders(self) -> List[Dict[str, str]]:
        values = self._read_range(f"{ORDERS_SHEET}!A2:Y")
        out: List[Dict[str, str]] = []
        for r in values:
            rr = r + [""] * (25 - len(r))
            oid = rr[0].strip()
            if not oid:
                continue
            out.append({
                "order_id": rr[0],
                "created_at": rr[1],
                "location": rr[2],
                "price_krw": rr[3],
                "status": rr[4],
                "client_tg_id": rr[5],
                "client_username": rr[6],
                "recipient_contact_text": rr[7],
                "pickup_address_ko": rr[8],
                "drop_address_ko": rr[9],
                "door_code": rr[10],
                "delivery_type": rr[11],
                "delivery_time_type": rr[12],
                "delivery_time_text": rr[13],
                "taken_at": rr[14],
                "courier_tg_id": rr[15],
                "courier_name": rr[16],
                "courier_phone": rr[17],
                "in_progress_at": rr[18],
                "done_requested_at": rr[19],
                "completed_at": rr[20],
                "proof_image_file_id": rr[21],
                "proof_image_message_id": rr[22],
                "canceled_at": rr[23],
                "canceled_by": rr[24],
            })
        return out


# =========================
# DATA
# =========================
@dataclass
class CourierProfile:
    courier_tg_id: int
    username: str
    name: str
    phone: str
    transport: str
    status: str
    applied_at: str = ""
    approved_at: str = ""
    rejected_at: str = ""


@dataclass
class Order:
    order_id: str
    created_at: str
    location: str
    price_krw: int
    status: str

    client_tg_id: int
    client_username: str
    recipient_contact_text: str

    pickup_address_ko: str
    drop_address_ko: str
    door_code: str

    delivery_type: str
    delivery_type_other_text: str

    delivery_time_type: str
    delivery_time_text: str

    taken_at: str = ""
    courier_tg_id: int = 0
    courier_name: str = ""
    courier_phone: str = ""

    in_progress_at: str = ""

    done_requested_at: str = ""
    completed_at: str = ""
    proof_image_file_id: str = ""
    proof_image_message_id: str = ""

    canceled_at: str = ""
    canceled_by: str = ""


ORDERS: Dict[str, Order] = {}
COURIERS: Dict[int, CourierProfile] = {}

SHEETS: Optional[SheetsStore] = None
ORDER_LOCK = asyncio.Lock()


def courier_is_approved(courier_id: int) -> bool:
    prof = COURIERS.get(courier_id)
    return bool(prof and prof.status == COURIER_APPROVED)


def get_active_order_for_courier(courier_id: int) -> Optional["Order"]:
    active_statuses = (
        ORDER_TAKEN,
        ORDER_EN_ROUTE,
        ORDER_PICKED_UP,
        ORDER_DONE_PENDING,
    )
    for o in ORDERS.values():
        if o.courier_tg_id == courier_id and o.status in active_statuses:
            return o
    return None


# =========================
# UI (KEYBOARDS)
# =========================

def kb_back_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="home:back")]
    ])

def kb_home_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Старт", callback_data="home:start")],
        [InlineKeyboardButton("📜 Правила сервиса", callback_data="home:rules")],
        [InlineKeyboardButton("🧾 Как сделать заказ", callback_data="home:client")],
        [InlineKeyboardButton("🛵 Как принять заказ", callback_data="home:courier")],
    ])



def kb_back_to_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="info:back")]
    ])


def kb_location() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Асан", callback_data=f"loc:{LOC_ASAN}"),
        InlineKeyboardButton("Дунпо", callback_data=f"loc:{LOC_DUNPO}"),
        InlineKeyboardButton("Синчанг", callback_data=f"loc:{LOC_SINCHANG}"),
    ]])


def kb_role() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🙋 Я клиент", callback_data="role:client")],
        [InlineKeyboardButton("🛵 Я курьер", callback_data="role:courier")],
    ])


def kb_client_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Создать доставку", callback_data="client:new_order")],
        [InlineKeyboardButton("📦 Статус доставки", callback_data="client:status:open")],
        [InlineKeyboardButton("🔁 Сменить роль", callback_data="role:reset")],
    ])


def kb_client_price_choice() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📍 Дунпо ( {DEFAULT_PRICE_KRW} вон )", callback_data="client:price:local")],
        [InlineKeyboardButton("🌐 Другие районы (ввести цену)", callback_data="client:price:custom")],
    ])

def kb_client_price_recommend() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять рекомендованную цену", callback_data="client:price:accept_recommended")],
        [InlineKeyboardButton("✍️ Ввести цену вручную", callback_data="client:price:manual")],
    ])

def kb_courier_menu_not_applied() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Стать курьером", callback_data="courier:apply")],
        [InlineKeyboardButton("🔁 Сменить роль", callback_data="role:reset")],
    ])


def kb_courier_menu_pending() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Сменить роль", callback_data="role:reset")]])


def kb_courier_menu_approved(courier_id: int):
    active = get_active_order_for_courier(courier_id)

    if active:
        rows = [
            [InlineKeyboardButton("📦 Активный заказ", callback_data="courier:active_order")]
        ]
    else:
        rows = [
            [InlineKeyboardButton("📋 Текущие заявки", callback_data="courier:orders")],
            [InlineKeyboardButton("📊 Статистика", callback_data="courier:stats")]
        ]

    rows.append(
        [InlineKeyboardButton("🔁 Сменить роль", callback_data="role:reset")]
    )
    rows.append(
        [InlineKeyboardButton("🧹 Начать заново", callback_data="reset:hard")]
    )
    return InlineKeyboardMarkup(rows)

def kb_active_order():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Активный заказ", callback_data="courier:active_order")]
    ])

def kb_door_code() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Нет кода", callback_data="client:door_none")]])


def kb_delivery_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🍱 Еда", callback_data="client:type:food")],
        [InlineKeyboardButton("🛒 Покупки", callback_data="client:type:shopping")],
        [InlineKeyboardButton("📄 Документы", callback_data="client:type:docs")],
        [InlineKeyboardButton("📦 Другое", callback_data="client:type:other")],
    ])


def kb_delivery_time() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Сейчас", callback_data="client:time:now")],
        [InlineKeyboardButton("🕒 Сегодня", callback_data="client:time:today")],
        [InlineKeyboardButton("🗓 Указать время", callback_data="client:time:custom")],
    ])


def kb_confirm_order() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить заказ", callback_data="client:confirm:yes")],
        [InlineKeyboardButton("❌ Отменить", callback_data="client:confirm:no")],
    ])


def kb_order_offer(order: "Order") -> InlineKeyboardMarkup:
    # 3 кнопки, как договаривались:
    # - Naver поиск забора
    # - Naver поиск доставки
    # - адрес некорректен
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧭 Забор (Naver)", url=naver_map_search_url(order.pickup_address_ko))],
        [InlineKeyboardButton("🧭 Доставка (Naver)", url=naver_map_search_url(order.drop_address_ko))],
        [InlineKeyboardButton("⚠️ Адрес некорректен", callback_data=f"badaddr:{order.order_id}")],
        [InlineKeyboardButton("🤝 Взять заказ", callback_data=f"take:{order.order_id}")],
        [InlineKeyboardButton("⏭ Пропустить", callback_data=f"skip:{order.order_id}")],
    ])

def kb_order_en_route(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Заказ на руках", callback_data=f"picked:{order_id}")]
    ])

def kb_order_picked_up(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Заказ доставлен", callback_data=f"done:{order_id}")]
    ])


def kb_order_taken(order_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚗 Выезжаю", callback_data=f"progress:{order_id}")]
    ])


def kb_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 Новые заказы", callback_data="admin:new_orders")],
        [InlineKeyboardButton("🧍 Заявки курьеров", callback_data="admin:apps")],
        [InlineKeyboardButton("✅ Одобренные курьеры", callback_data="admin:approved")],
    ])


def kb_admin_app_decision(courier_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Одобрить", callback_data=f"admin:approve:{courier_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"admin:reject:{courier_id}"),
    ]])


def kb_client_problem_delete(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🗑 Удалить заказ #{order_id}", callback_data=f"client:delete:{order_id}")],
        [InlineKeyboardButton("🏠 Меню", callback_data="client:menu")],
    ])


def kb_client_status(order: "Order", can_cancel: bool) -> InlineKeyboardMarkup:
    rows = []
    if order.status == ORDER_PROBLEM:
        rows.append([InlineKeyboardButton(f"🗑 Удалить заказ #{order.order_id}", callback_data=f"client:delete:{order.order_id}")])
    elif can_cancel:
        rows.append([InlineKeyboardButton("🗑 Отозвать заказ", callback_data=f"client:cancel:{order.order_id}")])

    rows.append([InlineKeyboardButton("🔄 Обновить статус", callback_data="client:status:open")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="client:menu")])
    return InlineKeyboardMarkup(rows)


def kb_client_orders_filters() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 За сегодня", callback_data="client:orders:today")],
        [InlineKeyboardButton("📆 За неделю", callback_data="client:orders:week")],
        [InlineKeyboardButton("🗓 За месяц", callback_data="client:orders:month")],
        [InlineKeyboardButton("🏠 Меню", callback_data="client:menu")],
    ])


# =========================
# TEXT HELPERS
# =========================



def text_rules() -> str:
    return (
        "📜 Правила сервиса EasyGo\n\n"
        "⚠️ Перед началом всегда вводите /start\n\n"
        "EasyGo — платформа для связи клиентов и курьеров.\n"
        "Мы не принимаем оплату и не участвуем в расчетах.\n\n"
        "💰 Оплата\n"
        "Клиент платит курьеру напрямую.\n"
        "Цена фиксируется при создании заказа.\n\n"
        "🛵 Курьеры\n"
        "Курьер может только откликнуться на заказ.\n"
        "Связь с клиентом возможна ТОЛЬКО после принятия заказа.\n\n"
        "📍 Адреса\n"
        "Указываются на корейском языке.\n"
        "Перед принятием заказа курьер обязан проверить маршрут.\n\n"
        "📸 Подтверждение\n"
        "Заказ завершается только после отправки фото.\n\n"
        "🚫 Ответственность\n"
        "EasyGo не решает споры и не компенсирует убытки.\n"
        "Нарушения приводят к отключению доступа."
    )


def text_how_client() -> str:
    return (
        "🧾 Как сделать заказ\n\n"
        "1️⃣ Напишите /start\n"
        "2️⃣ Выберите роль «Я клиент»\n"
        "3️⃣ Нажмите «Создать доставку»\n"
        "4️⃣ Укажите адреса и контакт\n\n"
        "Если доставка вне Дунпо:\n"
        "— бот покажет рекомендованную цену\n"
        "— вы можете принять ее или ввести свою\n\n"
        "После подтверждения заказ становится доступен курьерам.\n"
        "Связь возможна ТОЛЬКО после принятия заказа курьером."
    )


def text_how_courier() -> str:
    return (
        "🛵 Как принять заказ\n\n"
        "1️⃣ Напишите /start\n"
        "2️⃣ Выберите роль «Я курьер»\n"
        "3️⃣ Нажмите «Текущие заявки»\n\n"
        "❗ До принятия заказа\n"
        "связь с клиентом запрещена\n\n"
        "4️⃣ Проверьте адреса через Naver\n"
        "5️⃣ Нажмите «Взять заказ»\n"
        "6️⃣ После доставки отправьте фото"
    )

def build_courier_stats_text(courier_id: int) -> str:
    now = datetime.now()

    def in_period(o: Order, days: int):
        dt = parse_ts(o.completed_at)
        if not dt:
            return False
        return dt >= now - timedelta(days=days)

    done = [
        o for o in ORDERS.values()
        if o.courier_tg_id == courier_id and o.status == ORDER_DONE
    ]

    today = sum(o.price_krw for o in done if in_period(o, 1))
    week = sum(o.price_krw for o in done if in_period(o, 7))
    month = sum(o.price_krw for o in done if in_period(o, 30))

    return (
        "📊 Мои заказы\n\n"
        f"📅 Сегодня: {today} вон\n"
        f"📆 Неделя: {week} вон\n"
        f"🗓 Месяц: {month} вон"
    )

def _dtype_line(dtype: str, other: str) -> str:
    if dtype == "food":
        return "Еда"
    if dtype == "shopping":
        return "Покупки"
    if dtype == "docs":
        return "Документы"
    if dtype == "other":
        return f"Другое ({other})" if other else "Другое"
    return dtype or ""


def _time_line(ttype: str, ttext: str) -> str:
    if ttype == "now":
        return "Сейчас"
    if ttype == "today":
        return "Сегодня"
    if ttype == "custom":
        return ttext or "уточняется"
    return ttext or "уточняется"


def order_status_ru(o: Order) -> str:
    if o.status == ORDER_NEW:
        return "Ищем курьера"
    if o.status == ORDER_TAKEN:
        return "Курьер назначен"
    if o.status == ORDER_EN_ROUTE:
        return "В пути"
    if o.status == ORDER_DONE_PENDING:
        return "Ожидается подтверждение"
    if o.status == ORDER_DONE:
        return "Доставлено"
    if o.status == ORDER_CANCELED:
        return "Отозвано"
    if o.status == ORDER_PROBLEM:
        return "Проблема с адресом"
    return o.status


def render_order_summary_for_confirm(d: Dict[str, Any]) -> str:
    door = d.get("door_code", "") or "нет"
    dtype = _dtype_line(d.get("delivery_type", ""), d.get("delivery_type_other_text", ""))
    tline = _time_line(d.get("delivery_time_type", ""), d.get("delivery_time_text", ""))
    price = int(d.get("price_krw") or 0)
    price_line = f"{price} вон" if price > 0 else "уточняется"

    return (
        "🧾 Проверьте заказ:\n\n"
        f"📍 Адрес забора:\n{d.get('pickup_address_ko', '')}\n\n"
        f"🏁 Адрес доставки:\n{d.get('drop_address_ko', '')}\n\n"
        f"🔒 Код подъезда:\n{door}\n\n"
        f"📦 Тип доставки:\n{dtype}\n\n"
        f"🕒 Время:\n{tline}\n\n"
        f"📞 Контакт:\n{d.get('recipient_contact_text', '')}\n\n"
        f"💰 Цена: {price_line}"
        
    )


def render_order_offer_text(order: Order) -> str:
    dtype = _dtype_line(order.delivery_type, order.delivery_type_other_text)
    tline = _time_line(order.delivery_time_type, order.delivery_time_text)
    return (
        f"🆕 Новый заказ #{order.order_id}\n\n"
        f"📦 Тип: {dtype}\n"
        f"🕒 Время: {tline}\n"
        f"💰 Цена: {order.price_krw} вон\n\n"
        f"📍 Адрес забора:\n`{order.pickup_address_ko}`\n\n"
        f"🏁 Адрес доставки:\n`{order.drop_address_ko}`"
    )


def render_order_taken_text(order: Order) -> str:
    door = order.door_code or "нет"
    return (
        "✅ Вы взяли заказ.\n\n"
        f"📦 Заказ #{order.order_id}\n"
        f"💰 Цена: {order.price_krw} вон\n\n"
        f"📍 Адрес забора:\n`{order.pickup_address_ko}`\n\n"
        f"🏁 Адрес доставки:\n`{order.drop_address_ko}`\n\n"
        f"🔒 Код подъезда:\n{door}\n\n"
        f"📞 Контакт:\n{order.recipient_contact_text}\n\n"
        "Свяжитесь с клиентом и уточните детали.\n"
        "Когда вы выехали, нажмите кнопку 'Выезжаю/в пути'."
    )


def render_client_status(o: Order) -> str:
    lines = []
    lines.append(f"📦 Статус заказа #{o.order_id}")
    lines.append("")
    lines.append(f"Статус: {order_status_ru(o)}")
    lines.append(f"Цена: {o.price_krw} вон")
    lines.append("")
    lines.append("📍 Адрес забора:")
    lines.append(o.pickup_address_ko)
    lines.append("")
    lines.append("🏁 Адрес доставки:")
    lines.append(o.drop_address_ko)
    lines.append("")

    if o.status in (ORDER_TAKEN, ORDER_EN_ROUTE, ORDER_DONE_PENDING, ORDER_DONE):
        if o.courier_name or o.courier_phone:
            lines.append(f"Курьер: {o.courier_name} {o.courier_phone}".strip())
        if o.taken_at:
            lines.append(f"Курьер назначен: {o.taken_at}")
    if o.status in (ORDER_EN_ROUTE, ORDER_DONE_PENDING, ORDER_DONE):
        if o.in_progress_at:
            lines.append(f"В пути с: {o.in_progress_at}")
    if o.status == ORDER_DONE:
        if o.completed_at:
            lines.append(f"Доставлено: {o.completed_at}")
    if o.status in (ORDER_CANCELED, ORDER_PROBLEM):
        if o.canceled_at:
            lines.append(f"Обновлено: {o.canceled_at}")

    return "\n".join(lines)


def render_admin_order_line(o: Order) -> str:
    extra = []
    if o.courier_tg_id:
        extra.append(f"Курьер: {o.courier_name}, {o.courier_phone}")
    if o.taken_at:
        extra.append(f"Назначен: {o.taken_at}")
    if o.in_progress_at:
        extra.append(f"В пути: {o.in_progress_at}")
    if o.completed_at:
        extra.append(f"Доставлено: {o.completed_at}")
    if o.canceled_at:
        extra.append(f"Обновлено: {o.canceled_at} ({o.canceled_by})")

    extra_text = ("\n" + "\n".join(extra)) if extra else ""
    return (
        f"Заказ #{o.order_id}\n"
        f"Статус: {o.status}{extra_text}\n"
        f"Цена: {o.price_krw} вон\n"
        f"Забор: {o.pickup_address_ko}\n"
        f"Доставка: {o.drop_address_ko}"
    )


# =========================
# HTTP SERVER (OPTIONAL)
# =========================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()


def run_http():
    port = int(os.environ.get("PORT", "8080"))
    httpd = HTTPServer(("0.0.0.0", port), Handler)
    log.info("HTTP server on port %s", port)
    httpd.serve_forever()

# =========================
# HOME ROOT (single entry point)
# =========================
HOME_TEXT = (
    "👋 Добро пожаловать в EasyGo\n\n"
    "Выберите действие:"
)

async def render_home_root(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    # сбрасываем FSM, но НЕ трогаем данные в Sheets и не ломаем логику
    init_user_defaults(context)
    context.user_data[USER_ROLE_KEY] = ROLE_UNKNOWN
    context.user_data[CLIENT_STATE_KEY] = C_NONE
    context.user_data[COURIER_STATE_KEY] = K_NONE
    context.user_data.pop("draft_order", None)
    context.user_data.pop("awaiting_proof_order_id", None)

    await ui_render(
        context,
        chat_id,
        HOME_TEXT,
        reply_markup=kb_home_root()
    )

# =========================
# START FLOW
# =========================
def init_user_defaults(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault(USER_ROLE_KEY, ROLE_UNKNOWN)
    context.user_data.setdefault(USER_LOCATION_KEY, "")
    context.user_data.setdefault(CLIENT_STATE_KEY, C_NONE)
    context.user_data.setdefault(COURIER_STATE_KEY, K_NONE)
    context.user_data.setdefault("warned_naver_check", False)  # предупреждение курьеру, один раз


# =========================
# COMMANDS
# =========================
import asyncio

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user

    # 🔒 блокируем остальные апдейты
    context.user_data[UI_RESET_KEY] = True

    # 💣 ручной сброс
    context.user_data.pop(UI_MSG_ID_KEY, None)
    context.user_data.pop("draft_order", None)
    context.user_data.pop("awaiting_proof_order_id", None)

    context.user_data[USER_ROLE_KEY] = ROLE_UNKNOWN
    context.user_data[CLIENT_STATE_KEY] = C_NONE
    context.user_data[COURIER_STATE_KEY] = K_NONE
    context.user_data[USER_LOCATION_KEY] = ""
    init_user_defaults(context)

    # 🔓 РАЗРЕШАЕМ UI ТОЛЬКО ДЛЯ /start
    context.user_data.pop(UI_RESET_KEY, None)

    # ✅ гарантированный рендер
    await render_home_root(context, chat.id)

    # 📝 лог
    if SHEETS and user:
        asyncio.create_task(log_start(user))

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_admin(update.effective_user.id):
        return

    if SHEETS:
        SHEETS.log_event(update.effective_user.id, role_for_log(context), "ADMIN_OPEN")

    await ui_render(
        context,
        update.effective_chat.id,
        "🛠 Панель администратора",
        reply_markup=kb_admin_menu()
    )


# =========================
# NOTIFICATIONS
# =========================

async def handle_courier_orders(query, context: ContextTypes.DEFAULT_TYPE):
    uid = query.from_user.id



    if not courier_is_approved(uid):
        await ui_render(context, uid, "Нет доступа.")
        return

    # 🔑 если есть активный заказ — ТОЛЬКО ОН
    active = get_active_order_for_courier(uid)
    if active:
        context.user_data.pop(UI_MSG_ID_KEY, None)
        await render_active_order_screen(query, context, active)
        return

    # 🔑 берем ОДИН следующий заказ
    orders = [o for o in ORDERS.values() if o.status == ORDER_NEW]

    if not orders:
        await ui_render(
            context,
            uid,
            "📭 Сейчас нет доступных заказов.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="courier_refresh")],
                [InlineKeyboardButton("🏠 Выйти", callback_data="go_start")]
            ])
        )
        return

    orders.sort(key=lambda o: int(o.order_id), reverse=True)
    order = orders[0]

    await ui_render(
        context,
        uid,
        render_order_offer_text(order),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🤝 Взять заказ", callback_data=f"take:{order.order_id}")],
            [InlineKeyboardButton("🔄 Обновить", callback_data="courier_refresh")],
            [InlineKeyboardButton("🏠 Выйти", callback_data="go_start")]
        ]),
        parse_mode="Markdown",
    )

async def _send_courier_naver_warning_once(context: ContextTypes.DEFAULT_TYPE, courier_id: int):
    # минимальный текст, один раз
    # хранится в user_data конкретного чата, но в send_message без update нет context.user_data.
    # поэтому делаем через bot_data пер-курьер.
    key = f"warned_naver_check:{courier_id}"
    if context.bot_data.get(key):
        return
    context.bot_data[key] = True
    try:
        await tg_retry(lambda: context.bot.send_message(
            chat_id=courier_id,
            text="Перед принятием проверьте адреса в Naver."
        ))
    except Exception as e:
        log.warning("Courier warning send failed: %s", e)


async def notify_new_order(context: ContextTypes.DEFAULT_TYPE, order: Order):
    text = render_order_offer_text(order)

    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_message(
                chat_id=aid,
                text=f"🆕 Новый заказ\n\n{text}"
            ))
        except Exception as e:
            log.warning("Admin notify failed: %s", e)

    for cid, prof in COURIERS.items():
        if prof.status != COURIER_APPROVED:
            continue
        await _send_courier_naver_warning_once(context, cid)
        try:
            await tg_retry(lambda ccid=cid: context.bot.send_message(
                chat_id=ccid,
                text=text,
                reply_markup=kb_order_offer(order),
                parse_mode="Markdown",
            ))
        except Exception as e:
            log.warning("Courier notify failed: %s", e)


async def notify_order_canceled(context: ContextTypes.DEFAULT_TYPE, order: Order):
    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_message(
                chat_id=aid,
                text=f"🗑 Заказ #{order.order_id} отозван клиентом."
            ))
        except Exception as e:
            log.warning("Admin cancel notify failed: %s", e)

    for cid, prof in COURIERS.items():
        if prof.status != COURIER_APPROVED:
            continue
        try:
            await tg_retry(lambda ccid=cid: context.bot.send_message(
                chat_id=ccid,
                text=f"🗑 Заказ #{order.order_id} отозван и больше недоступен."
            ))
        except Exception as e:
            log.warning("Courier cancel notify failed: %s", e)


async def notify_order_bad_address(context: ContextTypes.DEFAULT_TYPE, order: Order):
    # клиенту - именно по этому заказу + кнопка удаления
    try:
        await tg_retry(lambda: context.bot.send_message(
            chat_id=order.client_tg_id,
            text=(
                f"⚠️ По заказу #{order.order_id} курьер сообщил, что адрес некорректен.\n"
                "Пожалуйста, удалите заказ и создайте новый с корректным адресом."
            ),
            reply_markup=kb_client_problem_delete(order.order_id)
        ))
    except Exception as e:
        log.warning("Client bad-address notify failed: %s", e)

    # админам
    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_message(
                chat_id=aid,
                text=f"⚠️ Заказ #{order.order_id}: курьер отметил адрес некорректным. Заказ скрыт из доступных."
            ))
        except Exception as e:
            log.warning("Admin bad-address notify failed: %s", e)


# =========================
# ADMIN CALLBACKS
# =========================
async def handle_admin_callbacks(query, context: ContextTypes.DEFAULT_TYPE, data: str):
    uid = query.from_user.id

    if data == "admin:new_orders":
        items = list(ORDERS.values())
        if not items:
            ui_render(context, uid, "Пока нет заказов.")
            return

        items.sort(key=lambda o: int(o.order_id), reverse=True)
        for o in items[:10]:
            await ui_render(
                context,
                uid,
                render_admin_order_line(o),
                reply_markup=kb_admin_menu()
            )
        return

    if data == "admin:apps":
        pending = [c for c in COURIERS.values() if c.status == COURIER_PENDING]
        if not pending:
            await ui_render(context, uid, "Нет заявок.")
            return
        for c in pending:
            text = (
                "🧍 Заявка курьера\n\n"
                f"Имя: {c.name}\n"
                f"Телефон: {c.phone}\n"
                f"Транспорт: {c.transport}\n"
                f"ID: {c.courier_tg_id}"
            )
            await ui_render(
                context,
                uid,
                text,
                reply_markup=kb_admin_app_decision(c.courier_tg_id)
            )
        return

    if data == "admin:approved":
        approved = [c for c in COURIERS.values() if c.status == COURIER_APPROVED]
        if not approved:
            await ui_render(context, uid, "Нет одобренных курьеров.")
            return
        lines = [f"{c.name} - {c.phone} - {c.transport} (ID {c.courier_tg_id})" for c in approved]
        await ui_render(context, uid, "\n".join(lines))
        return

    if data.startswith("admin:approve:"):
        cid = int(data.split(":")[-1])
        c = COURIERS.get(cid)
        if not c:
            await ui_render(context, uid, "Курьер не найден.")
            return

        c.status = COURIER_APPROVED
        c.approved_at = now_ts()
        c.rejected_at = ""
        COURIERS[cid] = c

        if SHEETS:
            SHEETS.upsert_courier(asdict(c))
            SHEETS.log_event(uid, ROLE_COURIER, "COURIER_APPROVED", meta=str(cid))

        await ui_render(context, uid, "✅ Курьер одобрен.")
        await tg_retry(lambda: context.bot.send_message(
            chat_id=cid,
            text="✅ Вы одобрены как курьер. Новые заказы будут приходить автоматически.",
            reply_markup=kb_courier_menu_approved(cid)
        ))
        return

    if data.startswith("admin:reject:"):
        cid = int(data.split(":")[-1])
        c = COURIERS.get(cid)
        if not c:
            await ui_render(context, uid, "Курьер не найден.")
            return

        c.status = COURIER_REJECTED
        c.rejected_at = now_ts()
        c.approved_at = ""
        COURIERS[cid] = c

        if SHEETS:
            SHEETS.upsert_courier(asdict(c))
            SHEETS.log_event(uid, ROLE_COURIER, "COURIER_REJECTED", meta=str(cid))

        await ui_render(context, uid, "❌ Заявка отклонена.")
        await tg_retry(lambda: context.bot.send_message(
            chat_id=cid,
            text="К сожалению, ваша заявка отклонена."
        ))
        return


# =========================
# COURIER: CURRENT ORDERS
# =========================
async def show_current_orders_for_courier(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    if not courier_is_approved(chat_id):
        await tg_retry(lambda: context.bot.send_message(chat_id=chat_id, text="Нет доступа."))
        return

    items = [o for o in ORDERS.values() if o.status == ORDER_NEW]
    if not items:
        await tg_retry(lambda: context.bot.send_message(chat_id=chat_id, text="Сейчас нет доступных заявок."))
        return

    items.sort(key=lambda o: int(o.order_id), reverse=True)

    await _send_courier_naver_warning_once(context, chat_id)
    await tg_retry(lambda: context.bot.send_message(chat_id=chat_id, text="📋 Текущие заявки:"))

    for o in items[:20]:
        try:
            await tg_retry(lambda order=o: context.bot.send_message(
                chat_id=chat_id,
                text=render_order_offer_text(order),
                reply_markup=kb_order_offer(order),
                parse_mode="Markdown",
            ))
        except BadRequest as e:
            # чтобы не "висло" на одной битой отправке
            log.warning("BadRequest sending current order %s to %s: %s", o.order_id, chat_id, e)
        except Exception as e:
            log.warning("Failed sending current order %s to %s: %s", o.order_id, chat_id, e)

async def handle_picked_up(query, context, courier_id: int, order_id: str):
    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, courier_id, "Заказ не найден.")
            return
        if order.courier_tg_id != courier_id:
            await ui_render(context, courier_id, "Это не ваш заказ.")
            return
        if order.status != ORDER_EN_ROUTE:
            await ui_render(context, courier_id, "Сейчас нельзя отметить заказ на руках.")
            return

        order.status = ORDER_PICKED_UP
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(courier_id, ROLE_COURIER, "ORDER_PICKED_UP", order_id=order_id)

    await ui_render(
        context,
        courier_id,
        "📦 Заказ у вас на руках.\nКогда доставите — нажмите кнопку ниже.",
        reply_markup=kb_order_picked_up(order.order_id)
    )

# =========================
# CLIENT: STATUS + ORDERS LIST
# =========================
def get_client_orders(uid: int) -> List[Order]:
    items = [o for o in ORDERS.values() if o.client_tg_id == uid]
    items.sort(key=lambda x: int(x.order_id), reverse=True)
    return items


def pick_active_order(uid: int) -> Optional[Order]:
    items = get_client_orders(uid)
    for o in items:
        if o.status not in (ORDER_DONE, ORDER_CANCELED, ORDER_PROBLEM):
            return o
    return items[0] if items else None


def filter_orders_by_period(items: List[Order], period: str) -> List[Order]:
    now = datetime.now()
    if period == "today":
        start = datetime(now.year, now.month, now.day)
    elif period == "week":
        start = now - timedelta(days=7)
    else:
        start = now - timedelta(days=30)

    out = []
    for o in items:
        dt = parse_ts(o.created_at)
        if not dt:
            out.append(o)
            continue
        if dt >= start:
            out.append(o)
    return out


def render_orders_list(items: List[Order], limit: int = 20) -> str:
    if not items:
        return "Нет заказов за выбранный период."
    lines = ["🧾 Ваши заказы:"]
    for o in items[:limit]:
        st = order_status_ru(o)
        lines.append(f"#{o.order_id} | {st} | {o.created_at} | {o.price_krw} вон")
    return "\n".join(lines)


# =========================
# TAKE ORDER + IN PROGRESS + COMPLETE + CANCEL + PROBLEM
# =========================
async def handle_take_order(query, context: ContextTypes.DEFAULT_TYPE, courier_id: int, order_id: str):
    # курьер должен быть одобрен
    if not courier_is_approved(courier_id):
        await ui_render(
            context,
            courier_id,
            "Чтобы брать заказы, нужно одобрение администратора."
        )
        return

    # ❗ жесткое правило: 1 активный заказ
    active = get_active_order_for_courier(courier_id)
    if active:
        await ui_render(
            context,
            courier_id,
            (
                f"⚠️ У вас уже есть активный заказ #{active.order_id}.\n"
                "Сначала завершите его или откройте через меню."
            ),
            reply_markup=kb_active_order()
        )
        return

    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, courier_id, "Заказ не найден.")
            return

        if order.status != ORDER_NEW:
            await ui_render(context, courier_id, "Этот заказ уже недоступен.")
            if SHEETS:
                SHEETS.log_event(
                    courier_id,
                    ROLE_COURIER,
                    "TAKE_FAIL_NOT_NEW",
                    order_id=order_id,
                    meta=order.status
                )
            return

        prof = COURIERS.get(courier_id)

        # назначаем заказ курьеру
        order.status = ORDER_TAKEN
        order.taken_at = now_ts()
        order.courier_tg_id = courier_id
        order.courier_name = prof.name if prof else ""
        order.courier_phone = prof.phone if prof else ""
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(
                courier_id,
                ROLE_COURIER,
                "ORDER_TAKEN",
                order_id=order_id
            )

    # 🔑 ВОТ ЭТА СТРОКА — КРИТИЧЕСКАЯ
    context.user_data.pop(UI_MSG_ID_KEY, None)

    # ✅ один-единственный UI render
    await ui_render(
        context,
        courier_id,
        render_order_taken_text(order),
        reply_markup=kb_order_taken(order.order_id)
    )

    # уведомления админам (вне UI)
    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_message(
                chat_id=aid,
                text=f"✅ Заказ #{order.order_id} взят курьером {order.courier_name} {order.courier_phone}".strip()
            ))
        except Exception as e:
            log.warning("Admin taken notify failed: %s", e)


async def handle_bad_address(query, context: ContextTypes.DEFAULT_TYPE, courier_id: int, order_id: str):
    if not courier_is_approved(courier_id):
        await ui_render(context, courier_id, "Нет доступа.")
        return

    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, courier_id, "Заказ не найден.")
            return
        if order.status != ORDER_NEW:
            await ui_render(context, courier_id, "Этот заказ уже недоступен.")
            if SHEETS:
                SHEETS.log_event(courier_id, ROLE_COURIER, "BADADDR_FAIL_NOT_NEW", order_id=order_id, meta=order.status)
            return

        order.status = ORDER_PROBLEM
        order.canceled_at = ""
        order.canceled_by = ""
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(courier_id, ROLE_COURIER, "ORDER_BAD_ADDRESS", order_id=order_id)

    # Сообщение курьеру + убираем "offer" клаву у сообщения (чтобы не пытались взять)
    try:
        await tg_retry(lambda: query.edit_message_reply_markup(reply_markup=None))
    except Exception:
        pass

    await ui_render(context, courier_id,
        f"⚠️ Ок, заказ #{order.order_id} помечен как проблемный..."
    )

    await notify_order_bad_address(context, order)


async def handle_in_progress_clicked(query, context: ContextTypes.DEFAULT_TYPE, courier_id: int, order_id: str):
    if not courier_is_approved(courier_id):
        await ui_render(context, courier_id, "Нет доступа.")
        return
    
    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, courier_id, "Заказ не найден.")
            return

        if order.courier_tg_id != courier_id:
            await ui_render(context, courier_id, "Этот заказ закреплен за другим курьером.")
            return

        if order.status != ORDER_TAKEN:
            await ui_render(context, courier_id, "Нельзя выехать сейчас.")
            return
        
        order.in_progress_at = now_ts()
        order.status = ORDER_EN_ROUTE
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(courier_id, ROLE_COURIER, "ORDER_EN_ROUTE", order_id=order_id)

    await ui_render(
        context,
        courier_id,
        render_order_taken_text(order),
        reply_markup=kb_order_en_route(order.order_id)
    )

    try:
        await tg_retry(lambda: context.bot.send_message(
            chat_id=order.client_tg_id,
            text=(
                "🚗 Курьер выехал к вам.\n"
                f"В пути с: {order.in_progress_at}\n"
                f"Курьер: {order.courier_name} {order.courier_phone}"
            ).strip()
        ))
    except Exception as e:
        log.warning("Client in-progress notify failed: %s", e)

    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_message(
                chat_id=aid,
                text=f"🚗 Заказ #{order.order_id} - курьер в пути (с {order.in_progress_at})."
            ))
        except Exception as e:
            log.warning("Admin in-progress notify failed: %s", e)


async def handle_done_clicked(query, context: ContextTypes.DEFAULT_TYPE, courier_id: int, order_id: str):
    if not courier_is_approved(courier_id):
        await ui_render(context, courier_id, "Нет доступа.")
        return

    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, courier_id, "Заказ не найден.")
            return
        if order.courier_tg_id != courier_id:
            await ui_render(context, courier_id, "Этот заказ закреплен за другим курьером.")
            return
        if order.status != ORDER_PICKED_UP:
            await ui_render(context, courier_id, "Сначала возьмите заказ на руки.")
            return

        order.status = ORDER_DONE_PENDING
        order.done_requested_at = now_ts()
        ORDERS[order_id] = order
        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(courier_id, ROLE_COURIER, "DONE_CLICKED", order_id=order_id)

    context.user_data[COURIER_STATE_KEY] = K_AWAITING_PROOF
    context.user_data["awaiting_proof_order_id"] = order_id

    await ui_render(
        context,
        courier_id,
        "📸 Пожалуйста, отправьте скриншот подтверждения доставки.\nЭто обязательно."
    )


async def handle_proof_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    order_id = context.user_data.get("awaiting_proof_order_id", "")

    if not order_id:
        context.user_data[COURIER_STATE_KEY] = K_NONE
        await ui_render(
            context,
            update.effective_chat.id,
            "Не понимаю, к какому заказу это относится."
        )
        return

    order = ORDERS.get(order_id)
    if not order:
        context.user_data[COURIER_STATE_KEY] = K_NONE
        context.user_data.pop("awaiting_proof_order_id", None)
        await ui_render(
            context,
            update.effective_chat.id,
            "Заказ не найден."
        )
        return

    if order.courier_tg_id != uid:
        context.user_data[COURIER_STATE_KEY] = K_NONE
        context.user_data.pop("awaiting_proof_order_id", None)
        await ui_render(
            context,
            update.effective_chat.id,
            "Этот заказ закреплен за другим курьером."
        )
        return

    if order.status != ORDER_DONE_PENDING:
        context.user_data[COURIER_STATE_KEY] = K_NONE
        context.user_data.pop("awaiting_proof_order_id", None)
        await ui_render(
            context,
            update.effective_chat.id,
            "Этот заказ сейчас не ожидает скриншот."
        )
        return

    photo = update.message.photo[-1]
    file_id = photo.file_id
    msg_id = str(update.message.message_id)

    async with ORDER_LOCK:
        order.proof_image_file_id = file_id
        order.proof_image_message_id = msg_id
        order.completed_at = now_ts()
        order.status = ORDER_DONE
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(uid, ROLE_COURIER, "PROOF_RECEIVED", order_id=order_id)

    # 🔴 ЖЕСТКО разрываем старый UI
    context.user_data.pop(UI_MSG_ID_KEY, None)

    # ✅ Новый экран курьера без активного заказа
    await ui_render(
        context,
        update.effective_chat.id,
        "✅ Заказ завершен.\n\n🛵 Меню курьера:",
        reply_markup=kb_courier_menu_approved(uid)
    )

    # уведомляем клиента
    try:
        await tg_retry(lambda: context.bot.send_photo(
            chat_id=order.client_tg_id,
            photo=file_id,
            caption="✅ Ваш заказ выполнен."
        ))
    except Exception as e:
        log.warning("Client proof send failed: %s", e)

    # уведомляем админов
    for admin_id in ADMIN_IDS:
        try:
            await tg_retry(lambda aid=admin_id: context.bot.send_photo(
                chat_id=aid,
                photo=file_id,
                caption=(
                    f"✅ Заказ #{order.order_id} завершен.\n"
                    f"Курьер: {order.courier_name}, {order.courier_phone}"
                )
            ))
        except Exception as e:
            log.warning("Admin proof send failed: %s", e)

    context.user_data[COURIER_STATE_KEY] = K_NONE
    context.user_data.pop("awaiting_proof_order_id", None)


async def handle_client_cancel(query, context: ContextTypes.DEFAULT_TYPE, uid: int, order_id: str):
    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, uid, "Заказ не найден.")
            return
        if order.client_tg_id != uid:
            await ui_render(context, uid, "Нет доступа.")
            return
        if order.status != ORDER_NEW:
            await ui_render(context, uid, "Нельзя отозвать заказ на этой стадии.")
            return

        order.status = ORDER_CANCELED
        order.canceled_at = now_ts()
        order.canceled_by = "client"
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_CANCELED_BY_CLIENT", order_id=order_id)

    await ui_render(context, uid, "🗑 Заказ отозван.", reply_markup=kb_client_menu())
    await notify_order_canceled(context, order)


async def handle_client_delete_problem(query, context: ContextTypes.DEFAULT_TYPE, uid: int, order_id: str):
    async with ORDER_LOCK:
        order = ORDERS.get(order_id)
        if not order:
            await ui_render(context, uid, "Заказ не найден.")
            return
        if order.client_tg_id != uid:
            await ui_render(context, uid, "Нет доступа.")
            return
        if order.status == ORDER_DONE:
            await ui_render(context, uid, "Этот заказ уже выполнен и не может быть удален.")
            return

        order.status = ORDER_CANCELED
        order.canceled_at = now_ts()
        order.canceled_by = "client_delete_problem"
        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.update_order(asdict(order))
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_DELETED_AFTER_BADADDR", order_id=order_id)

    await ui_render(context, uid, "🗑 Заказ удален.", reply_markup=kb_client_menu())

# =========================
# GOOGLE GEOCODE & Distance Matrix
# =========================

import math

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def google_geocode(address: str) -> Optional[tuple[float, float]]:
    if not GOOGLE_MAPS_API_KEY:
        log.warning("GOOGLE GEOCODE SKIP: API KEY MISSING")
        return None

    log.info("GOOGLE GEOCODE REQUEST | addr=%r", address)

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": GOOGLE_MAPS_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=5)
        log.info("GOOGLE GEOCODE HTTP %s | %s", r.status_code, r.url)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.exception("GOOGLE GEOCODE ERROR")
        return None

    if data.get("status") != "OK":
        return None

    loc = data["results"][0]["geometry"]["location"]
    return loc["lat"], loc["lng"]

def google_distance_km(
    lat1: float,
    lng1: float,
    lat2: float,
    lng2: float,
) -> Optional[float]:

    if not GOOGLE_MAPS_API_KEY:
        log.warning("GOOGLE DISTANCE SKIP: API KEY MISSING")
        return None

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": f"{lat1},{lng1}",
        "destinations": f"{lat2},{lng2}",
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    log.info(
        "GOOGLE DISTANCE REQUEST | %s,%s -> %s,%s",
        lat1, lng1, lat2, lng2
    )

    try:
        r = requests.get(url, params=params, timeout=5)
        log.info(
            "GOOGLE DISTANCE HTTP %s | %s",
            r.status_code,
            r.url
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        log.exception("GOOGLE DISTANCE ERROR")
        return None

    if data.get("status") != "OK":
        log.warning(
            "GOOGLE DISTANCE FAIL | status=%s | body=%s",
            data.get("status"),
            data
        )
        return None

    try:
        el = data["rows"][0]["elements"][0]
    except Exception:
        log.warning("GOOGLE DISTANCE BAD STRUCTURE | body=%s", data)
        return None

    if el.get("status") != "OK":
        log.warning(
            "GOOGLE DISTANCE ELEMENT FAIL | status=%s | body=%s",
            el.get("status"),
            el
        )
        return None

    meters = el.get("distance", {}).get("value")
    if meters is None:
        log.warning("GOOGLE DISTANCE NO DISTANCE FIELD | body=%s", el)
        return None

    km = meters / 1000.0
    log.info("GOOGLE DISTANCE OK | km=%.2f", km)
    return km



# =========================
# NAVER
# =========================

def naver_geocode(address: str):
    url = "https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode"
    headers = {
        "X-NCP-APIGW-API-KEY-ID": os.getenv("NAVER_CLIENT_ID"),
        "X-NCP-APIGW-API-KEY": os.getenv("NAVER_CLIENT_SECRET"),
    }
    params = {"query": address}

    log.info(
        "NAVER GEOCODE REQUEST | addr='%s' | id_set=%s | secret_set=%s",
        address,
        bool(headers.get("X-NCP-APIGW-API-KEY-ID")),
        bool(headers.get("X-NCP-APIGW-API-KEY")),
    )

    r = requests.get(url, headers=headers, params=params, timeout=5)

    log.info(
        "NAVER GEOCODE RESPONSE | status=%s | body=%s",
        r.status_code,
        r.text[:300],  # не больше, чтобы не заспамить
    )

    r.raise_for_status()
    data = r.json()

    if not data.get("addresses"):
        return None

    a = data["addresses"][0]
    return float(a["y"]), float(a["x"])  # lat, lon

def naver_route_distance_km(
    start_lat: float,
    start_lon: float,
    goal_lat: float,
    goal_lon: float,
) -> Optional[float]:
    """
    Directions 5 API: distance meters -> km
    route.traoptimal[0].summary.distance
    """
    url = "https://naveropenapi.apigw.ntruss.com/map-direction/v1/driving"

    headers = {
        "X-NCP-APIGW-API-KEY-ID": os.getenv("NAVER_CLIENT_ID"),
        "X-NCP-APIGW-API-KEY": os.getenv("NAVER_CLIENT_SECRET"),
    }

    log.info(
        "NAVER ROUTE REQUEST | start=%s,%s | goal=%s,%s | id_set=%s | secret_set=%s",
        start_lat,
        start_lon,
        goal_lat,
        goal_lon,
        bool(headers.get("X-NCP-APIGW-API-KEY-ID")),
        bool(headers.get("X-NCP-APIGW-API-KEY")),
    )

    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        log.warning("NAVER ROUTE SKIP: missing API keys")
        return None

    params = {
        "start": f"{start_lon},{start_lat}",
        "goal": f"{goal_lon},{goal_lat}",
        "option": "traoptimal",
    }

    r = requests.get(url, headers=headers, params=params, timeout=6)

    log.info(
        "NAVER ROUTE RESPONSE | status=%s | body=%s",
        r.status_code,
        r.text[:300],
    )

    r.raise_for_status()
    data = r.json()

    route = data.get("route") or {}
    arr = route.get("traoptimal") or []
    if not arr:
        log.warning("NAVER ROUTE EMPTY")
        return None

    summary = (arr[0] or {}).get("summary") or {}
    dist_m = summary.get("distance")
    if dist_m is None:
        log.warning("NAVER ROUTE NO DISTANCE FIELD")
        return None

    try:
        return float(dist_m) / 1000.0
    except Exception as e:
        log.warning("NAVER ROUTE DIST PARSE ERROR: %s", e)
        return None

def calc_recommended_price_krw(pickup_addr: str, drop_addr: str) -> Optional[int]:
    log.info("PRICE CALC START | pickup=%r | drop=%r", pickup_addr, drop_addr)

    a = google_geocode(pickup_addr)
    b = google_geocode(drop_addr)
    if not a or not b:
        log.warning("PRICE CALC FAIL | geocode failed | a=%s b=%s", a, b)
        return None

    lat1, lng1 = a
    lat2, lng2 = b

    km = google_distance_km(lat1, lng1, lat2, lng2)
    source = "google"

    if km is None:
        base_km = haversine_km(lat1, lng1, lat2, lng2)
        km = base_km * 1.5
        source = "haversine_adjusted"

    log.info(
        "DISTANCE RESULT | km=%.2f | source=%s",
        km,
        source
    )
    def round_krw_1000(value: int) -> int:
        return int(math.ceil(value / 1000.0) * 1000)

    raw_price = int(round(km * PRICE_PER_KM_KRW))
    price = round_krw_1000(raw_price)
    log.info("PRICE FINAL | raw=%s | rounded=%s", raw_price, price)
    return price

    
# =========================
# MAIN CALLBACK HANDLER
# =========================

async def handle_hard_reset(query, context: ContextTypes.DEFAULT_TYPE):
    uid = query.from_user.id

    context.user_data.clear()
    context.user_data[UI_MSG_ID_KEY] = None
    context.user_data[CLIENT_STATE_KEY] = C_NONE
    context.user_data[COURIER_STATE_KEY] = K_NONE

    prof = COURIERS.get(uid)

    # если курьер одобрен — возвращаем меню курьера
    if prof and prof.status == COURIER_APPROVED:
        await ui_render(
            context,
            uid,
            "🛵 Меню курьера:",
            reply_markup=kb_courier_menu_approved(uid)
        )
        return

    # иначе — обычный старт
    await render_home_root(context, uid)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    if context.user_data.get(UI_RESET_KEY):
        await query.answer("Обновление…")
        return

    query = update.callback_query
    if not query:
        return

    await tg_retry(lambda: query.answer())

    uid = query.from_user.id
    uname = query.from_user.username or ""
    current_role = context.user_data.get(USER_ROLE_KEY, ROLE_UNKNOWN)
    data = query.data or ""

    if CLIENT_STATE_KEY not in context.user_data:
        await query.answer("Сессия обновлена. Нажмите /start", show_alert=False)
        return

    # ===== HOME SCREENS =====

    if data == "home:start":
        await ui_render(
            context,
            uid,
            "📍 Где вы находитесь?",
            reply_markup=kb_location()
        )
        return

    if data == "home:rules":
        await ui_render(
            context,
            uid,
            text_rules(),
            reply_markup=kb_back_home()
        )
        return

    if data == "home:client":
        await ui_render(
            context,
            uid,
            text_how_client(),
            reply_markup=kb_back_home()
        )
        return

    if data == "home:courier":
        await ui_render(
            context,
            uid,
            text_how_courier(),
            reply_markup=kb_back_home()
        )
        return

    if data == "home:back":
        await render_home_root(context, uid)
        return
    
   
    if data == "info:rules":
        await ui_render(context, uid, text_rules(), reply_markup=kb_back_to_start())
        return

    if data == "info:client":
        await ui_render(context, uid, text_how_client(), reply_markup=kb_back_to_start())
        return

    if data == "info:courier":
        await ui_render(context, uid, text_how_courier(), reply_markup=kb_back_to_start())
        return

    if data == "info:back":
        await render_home_root(context, uid)
        return


    if data == "courier:orders":
        await handle_courier_orders(query, context)
        return

    if data == "start:go":
        await ui_render(
            context,
            uid,
            "📍 Где вы находитесь?",
            reply_markup=kb_location()
        )
        return
    
    

    if data.startswith("loc:"):
        loc = data.split(":", 1)[1]
        context.user_data[USER_LOCATION_KEY] = loc
        if SHEETS:
            SHEETS.log_event(uid, current_role, "LOCATION_PICKED", meta=loc)

        if loc != LOC_DUNPO:
            await ui_render(
                context,
                uid,
                "Пока доставка работает только в Дунпо.\n\nВыберите 'Дунпо', чтобы продолжить.",
                reply_markup=kb_location()
            )
            return

        await ui_render(context, uid, "👤 Кто вы?", reply_markup=kb_role())
        return

    if data == "role:reset":
        context.user_data.pop(UI_MSG_ID_KEY, None)  # ⬅️ разрыв UI-сессии

        context.user_data[USER_ROLE_KEY] = ROLE_UNKNOWN
        context.user_data[CLIENT_STATE_KEY] = C_NONE
        context.user_data[COURIER_STATE_KEY] = K_NONE
        context.user_data.pop("draft_order", None)
        context.user_data.pop("awaiting_proof_order_id", None)

        if SHEETS:
            SHEETS.log_event(uid, ROLE_UNKNOWN, "ROLE_RESET")

        await render_home_root(context, uid)
        return

    if data == "reset:hard":
        await handle_hard_reset(query, context)
        return

    if data == "client:menu":
        await ui_render(
            context,
            uid,
            "🏠 Меню клиента:",
            reply_markup=kb_client_menu()
        )
        return

    if data == "role:client":
        context.user_data[USER_ROLE_KEY] = ROLE_CLIENT
        context.user_data[CLIENT_STATE_KEY] = C_NONE
        context.user_data.pop("draft_order", None)
        if SHEETS:
            SHEETS.log_event(uid, ROLE_CLIENT, "ROLE_PICKED")
        await ui_render(
            context,
            uid,
            "Что вы хотите сделать?",
            reply_markup=kb_client_menu()
        )
        return

    if data == "role:courier":
        context.user_data[USER_ROLE_KEY] = ROLE_COURIER
        context.user_data[COURIER_STATE_KEY] = K_NONE
        if SHEETS:
            SHEETS.log_event(uid, ROLE_COURIER, "ROLE_PICKED")

        prof = COURIERS.get(uid)
        if not prof:
            await ui_render(
                context,
                uid,
                "Чтобы получать заказы, нужно стать курьером.",
                reply_markup=kb_courier_menu_not_applied()
            )
            return

        if prof.status == COURIER_PENDING:
            await ui_render(
                context,
                uid,
                "Заявка отправлена.\nОжидайте одобрения администратора.",
                reply_markup=kb_courier_menu_pending()
            )
            return

        if prof.status == COURIER_APPROVED:
            active = get_active_order_for_courier(uid)
            active_line = f"\nАктивный заказ: #{active.order_id}" if active else ""
            await ui_render(
                context,
                uid,
                f"✅ Вы одобрены как курьер.{active_line}\nНовые заказы будут приходить автоматически.",
                reply_markup=kb_courier_menu_approved(uid)
            )
            return

        await ui_render(
            context,
            uid,
            "Ваша заявка отклонена.",
            reply_markup=kb_courier_menu_not_applied()
        )
        return

    if data.startswith("picked:"):
        order_id = data.split(":", 1)[1]
        await handle_picked_up(query, context, uid, order_id)
        return

    # 🔄 ОБНОВЛЕНИЕ ЭКРАНА КУРЬЕРА
    if data == "courier_refresh":
        await handle_courier_orders(query, context)
        return

    if data == "courier:stats":
        text = build_courier_stats_text(uid)
        await ui_render(
            context,
            uid,
            text,
            reply_markup=kb_courier_menu_approved(uid)
        )
        return

    if data == "courier:active_order":
        active = get_active_order_for_courier(uid)
        if not active:
            await ui_render(
                context,
                uid,
                "Сейчас у вас нет активного заказа.",
                reply_markup=kb_courier_menu_approved(uid)
            )
            return

        if active.status == ORDER_TAKEN:
            kb = kb_order_taken(active.order_id)
        elif active.status == ORDER_EN_ROUTE:
            kb = kb_order_en_route(active.order_id)
        elif active.status == ORDER_PICKED_UP:
            kb = kb_order_picked_up(active.order_id)
        else:
            kb = None

        await ui_render(
            context,
            uid,
            render_order_taken_text(active),
            reply_markup=kb
        )
        return

    if data == "client:status:open":
        o = pick_active_order(uid)
        if not o:
            await ui_render(
                context,
                uid,
                "У вас пока нет заказов.",
                reply_markup=kb_client_menu()
            )
            return

        can_cancel = (o.status == ORDER_NEW)
        await ui_render(
            context,
            uid,
            render_client_status(o),
            reply_markup=kb_client_status(o, can_cancel)
        )
        return
      
    if data.startswith("client:cancel:"):
        order_id = data.split(":", 2)[2]
        await handle_client_cancel(query, context, uid, order_id)
        return

    if data.startswith("client:delete:"):
        order_id = data.split(":", 2)[2]
        await handle_client_delete_problem(query, context, uid, order_id)
        return

    if data == "client:orders:open":
        await ui_render(
            context,
            uid,
            "Выберите период:",
            reply_markup=kb_client_orders_filters()
        )
        return

    if data.startswith("client:orders:"):
        period = data.split(":")[-1]
        items = get_client_orders(uid)
        filtered = filter_orders_by_period(
            items,
            period if period in ("today", "week", "month") else "month"
        )

        await ui_render(
            context,
            uid,
            render_orders_list(filtered),
            reply_markup=kb_client_orders_filters()
        )
        return

    if data == "client:new_order":
        context.user_data.pop(UI_MSG_ID_KEY, None)
        context.user_data["draft_order"] = {}
        context.user_data[CLIENT_STATE_KEY] = C_PRICE_ZONE

        if SHEETS:
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_START_PRICE_ZONE")

        await ui_render(
            context,
            uid,
            "Выберите зону доставки:",
            reply_markup=kb_client_price_choice()
        )
        return

    if data == "client:price:local":
        if context.user_data.get(CLIENT_STATE_KEY) != C_PRICE_ZONE:
            return

        d = context.user_data.get("draft_order", {})
        d["zone"] = "dunpo"
        d["price_krw"] = DEFAULT_PRICE_KRW
        context.user_data["draft_order"] = d

        context.user_data[CLIENT_STATE_KEY] = C_PICKUP

        await ui_render(
            context,
            uid,
            "📍 Укажите адрес забора.\nАдрес нужно написать текстом и на корейском языке."
        )
        return
        
    if data == "client:price:custom":
        if context.user_data.get(CLIENT_STATE_KEY) != C_PRICE_ZONE:
            return

        d = context.user_data.get("draft_order", {})
        d["zone"] = "other"
        context.user_data["draft_order"] = d

        context.user_data[CLIENT_STATE_KEY] = C_PICKUP

        await ui_render(
            context,
            uid,
            "📍 Укажите адрес забора.\nАдрес нужно написать текстом и на корейском языке."
        )
        return

    if data == "client:price:accept_recommended":
        if context.user_data.get(CLIENT_STATE_KEY) != C_PRICE_RECOMMEND:
            return

        d = context.user_data.get("draft_order", {})
        rec = int(d.get("recommended_price_krw") or 0)
        if rec <= 0:
            # если вдруг пропало - уходим на ручной ввод
            context.user_data[CLIENT_STATE_KEY] = C_PRICE_FINAL
            await ui_render(context, uid, "Введите цену вручную (в вонах).")
            return

        d["price_krw"] = rec
        context.user_data["draft_order"] = d
        context.user_data[CLIENT_STATE_KEY] = C_CONFIRM

        await ui_render(
            context,
            uid,
            render_order_summary_for_confirm(d),
            reply_markup=kb_confirm_order()
        )
        return

    if data == "client:price:manual":
        if context.user_data.get(CLIENT_STATE_KEY) != C_PRICE_RECOMMEND:
            return

        context.user_data[CLIENT_STATE_KEY] = C_PRICE_FINAL
        await ui_render(context, uid, "Введите цену вручную (в вонах). Например: 12000")
        return



    if data == "client:door_none":
        if context.user_data.get(CLIENT_STATE_KEY) != C_DOOR:
            return

        d = context.user_data.get("draft_order", {})
        d["door_code"] = ""
        context.user_data["draft_order"] = d
        context.user_data[CLIENT_STATE_KEY] = C_TYPE
        if SHEETS:
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_DOOR_NONE")
        await ui_render(context, uid, "Выберите тип доставки.", reply_markup=kb_delivery_type())
        return

    if data.startswith("client:type:"):
        if context.user_data.get(CLIENT_STATE_KEY) != C_TYPE:
            return

        delivery_type = data.split(":")[-1]

        d = context.user_data.get("draft_order", {})
        d["delivery_type"] = delivery_type
        context.user_data["draft_order"] = d
        
        if delivery_type == "other":
            context.user_data[CLIENT_STATE_KEY] = C_TYPE_OTHER

            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TYPE_OTHER")

            await ui_render(
                context,
                uid,
                "Коротко опишите, что нужно доставить."
            )
            return

        # обычные типы доставки
        context.user_data[CLIENT_STATE_KEY] = C_TIME

        if SHEETS:
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TYPE", meta=delivery_type)

        await ui_render(
            context,
            update.effective_chat.id,
            "Когда нужна доставка?",
            reply_markup=kb_delivery_time()
        )
        return
    if data.startswith("client:time:"):
        if context.user_data.get(CLIENT_STATE_KEY) != C_TIME:
            return

        t = data.split(":")[-1]
        d = context.user_data.get("draft_order", {})

        if t in ("now", "today"):
            d["delivery_time_type"] = t
            d["delivery_time_text"] = ""
            context.user_data["draft_order"] = d
            context.user_data[CLIENT_STATE_KEY] = C_CONTACT
            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TIME", meta=t)
            await ui_render(context, uid, "Укажите контакт получателя.\nИмя и телефон или Telegram.")
            return

        d["delivery_time_type"] = "custom"
        context.user_data["draft_order"] = d
        context.user_data[CLIENT_STATE_KEY] = C_TIME_CUSTOM
        if SHEETS:
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TIME_CUSTOM")
        await ui_render(context, uid, "Напишите желаемое время доставки.")
        return

    if data.startswith("client:confirm:"):
        if context.user_data.get(CLIENT_STATE_KEY) != C_CONFIRM:
            return

        ans = data.split(":")[-1]

        # ---- CANCEL ----
        if ans == "no":
            context.user_data[CLIENT_STATE_KEY] = C_NONE
            context.user_data.pop("draft_order", None)
            context.user_data.pop(UI_MSG_ID_KEY, None)

            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_CANCEL_BEFORE_CREATE")

            await ui_render(
                context,
                uid,
                "❌ Заказ отменен.",
                reply_markup=kb_client_menu()
            )
            return

        # ---- CONFIRM ----
        d = context.user_data.get("draft_order", {})

        # Dunpo — фиксированная цена
        if d.get("zone") == "dunpo":
            d["price_krw"] = DEFAULT_PRICE_KRW
            context.user_data["draft_order"] = d

        price = int(d.get("price_krw") or 0)
        if price <= 0:
            context.user_data[CLIENT_STATE_KEY] = C_NONE
            context.user_data.pop("draft_order", None)
            context.user_data.pop(UI_MSG_ID_KEY, None)

            await ui_render(
                context,
                uid,
                "Не указана цена. Начните заново.",
                reply_markup=kb_client_menu()
            )
            return

        if not d.get("pickup_address_ko") or not d.get("drop_address_ko") or not d.get("recipient_contact_text"):
            context.user_data[CLIENT_STATE_KEY] = C_NONE
            context.user_data.pop("draft_order", None)
            context.user_data.pop(UI_MSG_ID_KEY, None)

            await ui_render(
                context,
                uid,
                "Не хватает данных. Начните заново.",
                reply_markup=kb_client_menu()
            )
            return

        order_id = SHEETS.next_order_id() if SHEETS else str(int(datetime.now().timestamp()))
        order = Order(
            order_id=order_id,
            created_at=now_ts(),
            location=LOC_DUNPO,
            price_krw=price,
            status=ORDER_NEW,

            client_tg_id=uid,
            client_username=uname,
            recipient_contact_text=d.get("recipient_contact_text", ""),

            pickup_address_ko=d.get("pickup_address_ko", ""),
            drop_address_ko=d.get("drop_address_ko", ""),
            door_code=d.get("door_code", ""),

            delivery_type=d.get("delivery_type", ""),
            delivery_type_other_text=d.get("delivery_type_other_text", ""),

            delivery_time_type=d.get("delivery_time_type", ""),
            delivery_time_text=d.get("delivery_time_text", ""),
        )

        ORDERS[order_id] = order

        if SHEETS:
            SHEETS.insert_order(asdict(order))
            SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_CONFIRMED", order_id=order_id)

        # ---- CLEAN EXIT ----
        context.user_data[CLIENT_STATE_KEY] = C_NONE
        context.user_data.pop("draft_order", None)
        context.user_data.pop(UI_MSG_ID_KEY, None)

        await ui_render(
            context,
            uid,
            "✅ Заказ принят.\nКурьер свяжется с вами напрямую."
        )
        await notify_new_order(context, order)
        return

    if data == "courier:apply":
        context.user_data[COURIER_STATE_KEY] = K_APPLY_NAME
        if SHEETS:
            SHEETS.log_event(uid, ROLE_COURIER, "COURIER_APPLY_START")
        await ui_render(context, uid, "Введите ваше имя.")
        return

    if data.startswith("badaddr:"):
        order_id = data.split(":", 1)[1]
        await handle_bad_address(query, context, uid, order_id)
        return

    if data.startswith("take:"):
        order_id = data.split(":", 1)[1]
        await handle_take_order(query, context, uid, order_id)
        return

    if data.startswith("progress:"):
        order_id = data.split(":", 1)[1]
        await handle_in_progress_clicked(query, context, uid, order_id)
        return

    if data.startswith("skip:"):
        if SHEETS:
            SHEETS.log_event(uid, ROLE_COURIER, "ORDER_SKIPPED", order_id=data.split(":", 1)[1])
        await ui_render(context, uid, "Ок.")
        return

    if data.startswith("done:"):
        order_id = data.split(":", 1)[1]
        await handle_done_clicked(query, context, uid, order_id)
        return

    if data.startswith("admin:"):
        if not is_admin(uid):
            await ui_render(context, uid, "Нет доступа.")
            return
        await handle_admin_callbacks(query, context, data)
        return


# =========================
# MESSAGE HANDLER
# =========================
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    if context.user_data.get(UI_RESET_KEY):
        log.info("MESSAGE IGNORED (reset in progress)")
        return
    
    if not update.effective_user or not update.message:
        return

    init_user_defaults(context)

    uid = update.effective_user.id
    uname = update.effective_user.username or ""
    text = (update.message.text or "").strip()

    if context.user_data.get(COURIER_STATE_KEY) == K_AWAITING_PROOF:
        if update.message.photo:
            await handle_proof_photo(update, context)
        else:
            await ui_render(
                context,
                update.effective_chat.id,
                "Нужен скриншот именно в виде изображения. Пожалуйста, отправьте фото."
            )
        return

    role = context.user_data.get(USER_ROLE_KEY, ROLE_UNKNOWN)

    S_client = context.user_data.get(CLIENT_STATE_KEY, C_NONE)

    # защитный сброс: если draft_order есть, но FSM выключен - чистим, чтобы не оживал флоу
    if S_client == C_NONE and "draft_order" in context.user_data:
        context.user_data.pop("draft_order", None)
        context.user_data.pop(UI_MSG_ID_KEY, None)

    # FSM клиента работает только когда state != C_NONE
    
    courier_state = context.user_data.get(COURIER_STATE_KEY, K_NONE)

    if courier_state != K_NONE and context.user_data.get(USER_ROLE_KEY) == ROLE_COURIER:
        # courier FSM
        
        prof = COURIERS.get(uid)

        if courier_state == K_APPLY_NAME:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Введите ваше имя."
                )
                return
            context.user_data["apply_name"] = text
            context.user_data[COURIER_STATE_KEY] = K_APPLY_PHONE
            await ui_render(
                context,
                update.effective_chat.id,
                "Введите номер телефона."
            )
            return

        if courier_state == K_APPLY_PHONE:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Введите номер телефона."
                )
                return
            context.user_data["apply_phone"] = text
            context.user_data[COURIER_STATE_KEY] = K_APPLY_TRANSPORT
            await ui_render(
                context,
                update.effective_chat.id,
                "Транспорт: Машина или Скутер?"
            )
            return

        if courier_state == K_APPLY_TRANSPORT:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Ответьте, машина или скутер."
                )
                return
            t = text.lower()
            transport = "car" if "маш" in t else "scooter" if "скут" in t else ""
            if not transport:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Ответьте, машина или скутер."
                )
                return

            name = context.user_data.get("apply_name", "")
            phone = context.user_data.get("apply_phone", "")

            prof = CourierProfile(
                courier_tg_id=uid,
                username=uname,
                name=name,
                phone=phone,
                transport=transport,
                status=COURIER_PENDING,
                applied_at=now_ts(),
            )
            COURIERS[uid] = prof

            if SHEETS:
                SHEETS.upsert_courier(asdict(prof))
                SHEETS.log_event(uid, ROLE_COURIER, "COURIER_APPLY_SUBMIT")

            context.user_data[COURIER_STATE_KEY] = K_NONE
            context.user_data.pop("apply_name", None)
            context.user_data.pop("apply_phone", None)

            await ui_render(
                context,
                update.effective_chat.id,
                "✅ Заявка отправлена.\nОжидайте одобрения администратора."
            )

            for admin_id in ADMIN_IDS:
                try:
                    text_admin = (
                        "🧍 Заявка курьера\n\n"
                        f"Имя: {name}\n"
                        f"Телефон: {phone}\n"
                        f"Транспорт: {transport}\n"
                        f"ID: {uid}"
                    )
                    await tg_retry(lambda aid=admin_id, tmsg=text_admin: context.bot.send_message(
                        chat_id=aid,
                        text=tmsg,
                        reply_markup=kb_admin_app_decision(uid)
                    ))
                except Exception as e:
                    log.warning("Admin app notify failed: %s", e)

            return

        
        elif prof and prof.status == COURIER_PENDING:
            await ui_render(
                    context,
                    update.effective_chat.id,
                    "Заявка отправлена. Ожидайте одобрения администратора."
                )
        else:
            await ui_render(
                    context,
                    update.effective_chat.id,
                    "Нажмите /start и выберите роль."
                )
        return

    
    
    if S_client != C_NONE:
        d = context.user_data.get("draft_order", {})
        
        if S_client == C_PRICE_FINAL:
            price = parse_price_krw(text)
            
            if price is None:
                await ui_render(
                    context,
                    uid,
                    "Введите сумму числом (1000–300000). Например: 12000"
                )
                return

            d["price_krw"] = price
            context.user_data["draft_order"] = d

            context.user_data[CLIENT_STATE_KEY] = C_CONFIRM

            await ui_render(
                context,
                uid,
                render_order_summary_for_confirm(d),
                reply_markup=kb_confirm_order()
            )
            return

        if S_client == C_PICKUP:
            if not is_korean_address(text):
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "📍 Адрес забора должен быть на корейском языке.\nПожалуйста, попробуйте еще раз."
                )
                return

            d["pickup_address_ko"] = text
            context.user_data["draft_order"] = d   # ОБЯЗАТЕЛЬНО
            context.user_data[CLIENT_STATE_KEY] = C_DROP

            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_PICKUP")

            await ui_render(
                context,
                update.effective_chat.id,
                "Укажите адрес доставки. Адрес нужно написать текстом на корейском языке."
            )
            return

        if S_client == C_DROP:
            if not is_korean_address(text):
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Пожалуйста, укажите адрес на корейском языке. Это нужно для навигатора."
                )
                return

            d["drop_address_ko"] = text
            context.user_data["draft_order"] = d

            pickup = d.get("pickup_address_ko")
            dropoff = d.get("drop_address_ko")

            log.info(f"ROUTE CHECK from='{pickup}' to='{dropoff}'")

            context.user_data[CLIENT_STATE_KEY] = C_DOOR

            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_DROP")

            await ui_render(
                context,
                update.effective_chat.id,
                "🔒 Если нужен код подъезда или домофона, напишите его.\nЕсли кода нет, нажмите кнопку ниже.",
                reply_markup=kb_door_code()
            )
            return

        if S_client == C_DOOR:
            d["door_code"] = text
            context.user_data["draft_order"] = d
            context.user_data[CLIENT_STATE_KEY] = C_TYPE
            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_DOOR_TEXT")
            await ui_render(
                context,
                update.effective_chat.id,
                "Выберите тип доставки.",
                reply_markup=kb_delivery_type()
            )
            return

        
        if S_client == C_TYPE_OTHER:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Коротко опишите, что нужно доставить."
                )
                return
            d["delivery_type_other_text"] = text
            context.user_data["draft_order"] = d
            context.user_data[CLIENT_STATE_KEY] = C_TIME
            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TYPE_OTHER_TEXT")
            await ui_render(
                    context,
                    update.effective_chat.id,
                    "Когда нужна доставка?",
                    reply_markup=kb_delivery_time()
            )
            return

        if S_client == C_TIME_CUSTOM:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Напишите желаемое время доставки."
                )
                return
            d["delivery_time_type"] = "custom"
            d["delivery_time_text"] = text
            context.user_data["draft_order"] = d
            context.user_data[CLIENT_STATE_KEY] = C_CONTACT
            if SHEETS:
                SHEETS.log_event(uid, ROLE_CLIENT, "ORDER_STEP_TIME_CUSTOM_TEXT")
            await ui_render(
                    context,
                    update.effective_chat.id,
                    "Укажите контакт получателя.\nИмя и телефон или Telegram."
            )
            return

        if S_client == C_CONTACT:
            if not text:
                await ui_render(
                    context,
                    update.effective_chat.id,
                    "Укажите контакт получателя.\nИмя и телефон или Telegram."
                )
                return

            d["recipient_contact_text"] = text
            context.user_data["draft_order"] = d

            # Dunpo — сразу подтверждение
            if d.get("zone") == "dunpo":
                d["price_krw"] = DEFAULT_PRICE_KRW
                context.user_data["draft_order"] = d
                context.user_data[CLIENT_STATE_KEY] = C_CONFIRM

                # 🔑 ЖЕСТКО РВЕМ СТАРЫЙ UI
                context.user_data.pop(UI_MSG_ID_KEY, None)

                await ui_render(
                    context,
                    uid,
                    render_order_summary_for_confirm(d),
                    reply_markup=kb_confirm_order()
                )
                return

            # Other районы - сначала показываем рекомендованную цену
            recommended = calc_recommended_price_krw(
                d.get("pickup_address_ko", ""),
                d.get("drop_address_ko", "")
            )

            if recommended:
                d["recommended_price_krw"] = recommended
                context.user_data["draft_order"] = d
                context.user_data[CLIENT_STATE_KEY] = C_PRICE_RECOMMEND

                await ui_render(
                    context,
                    uid,
                    (
                        f"💰 Рекомендованная цена: {recommended} ~вон\n"
                        f"(расчет: {PRICE_PER_KM_KRW} вон за км)\n\n"
                        "Принять эту цену или ввести свою?"
                    ),
                    reply_markup=kb_client_price_recommend()
                )
                return

            # fallback - как было раньше
            context.user_data[CLIENT_STATE_KEY] = C_PRICE_FINAL
            await ui_render(
                context,
                uid,
                "💰 Не удалось рассчитать маршрут. Укажите цену вручную (в вонах)."
            )
            return
        
    # если мы здесь — просто игнорируем
    log.info("MESSAGE IGNORED (no active FSM)")
    return


# =========================
# STARTUP HOOK
# =========================
async def on_startup(app: Application):
    global SHEETS

    try:
        # --- Sheets init ---
        service = build_sheets_service()
        SHEETS = SheetsStore(service, SHEET_ID)
        SHEETS.ensure_structure()
        SHEETS.warm_cache()

        # --- Load couriers ---
        COURIERS.clear()
        for c in SHEETS.load_all_couriers():
            try:
                cid = int(str(c.get("courier_tg_id", "")).strip())
            except Exception:
                continue

            COURIERS[cid] = CourierProfile(
                courier_tg_id=cid,
                username=c.get("username", ""),
                name=c.get("name", ""),
                phone=c.get("phone", ""),
                transport=c.get("transport", ""),
                status=(c.get("status", "") or "").strip().upper(),
                applied_at=c.get("applied_at", ""),
                approved_at=c.get("approved_at", ""),
                rejected_at=c.get("rejected_at", ""),
            )

        # --- Load orders ---
        ORDERS.clear()
        for o in SHEETS.load_all_orders():
            oid = str(o.get("order_id", "")).strip()
            if not oid:
                continue

            try:
                price = int(str(o.get("price_krw", "")).strip() or "0")
            except Exception:
                price = 0

            try:
                client_id = int(str(o.get("client_tg_id", "")).strip() or "0")
            except Exception:
                client_id = 0

            try:
                courier_id = int(str(o.get("courier_tg_id", "")).strip() or "0")
            except Exception:
                courier_id = 0

            ORDERS[oid] = Order(
                order_id=oid,
                created_at=o.get("created_at", ""),
                location=o.get("location", ""),
                price_krw=price,
                status=(o.get("status", "") or "").strip().upper(),

                client_tg_id=client_id,
                client_username=o.get("client_username", ""),
                recipient_contact_text=o.get("recipient_contact_text", ""),

                pickup_address_ko=o.get("pickup_address_ko", ""),
                drop_address_ko=o.get("drop_address_ko", ""),
                door_code=o.get("door_code", ""),

                delivery_type=o.get("delivery_type", ""),
                delivery_type_other_text=o.get("delivery_type_other_text", ""),
                delivery_time_type=o.get("delivery_time_type", ""),
                delivery_time_text=o.get("delivery_time_text", ""),

                taken_at=o.get("taken_at", ""),
                courier_tg_id=courier_id,
                courier_name=o.get("courier_name", ""),
                courier_phone=o.get("courier_phone", ""),

                in_progress_at=o.get("in_progress_at", ""),
                done_requested_at=o.get("done_requested_at", ""),
                completed_at=o.get("completed_at", ""),
                proof_image_file_id=o.get("proof_image_file_id", ""),
                proof_image_message_id=o.get("proof_image_message_id", ""),

                canceled_at=o.get("canceled_at", ""),
                canceled_by=o.get("canceled_by", ""),
            )

        log.info(
            "Sheets ready. Last order id: %s | couriers: %s | orders: %s",
            SHEETS.last_order_num, len(COURIERS), len(ORDERS)
        )

    except Exception:
        log.exception("FATAL startup error")
        raise
    
    
async def cmd_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    context.user_data.clear()
    context.user_data[UI_MSG_ID_KEY] = None
    init_user_defaults(context)

    await render_home_root(context, uid)


# =========================
# MAIN
# =========================
def main():
    Thread(target=run_http, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("go", cmd_go))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO, on_message))

    log.info("Bot starting...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
