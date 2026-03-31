#!/usr/bin/env python3
"""
THE VAULT - Telegram Micro-Economy Bot
Usage: python3 bot.py BOT_TOKEN ADMIN_USER_ID
Dependencies: pip install python-telegram-bot==20.7 httpx
"""
import sys, os, sqlite3, json, logging, asyncio, hashlib, random, string, re
from datetime import datetime, timedelta
from functools import wraps

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", "0"))
WEBHOOK_URL = os.environ.get("RENDER_EXTERNAL_URL", "")  # Set by Render
PORT = int(os.environ.get("PORT", "10000"))

if not BOT_TOKEN:
    print("Set BOT_TOKEN environment variable"); sys.exit(1)
if not ADMIN_USER_ID:
    print("Set ADMIN_USER_ID environment variable"); sys.exit(1)

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
    from telegram.constants import ParseMode
    from telegram.error import BadRequest, TelegramError
except ImportError:
    print("Install: pip install python-telegram-bot==20.7"); sys.exit(1)
import httpx

os.makedirs("data", exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("data/bot.log"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

DB_PATH = "data/vault.db"
# Global conversation states
user_states = {}

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def get_setting(key, default=''):
    try:
        conn = get_db()
        r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return r['value'] if r else default
    except:
        return default

def set_setting(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '',
        balance REAL DEFAULT 0.0, frozen_balance REAL DEFAULT 0.0,
        total_earned REAL DEFAULT 0.0, total_spent REAL DEFAULT 0.0, total_withdrawn REAL DEFAULT 0.0,
        reputation_score REAL DEFAULT 0.0, completed_gigs INTEGER DEFAULT 0, failed_gigs INTEGER DEFAULT 0,
        total_ratings INTEGER DEFAULT 0, rating_sum REAL DEFAULT 0.0,
        referral_code TEXT UNIQUE, referred_by INTEGER DEFAULT 0,
        referral_count INTEGER DEFAULT 0, referral_earnings REAL DEFAULT 0.0,
        is_premium INTEGER DEFAULT 0, premium_expires TEXT DEFAULT '',
        is_seller_verified INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, ban_reason TEXT DEFAULT '',
        risk_score REAL DEFAULT 0.0, last_ip TEXT DEFAULT '', is_vpn_detected INTEGER DEFAULT 0,
        vpn_check_count INTEGER DEFAULT 0, device_fingerprint TEXT DEFAULT '',
        total_disputes_filed INTEGER DEFAULT 0, total_disputes_lost INTEGER DEFAULT 0,
        badges TEXT DEFAULT '[]', bio TEXT DEFAULT '', skills TEXT DEFAULT '[]',
        portfolio_links TEXT DEFAULT '[]',
        joined_at TEXT DEFAULT CURRENT_TIMESTAMP, last_active TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS gigs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, poster_id INTEGER NOT NULL,
        title TEXT NOT NULL, description TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'Other',
        budget REAL NOT NULL, deadline_hours INTEGER DEFAULT 48,
        required_reputation REAL DEFAULT 0.0, required_level INTEGER DEFAULT 0,
        max_applicants INTEGER DEFAULT 5, status TEXT DEFAULT 'open',
        claimed_by INTEGER DEFAULT 0, claimed_at TEXT DEFAULT '', delivered_at TEXT DEFAULT '',
        completed_at TEXT DEFAULT '', delivery_file_id TEXT DEFAULT '', delivery_text TEXT DEFAULT '',
        poster_rating REAL DEFAULT 0, worker_rating REAL DEFAULT 0,
        is_featured INTEGER DEFAULT 0, view_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (poster_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS gig_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, gig_id INTEGER NOT NULL, applicant_id INTEGER NOT NULL,
        proposal_text TEXT DEFAULT '', proposed_budget REAL DEFAULT 0, status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (gig_id) REFERENCES gigs(id), FOREIGN KEY (applicant_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT, seller_id INTEGER NOT NULL,
        title TEXT NOT NULL, description TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'Other',
        price REAL NOT NULL, file_id TEXT NOT NULL, preview_file_id TEXT DEFAULT '',
        preview_text TEXT DEFAULT '', total_sales INTEGER DEFAULT 0, total_revenue REAL DEFAULT 0.0,
        avg_rating REAL DEFAULT 0.0, total_ratings INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1, is_approved INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (seller_id) REFERENCES users(user_id)
    );
    CREATE TABLE IF NOT EXISTS product_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL, buyer_id INTEGER NOT NULL,
        seller_id INTEGER NOT NULL, price REAL NOT NULL, platform_fee REAL DEFAULT 0.0,
        rating REAL DEFAULT 0, review_text TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(id)
    );
    CREATE TABLE IF NOT EXISTS escrow (
        id INTEGER PRIMARY KEY AUTOINCREMENT, gig_id INTEGER NOT NULL,
        payer_id INTEGER NOT NULL, payee_id INTEGER NOT NULL, amount REAL NOT NULL,
        platform_fee REAL DEFAULT 0.0, status TEXT DEFAULT 'held',
        released_at TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (gig_id) REFERENCES gigs(id)
    );
    CREATE TABLE IF NOT EXISTS disputes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, gig_id INTEGER DEFAULT 0,
        product_purchase_id INTEGER DEFAULT 0, filed_by INTEGER NOT NULL,
        filed_against INTEGER NOT NULL, reason TEXT NOT NULL,
        evidence_file_ids TEXT DEFAULT '[]', evidence_text TEXT DEFAULT '',
        admin_notes TEXT DEFAULT '', status TEXT DEFAULT 'open', resolution TEXT DEFAULT '',
        resolved_by TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP, resolved_at TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount REAL NOT NULL,
        type TEXT NOT NULL, reference_type TEXT DEFAULT '', reference_id INTEGER DEFAULT 0,
        description TEXT DEFAULT '', balance_after REAL DEFAULT 0.0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS deposits (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
        amount_inr REAL NOT NULL, vault_coins REAL NOT NULL, proof_file_id TEXT DEFAULT '',
        payment_method TEXT DEFAULT 'UPI', status TEXT DEFAULT 'pending',
        admin_note TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
        vault_coins REAL NOT NULL, amount_inr REAL NOT NULL, payout_method TEXT DEFAULT 'UPI',
        payout_details TEXT DEFAULT '', status TEXT DEFAULT 'pending',
        admin_note TEXT DEFAULT '', processed_at TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS premium_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, plan TEXT NOT NULL,
        price REAL NOT NULL, duration_days INTEGER NOT NULL,
        started_at TEXT DEFAULT CURRENT_TIMESTAMP, expires_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS referral_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER NOT NULL, referred_id INTEGER NOT NULL,
        trigger_type TEXT NOT NULL, reward_amount REAL NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS security_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, event_type TEXT NOT NULL,
        ip_address TEXT DEFAULT '', is_vpn INTEGER DEFAULT 0, is_proxy INTEGER DEFAULT 0,
        is_tor INTEGER DEFAULT 0, country TEXT DEFAULT '', isp TEXT DEFAULT '',
        risk_score REAL DEFAULT 0.0, raw_response TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, reported_id INTEGER NOT NULL,
        reason TEXT NOT NULL, evidence TEXT DEFAULT '', status TEXT DEFAULT 'pending',
        admin_action TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS badge_definitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
        emoji TEXT NOT NULL, description TEXT NOT NULL, criteria_type TEXT NOT NULL,
        criteria_value REAL NOT NULL, is_active INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, message TEXT NOT NULL,
        is_read INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, emoji TEXT DEFAULT '📁',
        description TEXT DEFAULT '', is_active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at);
    CREATE INDEX IF NOT EXISTS idx_gigs_status ON gigs(status);
    CREATE INDEX IF NOT EXISTS idx_gigs_cat ON gigs(category);
    CREATE INDEX IF NOT EXISTS idx_gigs_poster ON gigs(poster_id);
    CREATE INDEX IF NOT EXISTS idx_prod_cat ON products(category);
    CREATE INDEX IF NOT EXISTS idx_prod_seller ON products(seller_id);
    CREATE INDEX IF NOT EXISTS idx_escrow_st ON escrow(status);
    CREATE INDEX IF NOT EXISTS idx_disp_st ON disputes(status);
    CREATE INDEX IF NOT EXISTS idx_dep_st ON deposits(status);
    CREATE INDEX IF NOT EXISTS idx_wd_st ON withdrawals(status);
    CREATE INDEX IF NOT EXISTS idx_sec_user ON security_logs(user_id);
    CREATE INDEX IF NOT EXISTS idx_sec_created ON security_logs(created_at);
    CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id);
    CREATE INDEX IF NOT EXISTS idx_ga_gig ON gig_applications(gig_id);
    CREATE INDEX IF NOT EXISTS idx_pp_buyer ON product_purchases(buyer_id);
    """)
    # Seed settings
    defaults = {
        'bot_name': '⚡ THE VAULT', 'currency_name': 'Vault Coins', 'currency_symbol': '🪙',
        'currency_code': 'VC', 'inr_to_vc_rate': '10', 'vc_to_inr_rate': '0.05',
        'platform_fee_percent': '12', 'premium_fee_percent': '8',
        'min_deposit_inr': '50', 'max_deposit_inr': '10000',
        'min_withdrawal_vc': '2000', 'max_withdrawal_vc': '100000',
        'withdrawal_enabled': '0', 'withdrawal_fee_percent': '10',
        'upi_id': '', 'payment_instructions': 'Send payment to the UPI ID shown and upload screenshot as proof',
        'premium_enabled': '1', 'premium_monthly_price': '500',
        'premium_quarterly_price': '1200', 'premium_yearly_price': '4000',
        'premium_discount_percent': '0',
        'premium_features': 'Reduced fees (8% vs 12%)|Featured listings|Verified badge|Priority support|Advanced analytics|Unlimited active gigs|Custom profile bio',
        'referral_enabled': '1', 'referral_bonus_vc': '50',
        'referral_percent_on_transactions': '5', 'referral_max_percent_transactions': '10',
        'referral_required_for_bonus': '1',
        'welcome_message': 'The decentralized marketplace on Telegram. Trade skills, sell products, build reputation.',
        'support_username': '', 'maintenance_mode': '0', 'new_user_bonus': '25',
        'min_gig_budget': '20', 'max_gig_budget': '50000',
        'max_active_gigs_free': '3', 'max_active_gigs_premium': '20',
        'max_active_products_free': '5', 'max_active_products_premium': '50',
        'escrow_release_hours': '72', 'auto_complete_hours': '168',
        'vpn_detection_enabled': '0', 'vpn_detection_api_key': '',
        'vpn_detection_api_url': 'https://vpnapi.io/api/', 'vpn_block_mode': 'warn',
        'max_risk_score': '80', 'ip_check_on_deposit': '1', 'ip_check_on_withdrawal': '1',
        'ip_check_on_registration': '0', 'require_verified_for_withdrawal': '1',
        'min_reputation_for_withdrawal': '2.0', 'min_completed_gigs_for_withdrawal': '3',
        'broadcast_footer': '', 'featured_gig_cost': '100', 'featured_product_cost': '150',
        'boost_duration_hours': '24', 'platform_total_fees': '0',
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    cats = [('Writing','✍️'),('Design','🎨'),('Programming','💻'),('Marketing','📢'),
            ('Video/Audio','🎬'),('Data Entry','📊'),('Translation','🌐'),('Education','📚'),
            ('Social Media','📱'),('Business','💼'),('Music','🎵'),('Other','📦')]
    for i,(n,e) in enumerate(cats):
        c.execute("INSERT OR IGNORE INTO categories (name,emoji,sort_order) VALUES (?,?,?)",(n,e,i))
    badges = [
        ('first_gig','First Gig','🌟','Complete your first gig','completed_gigs',1),
        ('five_star','Five Star','⭐','Receive a 5-star rating','five_star_ratings',1),
        ('streak_7','On Fire','🔥','7-day activity streak','streak_days',7),
        ('big_spender','Big Spender','💰','Spend 5000 VC total','total_spent',5000),
        ('top_seller','Top Seller','🏆','Earn 10000 VC from sales','total_earned',10000),
        ('trusted','Trusted','🛡️','Complete 25 gigs with no disputes','completed_gigs_clean',25),
        ('referral_king','Referral King','👑','Refer 10 users','referral_count',10),
        ('veteran','Veteran','🎖️','Member for 90 days','account_age_days',90),
        ('perfectionist','Perfectionist','💎','Maintain 4.8+ rating over 20 ratings','high_rating',20),
        ('centurion','Centurion','🏛️','Complete 100 transactions','total_transactions',100),
    ]
    for code,name,emoji,desc,ct,cv in badges:
        c.execute("INSERT OR IGNORE INTO badge_definitions (code,name,emoji,description,criteria_type,criteria_value) VALUES (?,?,?,?,?,?)",
                  (code,name,emoji,desc,ct,cv))
    conn.commit(); conn.close()
    logger.info("Database initialized")


# ============================================================
# ECONOMY ENGINE
# ============================================================

def gen_referral_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def ensure_user(user_id, username='', first_name=''):
    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not u:
        code = gen_referral_code()
        conn.execute("INSERT INTO users (user_id, username, first_name, referral_code) VALUES (?,?,?,?)",
                      (user_id, username or '', first_name or '', code))
        bonus = float(get_setting('new_user_bonus', '0'))
        if bonus > 0:
            conn.execute("UPDATE users SET balance=? WHERE user_id=?", (bonus, user_id))
            conn.execute("INSERT INTO transactions (user_id, amount, type, description, balance_after) VALUES (?,?,?,?,?)",
                          (user_id, bonus, 'welcome_bonus', f'Welcome bonus: {bonus} VC', bonus))
        conn.commit()
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    else:
        conn.execute("UPDATE users SET username=?, first_name=?, last_active=CURRENT_TIMESTAMP WHERE user_id=?",
                      (username or u['username'], first_name or u['first_name'], user_id))
        conn.commit()
    conn.close()
    return dict(u)

def add_balance(user_id, amount, txn_type, description, ref_type='', ref_id=0):
    conn = get_db()
    try:
        conn.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
        if txn_type in ('gig_earning', 'product_sale'):
            conn.execute("UPDATE users SET total_earned = total_earned + ? WHERE user_id=?", (amount, user_id))
        u = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        bal = u['balance']
        conn.execute("INSERT INTO transactions (user_id, amount, type, reference_type, reference_id, description, balance_after) VALUES (?,?,?,?,?,?,?)",
                      (user_id, amount, txn_type, ref_type, ref_id, description, bal))
        conn.commit()
        conn.close()
        return bal
    except Exception as e:
        conn.rollback(); conn.close()
        logger.error(f"add_balance error: {e}")
        return None

def deduct_balance(user_id, amount, txn_type, description, ref_type='', ref_id=0):
    conn = get_db()
    try:
        u = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not u or u['balance'] < amount:
            conn.close(); return False
        conn.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
        if txn_type in ('gig_payment', 'product_purchase', 'premium_purchase', 'featured_listing'):
            conn.execute("UPDATE users SET total_spent = total_spent + ? WHERE user_id=?", (amount, user_id))
        elif txn_type == 'withdrawal':
            conn.execute("UPDATE users SET total_withdrawn = total_withdrawn + ? WHERE user_id=?", (amount, user_id))
        u2 = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        conn.execute("INSERT INTO transactions (user_id, amount, type, reference_type, reference_id, description, balance_after) VALUES (?,?,?,?,?,?,?)",
                      (user_id, -amount, txn_type, ref_type, ref_id, description, u2['balance']))
        conn.commit(); conn.close()
        return True
    except Exception as e:
        conn.rollback(); conn.close()
        logger.error(f"deduct_balance error: {e}"); return False

def freeze_balance(user_id, amount):
    conn = get_db()
    try:
        u = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not u or u['balance'] < amount:
            conn.close(); return False
        conn.execute("UPDATE users SET balance=balance-?, frozen_balance=frozen_balance+? WHERE user_id=?",
                      (amount, amount, user_id))
        conn.commit(); conn.close(); return True
    except:
        conn.rollback(); conn.close(); return False

def unfreeze_balance(user_id, amount):
    conn = get_db()
    try:
        conn.execute("UPDATE users SET balance=balance+?, frozen_balance=frozen_balance-? WHERE user_id=?",
                      (amount, amount, user_id))
        conn.commit(); conn.close(); return True
    except:
        conn.rollback(); conn.close(); return False

def transfer_frozen_to_user(from_id, to_id, amount, fee_pct):
    conn = get_db()
    try:
        fee = round(amount * fee_pct / 100, 2)
        net = round(amount - fee, 2)
        conn.execute("UPDATE users SET frozen_balance=frozen_balance-? WHERE user_id=?", (amount, from_id))
        conn.execute("UPDATE users SET balance=balance+?, total_earned=total_earned+? WHERE user_id=?", (net, net, to_id))
        to_u = conn.execute("SELECT balance FROM users WHERE user_id=?", (to_id,)).fetchone()
        conn.execute("INSERT INTO transactions (user_id, amount, type, description, balance_after) VALUES (?,?,?,?,?)",
                      (to_id, net, 'gig_earning', f'Gig payment received (fee: {fee} VC)', to_u['balance']))
        from_u = conn.execute("SELECT balance FROM users WHERE user_id=?", (from_id,)).fetchone()
        conn.execute("INSERT INTO transactions (user_id, amount, type, description, balance_after) VALUES (?,?,?,?,?)",
                      (from_id, -amount, 'escrow_release', f'Escrow released ({fee} VC fee)', from_u['balance']))
        # Track platform fees
        total_fees = float(get_setting('platform_total_fees', '0')) + fee
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES ('platform_total_fees',?)", (str(total_fees),))
        conn.commit(); conn.close()
        return True, net, fee
    except Exception as e:
        conn.rollback(); conn.close()
        logger.error(f"transfer error: {e}"); return False, 0, 0

# ============================================================
# REPUTATION & LEVELING
# ============================================================

def calculate_reputation(user):
    if user['total_ratings'] == 0:
        return 0.0
    avg_rating = user['rating_sum'] / user['total_ratings']
    completion_rate = user['completed_gigs'] / max(user['completed_gigs'] + user['failed_gigs'], 1)
    volume_score = min(user['completed_gigs'] / 50, 1.0)
    dispute_rate = user['total_disputes_lost'] / max(user['completed_gigs'], 1)
    try:
        days_active = (datetime.now() - datetime.fromisoformat(user['joined_at'])).days
    except:
        days_active = 0
    age_score = min(days_active / 180, 1.0)
    reputation = (
        (avg_rating / 5.0) * 0.35 +
        completion_rate * 0.25 +
        volume_score * 0.20 +
        age_score * 0.10 +
        (1 - min(dispute_rate, 1.0)) * 0.10
    ) * 5.0
    return round(reputation, 2)

def calculate_level(user):
    xp = (user['completed_gigs'] * 100 + user['total_ratings'] * 20 +
          int(user['total_earned'] / 10) + user['referral_count'] * 50)
    level = 0
    thresholds = [0, 100, 300, 600, 1000, 1500, 2500, 4000, 6000, 8500, 12000]
    for i, t in enumerate(thresholds):
        if xp >= t:
            level = i
    return level, xp

def get_level_title(level):
    titles = ['Newcomer', 'Apprentice', 'Contributor', 'Skilled', 'Expert',
              'Master', 'Guru', 'Legend', 'Mythic', 'Immortal', 'Transcendent']
    return titles[min(level, len(titles)-1)]

def update_reputation(user_id):
    conn = get_db()
    u = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone())
    rep = calculate_reputation(u)
    conn.execute("UPDATE users SET reputation_score=? WHERE user_id=?", (rep, user_id))
    conn.commit(); conn.close()
    return rep

async def check_badges(user_id, bot):
    conn = get_db()
    u = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone())
    current = json.loads(u.get('badges', '[]'))
    badges = conn.execute("SELECT * FROM badge_definitions WHERE is_active=1").fetchall()
    new_badges = []
    for b in badges:
        if b['code'] in current:
            continue
        earned = False
        ct, cv = b['criteria_type'], b['criteria_value']
        if ct == 'completed_gigs' and u['completed_gigs'] >= cv: earned = True
        elif ct == 'total_spent' and u['total_spent'] >= cv: earned = True
        elif ct == 'total_earned' and u['total_earned'] >= cv: earned = True
        elif ct == 'referral_count' and u['referral_count'] >= cv: earned = True
        elif ct == 'total_transactions':
            cnt = conn.execute("SELECT COUNT(*) as c FROM transactions WHERE user_id=?", (user_id,)).fetchone()['c']
            if cnt >= cv: earned = True
        elif ct == 'account_age_days':
            try:
                days = (datetime.now() - datetime.fromisoformat(u['joined_at'])).days
                if days >= cv: earned = True
            except: pass
        elif ct == 'completed_gigs_clean' and u['completed_gigs'] >= cv and u['total_disputes_lost'] == 0: earned = True
        elif ct == 'high_rating' and u['total_ratings'] >= cv:
            avg = u['rating_sum'] / u['total_ratings'] if u['total_ratings'] > 0 else 0
            if avg >= 4.8: earned = True
        elif ct == 'five_star_ratings':
            # Check if user has any 5-star ratings
            g5 = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE claimed_by=? AND worker_rating=5", (user_id,)).fetchone()['c']
            p5 = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE poster_id=? AND poster_rating=5", (user_id,)).fetchone()['c']
            if g5 + p5 >= cv: earned = True
        if earned:
            current.append(b['code'])
            new_badges.append(b)
    if new_badges:
        conn.execute("UPDATE users SET badges=? WHERE user_id=?", (json.dumps(current), user_id))
        conn.commit()
        for nb in new_badges:
            try:
                await bot.send_message(user_id,
                    f"🏅 <b>Badge Earned!</b>\n\n{nb['emoji']} <b>{nb['name']}</b>\n{nb['description']}",
                    parse_mode=ParseMode.HTML)
            except: pass
    conn.close()
    return new_badges

# ============================================================
# SECURITY / FRAUD DETECTION
# ============================================================

async def check_vpn(user_id, ip_address=''):
    enabled = get_setting('vpn_detection_enabled', '0')
    api_key = get_setting('vpn_detection_api_key', '')
    api_url = get_setting('vpn_detection_api_url', '')
    if enabled != '1' or not api_key or not ip_address:
        return {'is_vpn': False, 'is_proxy': False, 'is_tor': False, 'country': '', 'isp': '', 'risk_score': 0}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{api_url}{ip_address}?key={api_key}")
            data = resp.json()
        is_vpn = data.get('security', {}).get('vpn', False) or data.get('vpn', False)
        is_proxy = data.get('security', {}).get('proxy', False) or data.get('proxy', False)
        is_tor = data.get('security', {}).get('tor', False) or data.get('tor', False)
        country = data.get('location', {}).get('country', '') or data.get('country', '')
        isp = data.get('network', {}).get('isp', '') or data.get('isp', '')
        result = {'is_vpn': is_vpn, 'is_proxy': is_proxy, 'is_tor': is_tor,
                  'country': country, 'isp': isp, 'risk_score': 0}
        conn = get_db()
        conn.execute("INSERT INTO security_logs (user_id,event_type,ip_address,is_vpn,is_proxy,is_tor,country,isp,raw_response) VALUES (?,?,?,?,?,?,?,?,?)",
                      (user_id, 'vpn_check', ip_address, int(is_vpn), int(is_proxy), int(is_tor), country, isp, json.dumps(data)))
        conn.execute("UPDATE users SET last_ip=?, is_vpn_detected=?, vpn_check_count=vpn_check_count+1 WHERE user_id=?",
                      (ip_address, int(is_vpn or is_proxy or is_tor), user_id))
        conn.commit(); conn.close()
        return result
    except Exception as e:
        logger.error(f"VPN check error: {e}")
        return {'is_vpn': False, 'is_proxy': False, 'is_tor': False, 'country': '', 'isp': '', 'risk_score': 0}

def calculate_risk_score(user_id):
    conn = get_db()
    u = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone())
    score = 0
    if u['is_vpn_detected']: score += 30
    if u['total_disputes_lost'] > 0 and u['completed_gigs'] > 0:
        if u['total_disputes_lost'] / u['completed_gigs'] > 0.2: score += 15
    try:
        days = (datetime.now() - datetime.fromisoformat(u['joined_at'])).days
        if days < 1: score += 10
    except: pass
    # Check rapid transactions
    recent = conn.execute("SELECT COUNT(*) as c FROM transactions WHERE user_id=? AND created_at > datetime('now','-1 hour')", (user_id,)).fetchone()['c']
    if recent > 10: score += 15
    # Check same IP duplicates
    if u['last_ip']:
        dupes = conn.execute("SELECT COUNT(*) as c FROM users WHERE last_ip=? AND user_id!=?", (u['last_ip'], user_id)).fetchone()['c']
        score += min(dupes * 20, 40)
    # Withdraw-only pattern
    if u['total_withdrawn'] > 0 and u['completed_gigs'] == 0 and u['total_earned'] == 0: score += 20
    score = min(score, 100)
    conn.execute("UPDATE users SET risk_score=? WHERE user_id=?", (score, user_id))
    conn.commit(); conn.close()
    return score

def get_flagged_users():
    conn = get_db()
    users = conn.execute("SELECT * FROM users WHERE risk_score >= 30 ORDER BY risk_score DESC LIMIT 50").fetchall()
    conn.close()
    return [dict(u) for u in users]

def detect_circular_transactions(user_id):
    conn = get_db()
    # Find if A sent to B and B sent back similar amount
    sent = conn.execute("""SELECT DISTINCT g.claimed_by as other_id, g.budget FROM gigs g
        WHERE g.poster_id=? AND g.status='completed'""", (user_id,)).fetchall()
    flags = []
    for s in sent:
        reverse = conn.execute("""SELECT COUNT(*) as c FROM gigs WHERE poster_id=? AND claimed_by=?
            AND status='completed' AND budget BETWEEN ? AND ?""",
            (s['other_id'], user_id, s['budget']*0.8, s['budget']*1.2)).fetchone()['c']
        if reverse > 0:
            flags.append(s['other_id'])
    conn.close()
    return flags


# ============================================================
# NOTIFICATION HELPERS
# ============================================================

async def notify_user(bot, user_id, message):
    try:
        conn = get_db()
        conn.execute("INSERT INTO notifications (user_id, message) VALUES (?, ?)", (user_id, message))
        conn.commit(); conn.close()
        await bot.send_message(user_id, message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Notify error {user_id}: {e}")

def get_categories():
    conn = get_db()
    cats = conn.execute("SELECT * FROM categories WHERE is_active=1 ORDER BY sort_order").fetchall()
    conn.close()
    return [dict(c) for c in cats]

def is_premium(user):
    if not user['is_premium']:
        return False
    if user['premium_expires']:
        try:
            exp = datetime.fromisoformat(user['premium_expires'])
            if exp < datetime.now():
                conn = get_db()
                conn.execute("UPDATE users SET is_premium=0 WHERE user_id=?", (user['user_id'],))
                conn.commit(); conn.close()
                return False
        except:
            pass
    return True

def get_fee_percent(user):
    if is_premium(user):
        return float(get_setting('premium_fee_percent', '8'))
    return float(get_setting('platform_fee_percent', '12'))

# ============================================================
# KEYBOARD BUILDERS
# ============================================================

def main_menu_kb(user_id):
    is_admin = (user_id == ADMIN_USER_ID)
    kb = [
        [InlineKeyboardButton("💼 Gig Marketplace", callback_data="gigs_menu"),
         InlineKeyboardButton("🛍️ Digital Store", callback_data="store_menu")],
        [InlineKeyboardButton("💰 My Wallet", callback_data="wallet"),
         InlineKeyboardButton("👤 My Profile", callback_data="profile")],
        [InlineKeyboardButton("📊 Leaderboard", callback_data="leaderboard"),
         InlineKeyboardButton("🔔 Notifications", callback_data="notifications")],
        [InlineKeyboardButton("👥 Referrals", callback_data="referrals"),
         InlineKeyboardButton("⭐ Premium", callback_data="premium_menu")],
    ]
    if is_admin:
        kb.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

def back_btn(cb_data="main_menu"):
    return InlineKeyboardButton("◀️ Back", callback_data=cb_data)

# ============================================================
# MAIN MENU & START
# ============================================================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        u = ensure_user(user.id, user.username, user.first_name)
        if u['is_banned']:
            await update.message.reply_text(f"⛔ You are banned.\nReason: {u['ban_reason']}")
            return
        # Check referral
        if context.args and len(context.args) > 0:
            ref_code = context.args[0]
            if ref_code != u['referral_code']:
                conn = get_db()
                referrer = conn.execute("SELECT * FROM users WHERE referral_code=?", (ref_code,)).fetchone()
                if referrer and u['referred_by'] == 0:
                    conn.execute("UPDATE users SET referred_by=? WHERE user_id=?", (referrer['user_id'], user.id))
                    conn.execute("UPDATE users SET referral_count=referral_count+1 WHERE user_id=?", (referrer['user_id'],))
                    bonus = float(get_setting('referral_bonus_vc', '50'))
                    if bonus > 0 and get_setting('referral_enabled', '1') == '1':
                        add_balance(referrer['user_id'], bonus, 'referral_bonus',
                                    f'Referral bonus for inviting {user.first_name}')
                        conn.execute("UPDATE users SET referral_earnings=referral_earnings+? WHERE user_id=?",
                                      (bonus, referrer['user_id']))
                        conn.execute("INSERT INTO referral_rewards (referrer_id,referred_id,trigger_type,reward_amount) VALUES (?,?,?,?)",
                                      (referrer['user_id'], user.id, 'signup', bonus))
                        try:
                            await context.bot.send_message(referrer['user_id'],
                                f"🎉 <b>New Referral!</b>\n{user.first_name} joined using your link!\n+{bonus} 🪙", parse_mode=ParseMode.HTML)
                        except: pass
                    conn.commit()
                conn.close()

        welcome = get_setting('welcome_message', 'Welcome!')
        bot_name = get_setting('bot_name', '⚡ THE VAULT')
        sym = get_setting('currency_symbol', '🪙')
        text = (f"<b>{bot_name}</b>\n\n"
                f"{welcome}\n\n"
                f"💰 Balance: <b>{u['balance']:.1f}</b> {sym}\n"
                f"⭐ Reputation: <b>{u['reputation_score']:.1f}</b>/5.0\n")
        level, xp = calculate_level(u)
        text += f"📊 Level {level} — {get_level_title(level)} ({xp} XP)\n"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))
    except Exception as e:
        logger.error(f"start_cmd error: {e}")

async def main_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        user = query.from_user
        u = ensure_user(user.id, user.username, user.first_name)
        if u['is_banned']:
            await query.edit_message_text(f"⛔ Banned: {u['ban_reason']}")
            return
        bot_name = get_setting('bot_name', '⚡ THE VAULT')
        sym = get_setting('currency_symbol', '🪙')
        level, xp = calculate_level(u)
        text = (f"<b>{bot_name}</b>\n\n"
                f"💰 Balance: <b>{u['balance']:.1f}</b> {sym}\n"
                f"⭐ Reputation: <b>{u['reputation_score']:.1f}</b>/5.0\n"
                f"📊 Level {level} — {get_level_title(level)}\n")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))
    except BadRequest:
        pass
    except Exception as e:
        logger.error(f"main_menu error: {e}")

# ============================================================
# WALLET
# ============================================================

async def wallet_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        user = query.from_user
        u = ensure_user(user.id)
        sym = get_setting('currency_symbol', '🪙')
        text = (f"<b>💰 My Wallet</b>\n\n"
                f"Available: <b>{u['balance']:.1f}</b> {sym}\n"
                f"In Escrow: <b>{u['frozen_balance']:.1f}</b> {sym}\n"
                f"Total Earned: <b>{u['total_earned']:.1f}</b> {sym}\n"
                f"Total Spent: <b>{u['total_spent']:.1f}</b> {sym}\n"
                f"Total Withdrawn: <b>{u['total_withdrawn']:.1f}</b> {sym}\n")
        kb = [
            [InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
             InlineKeyboardButton("💸 Withdraw", callback_data="withdraw")],
            [InlineKeyboardButton("📜 Transaction History", callback_data="tx_history_0")],
            [back_btn()]
        ]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"wallet error: {e}")

async def tx_history_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[-1])
        user = query.from_user
        conn = get_db()
        txs = conn.execute("SELECT * FROM transactions WHERE user_id=? ORDER BY created_at DESC LIMIT 10 OFFSET ?",
                            (user.id, page*10)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM transactions WHERE user_id=?", (user.id,)).fetchone()['c']
        conn.close()
        if not txs:
            text = "<b>📜 Transaction History</b>\n\nNo transactions yet."
        else:
            text = "<b>📜 Transaction History</b>\n\n"
            for tx in txs:
                sign = "+" if tx['amount'] > 0 else ""
                text += f"{'🟢' if tx['amount']>0 else '🔴'} {sign}{tx['amount']:.1f} 🪙 — {tx['type']}\n"
                text += f"   <i>{tx['description'][:50]}</i>\n"
                text += f"   {tx['created_at'][:16]}\n\n"
        kb = []
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"tx_history_{page-1}"))
        if (page+1)*10 < total: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"tx_history_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("wallet")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"tx_history error: {e}")

# ============================================================
# DEPOSIT
# ============================================================

async def deposit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        upi = get_setting('upi_id', '')
        rate = get_setting('inr_to_vc_rate', '10')
        min_dep = get_setting('min_deposit_inr', '50')
        max_dep = get_setting('max_deposit_inr', '10000')
        instr = get_setting('payment_instructions', '')
        if not upi:
            await query.edit_message_text("⚠️ Deposits not configured yet. Contact admin.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            return
        text = (f"<b>💳 Deposit Vault Coins</b>\n\n"
                f"💱 Rate: ₹1 = {rate} 🪙\n"
                f"📌 Min: ₹{min_dep} | Max: ₹{max_dep}\n\n"
                f"<b>UPI ID:</b> <code>{upi}</code>\n\n"
                f"📋 {instr}\n\n"
                f"Send the amount in INR as a message (e.g. <code>100</code>)")
        user_states[query.from_user.id] = {'state': 'deposit_amount'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"deposit error: {e}")

async def withdraw_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        u = ensure_user(query.from_user.id)
        if get_setting('withdrawal_enabled', '0') != '1':
            await query.edit_message_text("⚠️ Withdrawals are currently disabled.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            return
        min_vc = get_setting('min_withdrawal_vc', '2000')
        max_vc = get_setting('max_withdrawal_vc', '100000')
        rate = get_setting('vc_to_inr_rate', '0.05')
        fee_pct = get_setting('withdrawal_fee_percent', '10')
        # Check requirements
        if get_setting('require_verified_for_withdrawal', '1') == '1' and not u['is_seller_verified']:
            await query.edit_message_text("⚠️ You must be verified to withdraw. Complete gigs to build reputation.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            return
        min_rep = float(get_setting('min_reputation_for_withdrawal', '2.0'))
        if u['reputation_score'] < min_rep:
            await query.edit_message_text(f"⚠️ Min reputation {min_rep} required. Current: {u['reputation_score']}",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            return
        min_gigs = int(get_setting('min_completed_gigs_for_withdrawal', '3'))
        if u['completed_gigs'] < min_gigs:
            await query.edit_message_text(f"⚠️ Complete at least {min_gigs} gigs first. Current: {u['completed_gigs']}",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            return
        text = (f"<b>💸 Withdraw</b>\n\n"
                f"Balance: {u['balance']:.1f} 🪙\n"
                f"Rate: 1 🪙 = ₹{rate}\n"
                f"Fee: {fee_pct}%\n"
                f"Min: {min_vc} 🪙 | Max: {max_vc} 🪙\n\n"
                f"Send amount of VC to withdraw (e.g. <code>2000</code>)")
        user_states[query.from_user.id] = {'state': 'withdraw_amount'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"withdraw error: {e}")


# ============================================================
# PROFILE
# ============================================================

async def profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        user = query.from_user
        u = ensure_user(user.id)
        sym = get_setting('currency_symbol', '🪙')
        level, xp = calculate_level(u)
        badges_list = json.loads(u.get('badges', '[]'))
        conn = get_db()
        badge_defs = {dict(b)['code']: dict(b) for b in conn.execute("SELECT * FROM badge_definitions").fetchall()}
        conn.close()
        badge_str = ' '.join(badge_defs[b]['emoji'] for b in badges_list if b in badge_defs) if badges_list else 'None yet'
        prem = "✅ Premium" if is_premium(u) else "Free"
        comp_rate = round(u['completed_gigs'] / max(u['completed_gigs'] + u['failed_gigs'], 1) * 100)
        text = (f"<b>👤 Profile — {user.first_name}</b>\n"
                f"@{user.username or 'N/A'}\n\n"
                f"📊 Level {level} — {get_level_title(level)} ({xp} XP)\n"
                f"⭐ Reputation: {u['reputation_score']:.1f}/5.0\n"
                f"💰 Balance: {u['balance']:.1f} {sym}\n"
                f"🏷️ Status: {prem}\n"
                f"✅ Gigs Completed: {u['completed_gigs']} ({comp_rate}% rate)\n"
                f"🏅 Badges: {badge_str}\n")
        if u['bio']:
            text += f"\n📝 Bio: {u['bio']}\n"
        kb = [
            [InlineKeyboardButton("✏️ Edit Bio", callback_data="edit_bio"),
             InlineKeyboardButton("🔧 Edit Skills", callback_data="edit_skills")],
            [InlineKeyboardButton("📋 My Gigs", callback_data="my_gigs_0"),
             InlineKeyboardButton("🛍️ My Products", callback_data="my_products_0")],
            [back_btn()]
        ]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"profile error: {e}")

async def edit_bio_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_states[query.from_user.id] = {'state': 'edit_bio'}
    await query.edit_message_text("✏️ Send your new bio (max 200 chars):",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("profile")]]))

async def edit_skills_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_states[query.from_user.id] = {'state': 'edit_skills'}
    await query.edit_message_text("🔧 Send your skills separated by commas\n(e.g. Python, Design, Marketing):",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("profile")]]))

# ============================================================
# REFERRALS
# ============================================================

async def referrals_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        u = ensure_user(query.from_user.id)
        bot_me = await context.bot.get_me()
        link = f"https://t.me/{bot_me.username}?start={u['referral_code']}"
        pct = get_setting('referral_percent_on_transactions', '5')
        bonus = get_setting('referral_bonus_vc', '50')
        text = (f"<b>👥 Referral Program</b>\n\n"
                f"🔗 Your link:\n<code>{link}</code>\n\n"
                f"👥 Referrals: <b>{u['referral_count']}</b>\n"
                f"💰 Earned: <b>{u['referral_earnings']:.1f}</b> 🪙\n\n"
                f"💡 Earn {bonus} 🪙 per signup + {pct}% of platform fees from their transactions!")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"referrals error: {e}")

# ============================================================
# PREMIUM
# ============================================================

async def premium_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        u = ensure_user(query.from_user.id)
        if get_setting('premium_enabled', '1') != '1':
            await query.edit_message_text("⚠️ Premium is currently disabled.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn()]]))
            return
        features = get_setting('premium_features', '').replace('|', '\n• ')
        mp = get_setting('premium_monthly_price', '500')
        qp = get_setting('premium_quarterly_price', '1200')
        yp = get_setting('premium_yearly_price', '4000')
        if is_premium(u):
            text = (f"<b>⭐ Premium Status: ACTIVE</b>\n"
                    f"Expires: {u['premium_expires'][:10]}\n\n"
                    f"<b>Your Benefits:</b>\n• {features}")
            kb = [[back_btn()]]
        else:
            text = (f"<b>⭐ Premium Membership</b>\n\n"
                    f"<b>Benefits:</b>\n• {features}\n\n"
                    f"<b>Plans:</b>\n"
                    f"📅 Monthly: {mp} 🪙\n"
                    f"📅 Quarterly: {qp} 🪙\n"
                    f"📅 Yearly: {yp} 🪙")
            kb = [
                [InlineKeyboardButton(f"Monthly ({mp} 🪙)", callback_data="buy_premium_monthly")],
                [InlineKeyboardButton(f"Quarterly ({qp} 🪙)", callback_data="buy_premium_quarterly")],
                [InlineKeyboardButton(f"Yearly ({yp} 🪙)", callback_data="buy_premium_yearly")],
                [back_btn()]
            ]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"premium error: {e}")

async def buy_premium_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        plan = query.data.replace("buy_premium_", "")
        u = ensure_user(query.from_user.id)
        prices = {'monthly': (float(get_setting('premium_monthly_price','500')), 30),
                  'quarterly': (float(get_setting('premium_quarterly_price','1200')), 90),
                  'yearly': (float(get_setting('premium_yearly_price','4000')), 365)}
        if plan not in prices:
            await query.edit_message_text("Invalid plan.", reply_markup=InlineKeyboardMarkup([[back_btn("premium_menu")]]))
            return
        price, days = prices[plan]
        if not deduct_balance(query.from_user.id, price, 'premium_purchase', f'Premium {plan} purchase'):
            await query.edit_message_text(f"❌ Insufficient balance. Need {price} 🪙.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("premium_menu")]]))
            return
        expires = (datetime.now() + timedelta(days=days)).isoformat()
        conn = get_db()
        conn.execute("UPDATE users SET is_premium=1, premium_expires=? WHERE user_id=?", (expires, query.from_user.id))
        conn.execute("INSERT INTO premium_purchases (user_id, plan, price, duration_days, expires_at) VALUES (?,?,?,?,?)",
                      (query.from_user.id, plan, price, days, expires))
        conn.commit(); conn.close()
        await query.edit_message_text(f"🎉 <b>Premium Activated!</b>\n\nPlan: {plan.title()}\nExpires: {expires[:10]}",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"buy_premium error: {e}")

# ============================================================
# GIG MARKETPLACE
# ============================================================

async def gigs_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        kb = [
            [InlineKeyboardButton("📋 Browse Gigs", callback_data="browse_gigs_all_0"),
             InlineKeyboardButton("➕ Post a Gig", callback_data="post_gig")],
            [InlineKeyboardButton("🔍 Search Gigs", callback_data="search_gigs"),
             InlineKeyboardButton("📂 By Category", callback_data="gig_categories")],
            [InlineKeyboardButton("📋 My Posted Gigs", callback_data="my_gigs_0"),
             InlineKeyboardButton("🔨 My Work", callback_data="my_work_0")],
            [back_btn()]
        ]
        await query.edit_message_text("<b>💼 Gig Marketplace</b>\n\nFind work or hire talent!",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def gig_categories_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        cats = get_categories()
        kb = []
        row = []
        for c in cats:
            row.append(InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"browse_gigs_{c['name']}_0"))
            if len(row) == 2:
                kb.append(row); row = []
        if row: kb.append(row)
        kb.append([back_btn("gigs_menu")])
        await query.edit_message_text("<b>📂 Gig Categories</b>", parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def browse_gigs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        parts = query.data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[2:-1])
        conn = get_db()
        if category == 'all':
            gigs = conn.execute("SELECT g.*, u.username, u.first_name, u.reputation_score FROM gigs g JOIN users u ON g.poster_id=u.user_id WHERE g.status='open' ORDER BY g.is_featured DESC, g.created_at DESC LIMIT 8 OFFSET ?", (page*8,)).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE status='open'").fetchone()['c']
        else:
            gigs = conn.execute("SELECT g.*, u.username, u.first_name, u.reputation_score FROM gigs g JOIN users u ON g.poster_id=u.user_id WHERE g.status='open' AND g.category=? ORDER BY g.is_featured DESC, g.created_at DESC LIMIT 8 OFFSET ?", (category, page*8)).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE status='open' AND category=?", (category,)).fetchone()['c']
        conn.close()
        if not gigs:
            text = "<b>💼 Gigs</b>\n\nNo open gigs found."
        else:
            text = f"<b>💼 Open Gigs</b> ({category if category!='all' else 'All'})\n\n"
            for g in gigs:
                feat = "⭐ " if g['is_featured'] else ""
                text += f"{feat}<b>{g['title']}</b>\n💰 {g['budget']:.0f} 🪙 | 📂 {g['category']} | ⏰ {g['deadline_hours']}h\n"
                text += f"👤 {g['first_name']} (⭐{g['reputation_score']:.1f})\n\n"
        kb = []
        for g in gigs:
            kb.append([InlineKeyboardButton(f"📋 {g['title'][:30]}", callback_data=f"view_gig_{g['id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"browse_gigs_{category}_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"browse_gigs_{category}_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("gigs_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"browse_gigs error: {e}")

async def view_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        g = conn.execute("SELECT g.*, u.username, u.first_name, u.reputation_score FROM gigs g JOIN users u ON g.poster_id=u.user_id WHERE g.id=?", (gig_id,)).fetchone()
        if not g:
            await query.edit_message_text("Gig not found.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            conn.close(); return
        apps = conn.execute("SELECT COUNT(*) as c FROM gig_applications WHERE gig_id=?", (gig_id,)).fetchone()['c']
        conn.execute("UPDATE gigs SET view_count=view_count+1 WHERE id=?", (gig_id,))
        conn.commit(); conn.close()
        feat = "⭐ FEATURED\n" if g['is_featured'] else ""
        text = (f"{feat}<b>{g['title']}</b>\n\n"
                f"📝 {g['description']}\n\n"
                f"💰 Budget: {g['budget']:.0f} 🪙\n"
                f"📂 Category: {g['category']}\n"
                f"⏰ Deadline: {g['deadline_hours']} hours\n"
                f"👤 Posted by: {g['first_name']} (⭐{g['reputation_score']:.1f})\n"
                f"📊 Applications: {apps}/{g['max_applicants']}\n"
                f"👁️ Views: {g['view_count']}\n"
                f"📅 Posted: {g['created_at'][:16]}\n"
                f"Status: {g['status'].upper()}")
        kb = []
        uid = query.from_user.id
        if g['status'] == 'open' and uid != g['poster_id']:
            kb.append([InlineKeyboardButton("📝 Apply", callback_data=f"apply_gig_{gig_id}")])
        if uid == g['poster_id'] and g['status'] == 'open':
            kb.append([InlineKeyboardButton("👥 View Applications", callback_data=f"gig_apps_{gig_id}")])
            kb.append([InlineKeyboardButton("❌ Cancel Gig", callback_data=f"cancel_gig_{gig_id}")])
        if uid == g['claimed_by'] and g['status'] == 'assigned':
            kb.append([InlineKeyboardButton("📦 Deliver Work", callback_data=f"deliver_gig_{gig_id}")])
        if uid == g['poster_id'] and g['status'] == 'delivered':
            kb.append([InlineKeyboardButton("✅ Approve", callback_data=f"approve_gig_{gig_id}"),
                        InlineKeyboardButton("🔄 Revision", callback_data=f"revision_gig_{gig_id}")])
            kb.append([InlineKeyboardButton("⚠️ Dispute", callback_data=f"dispute_gig_{gig_id}")])
        kb.append([back_btn("browse_gigs_all_0")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"view_gig error: {e}")

async def post_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        u = ensure_user(query.from_user.id)
        conn = get_db()
        active = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE poster_id=? AND status IN ('open','assigned','delivered','revision')",
                               (query.from_user.id,)).fetchone()['c']
        conn.close()
        max_gigs = int(get_setting('max_active_gigs_premium' if is_premium(u) else 'max_active_gigs_free', '3'))
        if active >= max_gigs:
            await query.edit_message_text(f"❌ Max {max_gigs} active gigs reached.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            return
        user_states[query.from_user.id] = {'state': 'gig_title'}
        await query.edit_message_text("📝 <b>Post a New Gig</b>\n\nSend the gig title:",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
    except BadRequest: pass

async def apply_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        existing = conn.execute("SELECT * FROM gig_applications WHERE gig_id=? AND applicant_id=?",
                                 (gig_id, query.from_user.id)).fetchone()
        if existing:
            await query.edit_message_text("You already applied!", reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))
            conn.close(); return
        g = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
        apps = conn.execute("SELECT COUNT(*) as c FROM gig_applications WHERE gig_id=?", (gig_id,)).fetchone()['c']
        conn.close()
        if apps >= g['max_applicants']:
            await query.edit_message_text("❌ Max applicants reached.", reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))
            return
        user_states[query.from_user.id] = {'state': 'gig_apply_text', 'gig_id': gig_id}
        await query.edit_message_text("📝 Send your proposal/pitch for this gig:",
                                       reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))
    except BadRequest: pass

async def gig_apps_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        apps = conn.execute("SELECT ga.*, u.first_name, u.username, u.reputation_score, u.completed_gigs FROM gig_applications ga JOIN users u ON ga.applicant_id=u.user_id WHERE ga.gig_id=? AND ga.status='pending'", (gig_id,)).fetchall()
        conn.close()
        if not apps:
            text = "No applications yet."
        else:
            text = f"<b>👥 Applications for Gig #{gig_id}</b>\n\n"
            for a in apps:
                text += (f"👤 {a['first_name']} (@{a['username'] or 'N/A'})\n"
                         f"⭐ Rep: {a['reputation_score']:.1f} | ✅ {a['completed_gigs']} gigs\n"
                         f"📝 {a['proposal_text'][:100]}\n\n")
        kb = []
        for a in apps:
            kb.append([InlineKeyboardButton(f"✅ Accept {a['first_name']}", callback_data=f"accept_app_{a['id']}")])
        kb.append([back_btn(f"view_gig_{gig_id}")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"gig_apps error: {e}")

async def accept_app_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        app_id = int(query.data.split("_")[-1])
        conn = get_db()
        app = conn.execute("SELECT * FROM gig_applications WHERE id=?", (app_id,)).fetchone()
        if not app:
            conn.close(); return
        gig = conn.execute("SELECT * FROM gigs WHERE id=?", (app['gig_id'],)).fetchone()
        if gig['status'] != 'open':
            await query.edit_message_text("Gig is no longer open.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            conn.close(); return
        # Freeze budget from poster
        if not freeze_balance(gig['poster_id'], gig['budget']):
            await query.edit_message_text("❌ Insufficient balance for escrow.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            conn.close(); return
        # Create escrow
        fee_pct = get_fee_percent(ensure_user(gig['poster_id']))
        conn.execute("INSERT INTO escrow (gig_id, payer_id, payee_id, amount, platform_fee) VALUES (?,?,?,?,?)",
                      (gig['id'], gig['poster_id'], app['applicant_id'], gig['budget'], fee_pct))
        conn.execute("UPDATE gigs SET status='assigned', claimed_by=?, claimed_at=CURRENT_TIMESTAMP WHERE id=?",
                      (app['applicant_id'], gig['id']))
        conn.execute("UPDATE gig_applications SET status='accepted' WHERE id=?", (app_id,))
        conn.execute("UPDATE gig_applications SET status='rejected' WHERE gig_id=? AND id!=?", (gig['id'], app_id))
        conn.commit(); conn.close()
        await notify_user(context.bot, app['applicant_id'],
                          f"🎉 Your application for <b>{gig['title']}</b> was accepted! Deliver within {gig['deadline_hours']}h.")
        await query.edit_message_text(f"✅ Assigned to applicant! Escrow of {gig['budget']} 🪙 created.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"accept_app error: {e}")

async def deliver_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    gig_id = int(query.data.split("_")[-1])
    user_states[query.from_user.id] = {'state': 'deliver_gig', 'gig_id': gig_id}
    await query.edit_message_text("📦 Send your delivery message (text or file):",
                                   reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))

async def approve_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
        if not gig or gig['status'] != 'delivered':
            conn.close(); return
        escrow = conn.execute("SELECT * FROM escrow WHERE gig_id=? AND status='held'", (gig_id,)).fetchone()
        if not escrow:
            conn.close(); return
        # Release escrow
        fee_pct = escrow['platform_fee']
        success, net, fee = transfer_frozen_to_user(gig['poster_id'], gig['claimed_by'], escrow['amount'], fee_pct)
        if success:
            conn2 = get_db()
            conn2.execute("UPDATE gigs SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", (gig_id,))
            conn2.execute("UPDATE escrow SET status='released', released_at=CURRENT_TIMESTAMP WHERE id=?", (escrow['id'],))
            conn2.execute("UPDATE users SET completed_gigs=completed_gigs+1 WHERE user_id=?", (gig['claimed_by'],))
            conn2.commit(); conn2.close()
            update_reputation(gig['claimed_by'])
            await check_badges(gig['claimed_by'], context.bot)
            # Referral commission
            ref_user = ensure_user(gig['claimed_by'])
            if ref_user['referred_by'] and get_setting('referral_enabled','1')=='1':
                ref_pct = float(get_setting('referral_percent_on_transactions','5'))
                ref_bonus = round(fee * ref_pct / 100, 2)
                if ref_bonus > 0:
                    add_balance(ref_user['referred_by'], ref_bonus, 'referral_bonus', f'Referral commission from gig #{gig_id}')
            await notify_user(context.bot, gig['claimed_by'],
                              f"💰 Gig <b>{gig['title']}</b> approved! You received {net:.1f} 🪙")
            # Ask both to rate
            user_states[query.from_user.id] = {'state': 'rate_worker', 'gig_id': gig_id}
            await query.edit_message_text(f"✅ Gig completed! Worker received {net:.1f} 🪙 (fee: {fee:.1f})\n\nRate the worker (1-5):",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
        else:
            await query.edit_message_text("❌ Error releasing escrow.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
        conn.close()
    except BadRequest: pass
    except Exception as e: logger.error(f"approve_gig error: {e}")

async def revision_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
        if gig and gig['status'] == 'delivered':
            conn.execute("UPDATE gigs SET status='revision' WHERE id=?", (gig_id,))
            conn.commit()
            await notify_user(context.bot, gig['claimed_by'],
                              f"🔄 Revision requested for <b>{gig['title']}</b>. Please re-deliver.")
        conn.close()
        await query.edit_message_text("🔄 Revision requested.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
    except BadRequest: pass

async def dispute_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    gig_id = int(query.data.split("_")[-1])
    user_states[query.from_user.id] = {'state': 'dispute_reason', 'gig_id': gig_id}
    await query.edit_message_text("⚠️ Describe the reason for your dispute:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))

async def cancel_gig_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        gig_id = int(query.data.split("_")[-1])
        conn = get_db()
        gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
        if gig and gig['status'] == 'open' and gig['poster_id'] == query.from_user.id:
            conn.execute("UPDATE gigs SET status='cancelled' WHERE id=?", (gig_id,))
            conn.commit()
        conn.close()
        await query.edit_message_text("❌ Gig cancelled.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
    except BadRequest: pass

async def my_gigs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        gigs = conn.execute("SELECT * FROM gigs WHERE poster_id=? ORDER BY created_at DESC LIMIT 8 OFFSET ?",
                             (query.from_user.id, page*8)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE poster_id=?", (query.from_user.id,)).fetchone()['c']
        conn.close()
        text = "<b>📋 My Posted Gigs</b>\n\n"
        if not gigs:
            text += "No gigs posted yet."
        for g in gigs:
            text += f"• <b>{g['title']}</b> — {g['status'].upper()} ({g['budget']:.0f} 🪙)\n"
        kb = []
        for g in gigs:
            kb.append([InlineKeyboardButton(f"{g['title'][:30]}", callback_data=f"view_gig_{g['id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"my_gigs_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"my_gigs_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("gigs_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def my_work_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        gigs = conn.execute("SELECT * FROM gigs WHERE claimed_by=? ORDER BY claimed_at DESC LIMIT 8 OFFSET ?",
                             (query.from_user.id, page*8)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE claimed_by=?", (query.from_user.id,)).fetchone()['c']
        conn.close()
        text = "<b>🔨 My Work</b>\n\n"
        if not gigs:
            text += "No work yet."
        for g in gigs:
            text += f"• <b>{g['title']}</b> — {g['status'].upper()} ({g['budget']:.0f} 🪙)\n"
        kb = []
        for g in gigs:
            kb.append([InlineKeyboardButton(f"{g['title'][:30]}", callback_data=f"view_gig_{g['id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"my_work_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"my_work_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("gigs_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass


# ============================================================
# DIGITAL STORE
# ============================================================

async def store_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        kb = [
            [InlineKeyboardButton("🛍️ Browse Products", callback_data="browse_products_all_0"),
             InlineKeyboardButton("➕ Sell Product", callback_data="sell_product")],
            [InlineKeyboardButton("🔍 Search", callback_data="search_products"),
             InlineKeyboardButton("📂 Categories", callback_data="product_categories")],
            [InlineKeyboardButton("📦 My Purchases", callback_data="my_purchases_0"),
             InlineKeyboardButton("🏪 My Store", callback_data="my_products_0")],
            [back_btn()]
        ]
        await query.edit_message_text("<b>🛍️ Digital Store</b>\n\nBuy and sell digital products!",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def product_categories_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        cats = get_categories()
        kb = [[InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"browse_products_{c['name']}_0")]
              for i in range(0, len(cats), 2) for c in cats[i:i+1]]
        # rebuild properly
        kb = []
        row = []
        for c in cats:
            row.append(InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"browse_products_{c['name']}_0"))
            if len(row) == 2: kb.append(row); row = []
        if row: kb.append(row)
        kb.append([back_btn("store_menu")])
        await query.edit_message_text("<b>📂 Product Categories</b>", parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def browse_products_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        parts = query.data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[2:-1])
        conn = get_db()
        if category == 'all':
            products = conn.execute("SELECT p.*, u.first_name, u.reputation_score FROM products p JOIN users u ON p.seller_id=u.user_id WHERE p.is_active=1 AND p.is_approved=1 ORDER BY p.created_at DESC LIMIT 8 OFFSET ?", (page*8,)).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM products WHERE is_active=1 AND is_approved=1").fetchone()['c']
        else:
            products = conn.execute("SELECT p.*, u.first_name, u.reputation_score FROM products p JOIN users u ON p.seller_id=u.user_id WHERE p.is_active=1 AND p.is_approved=1 AND p.category=? ORDER BY p.created_at DESC LIMIT 8 OFFSET ?", (category, page*8)).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM products WHERE is_active=1 AND is_approved=1 AND category=?", (category,)).fetchone()['c']
        conn.close()
        text = f"<b>🛍️ Products</b> ({category if category!='all' else 'All'})\n\n"
        if not products:
            text += "No products found."
        for p in products:
            stars = f"⭐{p['avg_rating']:.1f}" if p['total_ratings'] > 0 else "No ratings"
            text += f"• <b>{p['title']}</b> — {p['price']:.0f} 🪙\n  {stars} | {p['total_sales']} sold | by {p['first_name']}\n\n"
        kb = [[InlineKeyboardButton(f"🛍️ {p['title'][:30]}", callback_data=f"view_product_{p['id']}")] for p in products]
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"browse_products_{category}_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"browse_products_{category}_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("store_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"browse_products error: {e}")

async def view_product_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        prod_id = int(query.data.split("_")[-1])
        conn = get_db()
        p = conn.execute("SELECT p.*, u.first_name, u.username, u.reputation_score FROM products p JOIN users u ON p.seller_id=u.user_id WHERE p.id=?", (prod_id,)).fetchone()
        conn.close()
        if not p:
            await query.edit_message_text("Product not found.", reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
            return
        stars = f"⭐ {p['avg_rating']:.1f} ({p['total_ratings']} ratings)" if p['total_ratings'] > 0 else "No ratings yet"
        text = (f"<b>{p['title']}</b>\n\n"
                f"📝 {p['description']}\n\n"
                f"💰 Price: {p['price']:.0f} 🪙\n"
                f"📂 {p['category']}\n"
                f"{stars}\n"
                f"📊 {p['total_sales']} sold\n"
                f"👤 {p['first_name']} (⭐{p['reputation_score']:.1f})\n")
        if p['preview_text']:
            text += f"\n📋 Preview: {p['preview_text']}\n"
        kb = []
        if query.from_user.id != p['seller_id']:
            kb.append([InlineKeyboardButton(f"🛒 Buy ({p['price']:.0f} 🪙)", callback_data=f"buy_product_{prod_id}")])
        if query.from_user.id == p['seller_id']:
            kb.append([InlineKeyboardButton("✏️ Edit", callback_data=f"edit_product_{prod_id}"),
                        InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_product_{prod_id}")])
        kb.append([back_btn("browse_products_all_0")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def buy_product_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        prod_id = int(query.data.split("_")[-1])
        buyer_id = query.from_user.id
        conn = get_db()
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
        if not p or not p['is_active']:
            await query.edit_message_text("Product unavailable.", reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
            conn.close(); return
        buyer = ensure_user(buyer_id)
        seller = ensure_user(p['seller_id'])
        fee_pct = get_fee_percent(seller)
        fee = round(p['price'] * fee_pct / 100, 2)
        net = round(p['price'] - fee, 2)
        if not deduct_balance(buyer_id, p['price'], 'product_purchase', f"Bought: {p['title']}"):
            await query.edit_message_text(f"❌ Insufficient balance. Need {p['price']:.0f} 🪙",
                                           reply_markup=InlineKeyboardMarkup([[back_btn(f"view_product_{prod_id}")]]))
            conn.close(); return
        add_balance(p['seller_id'], net, 'product_sale', f"Sold: {p['title']} (fee: {fee})")
        # Track platform fees
        total_fees = float(get_setting('platform_total_fees', '0')) + fee
        set_setting('platform_total_fees', str(total_fees))
        conn.execute("INSERT INTO product_purchases (product_id, buyer_id, seller_id, price, platform_fee) VALUES (?,?,?,?,?)",
                      (prod_id, buyer_id, p['seller_id'], p['price'], fee))
        conn.execute("UPDATE products SET total_sales=total_sales+1, total_revenue=total_revenue+? WHERE id=?", (p['price'], prod_id))
        conn.commit(); conn.close()
        # Deliver file
        try:
            await context.bot.send_document(buyer_id, p['file_id'], caption=f"📦 <b>{p['title']}</b>\n\nThank you for your purchase!",
                                             parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(buyer_id, f"📦 Your purchase: {p['title']}\nFile ID: {p['file_id']}")
        # Referral
        if buyer['referred_by'] and get_setting('referral_enabled','1')=='1':
            ref_pct = float(get_setting('referral_percent_on_transactions','5'))
            ref_bonus = round(fee * ref_pct / 100, 2)
            if ref_bonus > 0:
                add_balance(buyer['referred_by'], ref_bonus, 'referral_bonus', f'Referral commission from product sale')
        await notify_user(context.bot, p['seller_id'], f"💰 Someone bought <b>{p['title']}</b>! +{net:.1f} 🪙")
        user_states[buyer_id] = {'state': 'rate_product', 'product_id': prod_id}
        await query.edit_message_text(f"✅ <b>Purchase Complete!</b>\n\nFile delivered! Rate this product (1-5):",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
        await check_badges(buyer_id, context.bot)
        await check_badges(p['seller_id'], context.bot)
    except BadRequest: pass
    except Exception as e: logger.error(f"buy_product error: {e}")

async def sell_product_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        u = ensure_user(query.from_user.id)
        conn = get_db()
        active = conn.execute("SELECT COUNT(*) as c FROM products WHERE seller_id=? AND is_active=1",
                               (query.from_user.id,)).fetchone()['c']
        conn.close()
        max_prods = int(get_setting('max_active_products_premium' if is_premium(u) else 'max_active_products_free', '5'))
        if active >= max_prods:
            await query.edit_message_text(f"❌ Max {max_prods} active products.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
            return
        user_states[query.from_user.id] = {'state': 'product_file'}
        await query.edit_message_text("📦 <b>Sell a Digital Product</b>\n\nFirst, send the file you want to sell:",
                                       parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
    except BadRequest: pass

async def my_products_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        products = conn.execute("SELECT * FROM products WHERE seller_id=? ORDER BY created_at DESC LIMIT 8 OFFSET ?",
                                 (query.from_user.id, page*8)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM products WHERE seller_id=?", (query.from_user.id,)).fetchone()['c']
        conn.close()
        text = "<b>🏪 My Products</b>\n\n"
        if not products: text += "No products yet."
        for p in products:
            status = "✅" if p['is_active'] else "❌"
            text += f"{status} <b>{p['title']}</b> — {p['price']:.0f} 🪙 ({p['total_sales']} sold)\n"
        kb = [[InlineKeyboardButton(p['title'][:30], callback_data=f"view_product_{p['id']}")] for p in products]
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"my_products_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"my_products_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("store_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def my_purchases_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        purchases = conn.execute("SELECT pp.*, p.title FROM product_purchases pp JOIN products p ON pp.product_id=p.id WHERE pp.buyer_id=? ORDER BY pp.created_at DESC LIMIT 8 OFFSET ?",
                                  (query.from_user.id, page*8)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM product_purchases WHERE buyer_id=?", (query.from_user.id,)).fetchone()['c']
        conn.close()
        text = "<b>📦 My Purchases</b>\n\n"
        if not purchases: text += "No purchases yet."
        for pp in purchases:
            text += f"• <b>{pp['title']}</b> — {pp['price']:.0f} 🪙 ({pp['created_at'][:10]})\n"
        kb = []
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"my_purchases_{page-1}"))
        if (page+1)*8 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"my_purchases_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("store_menu")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def delete_product_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        prod_id = int(query.data.split("_")[-1])
        conn = get_db()
        conn.execute("UPDATE products SET is_active=0 WHERE id=? AND seller_id=?", (prod_id, query.from_user.id))
        conn.commit(); conn.close()
        await query.edit_message_text("🗑️ Product deleted.", reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
    except BadRequest: pass

# ============================================================
# LEADERBOARD
# ============================================================

async def leaderboard_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        conn = get_db()
        top_rep = conn.execute("SELECT first_name, reputation_score, completed_gigs FROM users WHERE is_banned=0 ORDER BY reputation_score DESC LIMIT 10").fetchall()
        top_earn = conn.execute("SELECT first_name, total_earned FROM users WHERE is_banned=0 ORDER BY total_earned DESC LIMIT 10").fetchall()
        conn.close()
        text = "<b>📊 Leaderboard</b>\n\n<b>⭐ Top Reputation:</b>\n"
        for i, u in enumerate(top_rep):
            text += f"{i+1}. {u['first_name']} — ⭐{u['reputation_score']:.1f} ({u['completed_gigs']} gigs)\n"
        text += "\n<b>💰 Top Earners:</b>\n"
        for i, u in enumerate(top_earn):
            text += f"{i+1}. {u['first_name']} — {u['total_earned']:.0f} 🪙\n"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    except BadRequest: pass

# ============================================================
# NOTIFICATIONS
# ============================================================

async def notifications_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        conn = get_db()
        notifs = conn.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
                               (query.from_user.id,)).fetchall()
        unread = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE user_id=? AND is_read=0",
                               (query.from_user.id,)).fetchone()['c']
        conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (query.from_user.id,))
        conn.commit(); conn.close()
        text = f"<b>🔔 Notifications</b> ({unread} unread)\n\n"
        if not notifs: text += "No notifications."
        for n in notifs:
            icon = "🆕" if not n['is_read'] else "📌"
            text += f"{icon} {n['message'][:100]}\n{n['created_at'][:16]}\n\n"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    except BadRequest: pass

# ============================================================
# SEARCH
# ============================================================

async def search_gigs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_states[query.from_user.id] = {'state': 'search_gigs'}
    await query.edit_message_text("🔍 Send a keyword to search gigs:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))

async def search_products_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_states[query.from_user.id] = {'state': 'search_products'}
    await query.edit_message_text("🔍 Send a keyword to search products:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))


# ============================================================
# ADMIN PANEL
# ============================================================

async def admin_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID:
        return
    try:
        conn = get_db()
        total_users = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        total_gigs = conn.execute("SELECT COUNT(*) as c FROM gigs").fetchone()['c']
        open_gigs = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE status='open'").fetchone()['c']
        pending_deps = conn.execute("SELECT COUNT(*) as c FROM deposits WHERE status='pending'").fetchone()['c']
        pending_wds = conn.execute("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'").fetchone()['c']
        open_disputes = conn.execute("SELECT COUNT(*) as c FROM disputes WHERE status='open'").fetchone()['c']
        total_fees = get_setting('platform_total_fees', '0')
        total_products = conn.execute("SELECT COUNT(*) as c FROM products WHERE is_active=1").fetchone()['c']
        conn.close()
        text = (f"<b>🔧 Admin Panel</b>\n\n"
                f"👥 Users: {total_users}\n"
                f"💼 Gigs: {total_gigs} ({open_gigs} open)\n"
                f"🛍️ Products: {total_products}\n"
                f"💰 Platform Fees: {total_fees} 🪙\n"
                f"📥 Pending Deposits: {pending_deps}\n"
                f"📤 Pending Withdrawals: {pending_wds}\n"
                f"⚠️ Open Disputes: {open_disputes}\n")
        kb = [
            [InlineKeyboardButton("📥 Deposits", callback_data="admin_deposits_0"),
             InlineKeyboardButton("📤 Withdrawals", callback_data="admin_withdrawals_0")],
            [InlineKeyboardButton("⚠️ Disputes", callback_data="admin_disputes_0"),
             InlineKeyboardButton("🚨 Risk Alerts", callback_data="admin_risks")],
            [InlineKeyboardButton("👥 Users", callback_data="admin_users_0"),
             InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
             InlineKeyboardButton("💰 Add/Deduct VC", callback_data="admin_balance")],
            [InlineKeyboardButton("🔍 Check User", callback_data="admin_check_user"),
             InlineKeyboardButton("📊 Analytics", callback_data="admin_analytics")],
            [back_btn()]
        ]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"admin error: {e}")

async def admin_deposits_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        deps = conn.execute("SELECT d.*, u.first_name, u.username FROM deposits d JOIN users u ON d.user_id=u.user_id WHERE d.status='pending' ORDER BY d.created_at DESC LIMIT 5 OFFSET ?", (page*5,)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM deposits WHERE status='pending'").fetchone()['c']
        conn.close()
        text = f"<b>📥 Pending Deposits ({total})</b>\n\n"
        if not deps: text += "No pending deposits."
        for d in deps:
            text += (f"#{d['id']} | {d['first_name']} (@{d['username']})\n"
                     f"₹{d['amount_inr']} → {d['vault_coins']} 🪙 | {d['payment_method']}\n"
                     f"📅 {d['created_at'][:16]}\n\n")
        kb = []
        for d in deps:
            kb.append([InlineKeyboardButton(f"✅ Approve #{d['id']}", callback_data=f"approve_deposit_{d['id']}"),
                        InlineKeyboardButton(f"❌ Reject #{d['id']}", callback_data=f"reject_deposit_{d['id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_deposits_{page-1}"))
        if (page+1)*5 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_deposits_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("admin_panel")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def approve_deposit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        dep_id = int(query.data.split("_")[-1])
        conn = get_db()
        d = conn.execute("SELECT * FROM deposits WHERE id=? AND status='pending'", (dep_id,)).fetchone()
        if not d:
            await query.edit_message_text("Deposit not found or already processed.",
                                           reply_markup=InlineKeyboardMarkup([[back_btn("admin_deposits_0")]]))
            conn.close(); return
        conn.execute("UPDATE deposits SET status='approved' WHERE id=?", (dep_id,))
        conn.commit(); conn.close()
        add_balance(d['user_id'], d['vault_coins'], 'deposit', f"Deposit ₹{d['amount_inr']} approved", 'deposit', dep_id)
        await notify_user(context.bot, d['user_id'],
                          f"✅ Deposit of ₹{d['amount_inr']} approved! +{d['vault_coins']} 🪙")
        await query.edit_message_text(f"✅ Deposit #{dep_id} approved. {d['vault_coins']} 🪙 credited.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_deposits_0")]]))
    except BadRequest: pass

async def reject_deposit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        dep_id = int(query.data.split("_")[-1])
        conn = get_db()
        d = conn.execute("SELECT * FROM deposits WHERE id=? AND status='pending'", (dep_id,)).fetchone()
        if d:
            conn.execute("UPDATE deposits SET status='rejected' WHERE id=?", (dep_id,))
            conn.commit()
            await notify_user(context.bot, d['user_id'], f"❌ Deposit of ₹{d['amount_inr']} was rejected.")
        conn.close()
        await query.edit_message_text(f"❌ Deposit #{dep_id} rejected.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_deposits_0")]]))
    except BadRequest: pass

async def admin_withdrawals_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        wds = conn.execute("SELECT w.*, u.first_name, u.username FROM withdrawals w JOIN users u ON w.user_id=u.user_id WHERE w.status='pending' ORDER BY w.created_at DESC LIMIT 5 OFFSET ?", (page*5,)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'").fetchone()['c']
        conn.close()
        text = f"<b>📤 Pending Withdrawals ({total})</b>\n\n"
        if not wds: text += "No pending withdrawals."
        for w in wds:
            text += (f"#{w['id']} | {w['first_name']} (@{w['username']})\n"
                     f"{w['vault_coins']} 🪙 → ₹{w['amount_inr']} | {w['payout_method']}\n"
                     f"Details: {w['payout_details']}\n📅 {w['created_at'][:16]}\n\n")
        kb = []
        for w in wds:
            kb.append([InlineKeyboardButton(f"✅ Approve #{w['id']}", callback_data=f"approve_withdrawal_{w['id']}"),
                        InlineKeyboardButton(f"❌ Reject #{w['id']}", callback_data=f"reject_withdrawal_{w['id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_withdrawals_{page-1}"))
        if (page+1)*5 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_withdrawals_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("admin_panel")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def approve_withdrawal_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        wd_id = int(query.data.split("_")[-1])
        conn = get_db()
        w = conn.execute("SELECT * FROM withdrawals WHERE id=? AND status='pending'", (wd_id,)).fetchone()
        if w:
            conn.execute("UPDATE withdrawals SET status='approved', processed_at=CURRENT_TIMESTAMP WHERE id=?", (wd_id,))
            conn.commit()
            await notify_user(context.bot, w['user_id'],
                              f"✅ Withdrawal of {w['vault_coins']} 🪙 (₹{w['amount_inr']}) approved!")
        conn.close()
        await query.edit_message_text(f"✅ Withdrawal #{wd_id} approved.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_withdrawals_0")]]))
    except BadRequest: pass

async def reject_withdrawal_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        wd_id = int(query.data.split("_")[-1])
        conn = get_db()
        w = conn.execute("SELECT * FROM withdrawals WHERE id=? AND status='pending'", (wd_id,)).fetchone()
        if w:
            # Refund balance
            add_balance(w['user_id'], w['vault_coins'], 'refund', f'Withdrawal #{wd_id} rejected - refunded')
            conn.execute("UPDATE withdrawals SET status='rejected' WHERE id=?", (wd_id,))
            conn.commit()
            await notify_user(context.bot, w['user_id'],
                              f"❌ Withdrawal rejected. {w['vault_coins']} 🪙 refunded.")
        conn.close()
        await query.edit_message_text(f"❌ Withdrawal #{wd_id} rejected & refunded.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_withdrawals_0")]]))
    except BadRequest: pass

async def admin_disputes_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        disputes = conn.execute("SELECT d.*, u1.first_name as filer_name, u2.first_name as against_name FROM disputes d JOIN users u1 ON d.filed_by=u1.user_id JOIN users u2 ON d.filed_against=u2.user_id WHERE d.status='open' ORDER BY d.created_at DESC LIMIT 5 OFFSET ?", (page*5,)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM disputes WHERE status='open'").fetchone()['c']
        conn.close()
        text = f"<b>⚠️ Open Disputes ({total})</b>\n\n"
        if not disputes: text += "No open disputes."
        for d in disputes:
            ref = f"Gig #{d['gig_id']}" if d['gig_id'] else f"Purchase #{d['product_purchase_id']}"
            text += (f"#{d['id']} | {ref}\n"
                     f"Filed by: {d['filer_name']} vs {d['against_name']}\n"
                     f"Reason: {d['reason'][:80]}\n📅 {d['created_at'][:16]}\n\n")
        kb = []
        for d in disputes:
            kb.append([
                InlineKeyboardButton(f"👤 For filer #{d['id']}", callback_data=f"resolve_dispute_filer_{d['id']}"),
                InlineKeyboardButton(f"👤 For accused #{d['id']}", callback_data=f"resolve_dispute_accused_{d['id']}")
            ])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_disputes_{page-1}"))
        if (page+1)*5 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_disputes_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("admin_panel")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def resolve_dispute_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        parts = query.data.split("_")
        side = parts[2]  # 'filer' or 'accused'
        disp_id = int(parts[-1])
        conn = get_db()
        d = conn.execute("SELECT * FROM disputes WHERE id=? AND status='open'", (disp_id,)).fetchone()
        if not d:
            conn.close(); return
        winner = d['filed_by'] if side == 'filer' else d['filed_against']
        loser = d['filed_against'] if side == 'filer' else d['filed_by']
        # Handle gig dispute
        if d['gig_id']:
            gig = conn.execute("SELECT * FROM gigs WHERE id=?", (d['gig_id'],)).fetchone()
            escrow = conn.execute("SELECT * FROM escrow WHERE gig_id=? AND status='held'", (d['gig_id'],)).fetchone()
            if escrow:
                if winner == gig['poster_id']:
                    # Refund poster
                    unfreeze_balance(gig['poster_id'], escrow['amount'])
                    conn.execute("UPDATE gigs SET status='refunded' WHERE id=?", (d['gig_id'],))
                    conn.execute("UPDATE escrow SET status='refunded' WHERE id=?", (escrow['id'],))
                else:
                    # Pay worker
                    fee_pct = escrow['platform_fee']
                    transfer_frozen_to_user(gig['poster_id'], gig['claimed_by'], escrow['amount'], fee_pct)
                    conn.execute("UPDATE gigs SET status='completed' WHERE id=?", (d['gig_id'],))
                    conn.execute("UPDATE escrow SET status='released' WHERE id=?", (escrow['id'],))
        resolution = f"Resolved in favor of {'filer' if side=='filer' else 'accused'}"
        conn.execute("UPDATE disputes SET status='resolved', resolution=?, resolved_by='admin', resolved_at=CURRENT_TIMESTAMP WHERE id=?",
                      (resolution, disp_id))
        conn.execute("UPDATE users SET total_disputes_lost=total_disputes_lost+1 WHERE user_id=?", (loser,))
        conn.commit(); conn.close()
        update_reputation(loser)
        await notify_user(context.bot, winner, f"✅ Dispute #{disp_id} resolved in your favor!")
        await notify_user(context.bot, loser, f"❌ Dispute #{disp_id} resolved against you.")
        await query.edit_message_text(f"✅ Dispute #{disp_id} resolved for {side}.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_disputes_0")]]))
    except BadRequest: pass
    except Exception as e: logger.error(f"resolve_dispute error: {e}")

async def admin_risks_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        flagged = get_flagged_users()
        text = "<b>🚨 Risk Alerts</b>\n\n"
        if not flagged: text += "No flagged users."
        for u in flagged[:10]:
            text += (f"👤 {u['first_name']} (ID: {u['user_id']})\n"
                     f"   Risk: {u['risk_score']:.0f} | VPN: {'⚠️' if u['is_vpn_detected'] else '✅'}\n"
                     f"   Disputes: {u['total_disputes_lost']}/{u['total_disputes_filed']}\n\n")
        kb = [[back_btn("admin_panel")]]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def admin_users_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        page = int(query.data.split("_")[-1])
        conn = get_db()
        users = conn.execute("SELECT * FROM users ORDER BY last_active DESC LIMIT 10 OFFSET ?", (page*10,)).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        conn.close()
        text = f"<b>👥 Users ({total})</b>\n\n"
        for u in users:
            ban = "🚫" if u['is_banned'] else ""
            prem = "⭐" if u['is_premium'] else ""
            text += f"{ban}{prem} {u['first_name']} (@{u['username'] or 'N/A'}) — {u['balance']:.0f} 🪙\n"
        kb = []
        for u in users:
            kb.append([InlineKeyboardButton(f"{u['first_name']} ({u['user_id']})", callback_data=f"admin_user_{u['user_id']}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_users_{page-1}"))
        if (page+1)*10 < total: nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_users_{page+1}"))
        if nav: kb.append(nav)
        kb.append([back_btn("admin_panel")])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def admin_user_detail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        uid = int(query.data.split("_")[-1])
        conn = get_db()
        u = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone())
        conn.close()
        level, xp = calculate_level(u)
        risk = calculate_risk_score(uid)
        circular = detect_circular_transactions(uid)
        text = (f"<b>👤 User Detail</b>\n\n"
                f"ID: <code>{uid}</code>\n"
                f"Name: {u['first_name']} (@{u['username']})\n"
                f"Balance: {u['balance']:.1f} 🪙 (Frozen: {u['frozen_balance']:.1f})\n"
                f"Earned: {u['total_earned']:.1f} | Spent: {u['total_spent']:.1f}\n"
                f"Level: {level} ({xp} XP)\n"
                f"Rep: {u['reputation_score']:.1f} | Gigs: {u['completed_gigs']}\n"
                f"Premium: {'Yes' if u['is_premium'] else 'No'}\n"
                f"Banned: {'Yes' if u['is_banned'] else 'No'}\n"
                f"🚨 Risk Score: {risk}\n"
                f"VPN: {'Detected' if u['is_vpn_detected'] else 'Clean'}\n"
                f"IP: {u['last_ip'] or 'Unknown'}\n"
                f"Joined: {u['joined_at'][:10]}\n")
        if circular:
            text += f"⚠️ Circular transactions with: {circular}\n"
        kb = [
            [InlineKeyboardButton("🚫 Ban" if not u['is_banned'] else "✅ Unban", callback_data=f"admin_ban_{uid}"),
             InlineKeyboardButton("✅ Verify", callback_data=f"admin_verify_{uid}")],
            [InlineKeyboardButton("💰 Add Balance", callback_data=f"admin_add_bal_{uid}"),
             InlineKeyboardButton("💸 Deduct", callback_data=f"admin_deduct_bal_{uid}")],
            [back_btn("admin_users_0")]
        ]
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass
    except Exception as e: logger.error(f"admin_user error: {e}")

async def admin_ban_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        uid = int(query.data.split("_")[-1])
        conn = get_db()
        u = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
        if u['is_banned']:
            conn.execute("UPDATE users SET is_banned=0, ban_reason='' WHERE user_id=?", (uid,))
        else:
            conn.execute("UPDATE users SET is_banned=1, ban_reason='Banned by admin' WHERE user_id=?", (uid,))
        conn.commit(); conn.close()
        status = "unbanned" if u['is_banned'] else "banned"
        await query.edit_message_text(f"User {uid} {status}.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn(f"admin_user_{uid}")]]))
    except BadRequest: pass

async def admin_verify_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        uid = int(query.data.split("_")[-1])
        conn = get_db()
        conn.execute("UPDATE users SET is_seller_verified=1 WHERE user_id=?", (uid,))
        conn.commit(); conn.close()
        await notify_user(context.bot, uid, "✅ Your account has been verified by admin!")
        await query.edit_message_text(f"User {uid} verified.",
                                       reply_markup=InlineKeyboardMarkup([[back_btn(f"admin_user_{uid}")]]))
    except BadRequest: pass

async def admin_add_bal_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    uid = int(query.data.split("_")[-1])
    user_states[ADMIN_USER_ID] = {'state': 'admin_add_balance', 'target_uid': uid}
    await query.edit_message_text(f"Send amount to add to user {uid}:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn(f"admin_user_{uid}")]]))

async def admin_deduct_bal_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    uid = int(query.data.split("_")[-1])
    user_states[ADMIN_USER_ID] = {'state': 'admin_deduct_balance', 'target_uid': uid}
    await query.edit_message_text(f"Send amount to deduct from user {uid}:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn(f"admin_user_{uid}")]]))

async def admin_settings_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        kb = [
            [InlineKeyboardButton("💱 Economy", callback_data="admin_set_economy"),
             InlineKeyboardButton("💳 Payments", callback_data="admin_set_payments")],
            [InlineKeyboardButton("⭐ Premium", callback_data="admin_set_premium"),
             InlineKeyboardButton("👥 Referrals", callback_data="admin_set_referrals")],
            [InlineKeyboardButton("🔒 Security", callback_data="admin_set_security"),
             InlineKeyboardButton("📝 General", callback_data="admin_set_general")],
            [back_btn("admin_panel")]
        ]
        await query.edit_message_text("<b>⚙️ Settings</b>\n\nSelect a category:", parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest: pass

async def admin_set_economy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['inr_to_vc_rate', 'vc_to_inr_rate', 'platform_fee_percent', 'premium_fee_percent',
                'min_gig_budget', 'max_gig_budget', 'new_user_bonus', 'featured_gig_cost', 'featured_product_cost']
        text = "<b>💱 Economy Settings</b>\n\n"
        for k in keys:
            text += f"<code>{k}</code> = {get_setting(k)}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_set_payments_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['upi_id', 'payment_instructions', 'min_deposit_inr', 'max_deposit_inr',
                'withdrawal_enabled', 'min_withdrawal_vc', 'max_withdrawal_vc', 'withdrawal_fee_percent']
        text = "<b>💳 Payment Settings</b>\n\n"
        for k in keys:
            v = get_setting(k)
            text += f"<code>{k}</code> = {v[:50] if v else '(empty)'}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_set_premium_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['premium_enabled', 'premium_monthly_price', 'premium_quarterly_price',
                'premium_yearly_price', 'premium_discount_percent', 'premium_features']
        text = "<b>⭐ Premium Settings</b>\n\n"
        for k in keys:
            v = get_setting(k)
            text += f"<code>{k}</code> = {v[:60] if v else '(empty)'}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_set_referrals_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['referral_enabled', 'referral_bonus_vc', 'referral_percent_on_transactions', 'referral_max_percent_transactions']
        text = "<b>👥 Referral Settings</b>\n\n"
        for k in keys:
            text += f"<code>{k}</code> = {get_setting(k)}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_set_security_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['vpn_detection_enabled', 'vpn_detection_api_key', 'vpn_detection_api_url',
                'vpn_block_mode', 'max_risk_score', 'ip_check_on_deposit', 'ip_check_on_withdrawal',
                'require_verified_for_withdrawal', 'min_reputation_for_withdrawal', 'min_completed_gigs_for_withdrawal']
        text = "<b>🔒 Security Settings</b>\n\n"
        for k in keys:
            v = get_setting(k)
            if 'api_key' in k and v: v = v[:4] + '****'
            text += f"<code>{k}</code> = {v if v else '(empty)'}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_set_general_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        keys = ['bot_name', 'welcome_message', 'maintenance_mode', 'support_username', 'broadcast_footer']
        text = "<b>📝 General Settings</b>\n\n"
        for k in keys:
            v = get_setting(k)
            text += f"<code>{k}</code> = {v[:60] if v else '(empty)'}\n"
        text += "\nTo change, send: <code>set key value</code>"
        user_states[ADMIN_USER_ID] = {'state': 'admin_edit_setting'}
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
    except BadRequest: pass

async def admin_broadcast_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    user_states[ADMIN_USER_ID] = {'state': 'admin_broadcast'}
    await query.edit_message_text("📢 Send the broadcast message (HTML supported):",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("admin_panel")]]))

async def admin_balance_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    user_states[ADMIN_USER_ID] = {'state': 'admin_balance_uid'}
    await query.edit_message_text("Send user ID to add/deduct balance:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("admin_panel")]]))

async def admin_check_user_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    user_states[ADMIN_USER_ID] = {'state': 'admin_check_user'}
    await query.edit_message_text("Send user ID to check:",
                                   reply_markup=InlineKeyboardMarkup([[back_btn("admin_panel")]]))

async def admin_analytics_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_USER_ID: return
    try:
        conn = get_db()
        total_users = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        active_24h = conn.execute("SELECT COUNT(*) as c FROM users WHERE last_active > datetime('now', '-1 day')").fetchone()['c']
        total_gigs = conn.execute("SELECT COUNT(*) as c FROM gigs").fetchone()['c']
        completed_gigs = conn.execute("SELECT COUNT(*) as c FROM gigs WHERE status='completed'").fetchone()['c']
        total_vol = conn.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE amount>0").fetchone()['s']
        total_deposits = conn.execute("SELECT COALESCE(SUM(vault_coins),0) as s FROM deposits WHERE status='approved'").fetchone()['s']
        total_withdrawals = conn.execute("SELECT COALESCE(SUM(vault_coins),0) as s FROM withdrawals WHERE status='approved'").fetchone()['s']
        total_products = conn.execute("SELECT COUNT(*) as c FROM products WHERE is_active=1").fetchone()['c']
        total_sales = conn.execute("SELECT COUNT(*) as c FROM product_purchases").fetchone()['c']
        conn.close()
        text = (f"<b>📊 Analytics</b>\n\n"
                f"👥 Total Users: {total_users}\n"
                f"🟢 Active (24h): {active_24h}\n"
                f"💼 Total Gigs: {total_gigs} ({completed_gigs} completed)\n"
                f"🛍️ Products: {total_products} ({total_sales} sales)\n"
                f"💰 Total Volume: {total_vol:.0f} 🪙\n"
                f"📥 Total Deposits: {total_deposits:.0f} 🪙\n"
                f"📤 Total Withdrawals: {total_withdrawals:.0f} 🪙\n"
                f"🏦 Platform Fees: {get_setting('platform_total_fees','0')} 🪙\n")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup([[back_btn("admin_panel")]]))
    except BadRequest: pass


# ============================================================
# MESSAGE HANDLER — processes all text/file input based on state
# ============================================================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        uid = user.id
        msg = update.message
        state_data = user_states.get(uid, {})
        state = state_data.get('state', '')

        if not state:
            return

        text = msg.text or ''

        # ---- DEPOSIT FLOW ----
        if state == 'deposit_amount':
            try:
                amount_inr = float(text)
                min_d = float(get_setting('min_deposit_inr', '50'))
                max_d = float(get_setting('max_deposit_inr', '10000'))
                if amount_inr < min_d or amount_inr > max_d:
                    await msg.reply_text(f"Amount must be between ₹{min_d:.0f} and ₹{max_d:.0f}")
                    return
                rate = float(get_setting('inr_to_vc_rate', '10'))
                vc = round(amount_inr * rate, 2)
                user_states[uid] = {'state': 'deposit_proof', 'amount_inr': amount_inr, 'vault_coins': vc}
                await msg.reply_text(f"₹{amount_inr:.0f} = {vc:.0f} 🪙\n\nNow upload payment proof (screenshot):")
            except ValueError:
                await msg.reply_text("Please send a valid number.")
            return

        if state == 'deposit_proof':
            file_id = ''
            if msg.photo:
                file_id = msg.photo[-1].file_id
            elif msg.document:
                file_id = msg.document.file_id
            else:
                await msg.reply_text("Please send a photo or document as proof.")
                return
            conn = get_db()
            conn.execute("INSERT INTO deposits (user_id, amount_inr, vault_coins, proof_file_id) VALUES (?,?,?,?)",
                          (uid, state_data['amount_inr'], state_data['vault_coins'], file_id))
            conn.commit(); conn.close()
            user_states.pop(uid, None)
            await msg.reply_text(f"✅ Deposit request submitted!\n₹{state_data['amount_inr']:.0f} → {state_data['vault_coins']:.0f} 🪙\n\nWaiting for admin approval.",
                                  reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            # Notify admin
            try:
                await context.bot.send_message(ADMIN_USER_ID,
                    f"📥 <b>New Deposit</b>\nUser: {user.first_name} ({uid})\n₹{state_data['amount_inr']} → {state_data['vault_coins']} 🪙",
                    parse_mode=ParseMode.HTML)
                await context.bot.send_photo(ADMIN_USER_ID, file_id, caption=f"Proof from {user.first_name}")
            except: pass
            return

        # ---- WITHDRAW FLOW ----
        if state == 'withdraw_amount':
            try:
                vc_amount = float(text)
                min_w = float(get_setting('min_withdrawal_vc', '2000'))
                max_w = float(get_setting('max_withdrawal_vc', '100000'))
                u = ensure_user(uid)
                if vc_amount < min_w or vc_amount > max_w:
                    await msg.reply_text(f"Amount must be between {min_w:.0f} and {max_w:.0f} 🪙")
                    return
                if u['balance'] < vc_amount:
                    await msg.reply_text(f"Insufficient balance ({u['balance']:.0f} 🪙)")
                    return
                fee_pct = float(get_setting('withdrawal_fee_percent', '10'))
                fee = round(vc_amount * fee_pct / 100, 2)
                net_vc = vc_amount - fee
                rate = float(get_setting('vc_to_inr_rate', '0.05'))
                inr = round(net_vc * rate, 2)
                user_states[uid] = {'state': 'withdraw_details', 'vault_coins': vc_amount, 'amount_inr': inr, 'fee': fee}
                await msg.reply_text(f"{vc_amount:.0f} 🪙 - {fee:.0f} fee = {net_vc:.0f} 🪙 → ₹{inr:.2f}\n\nSend your UPI ID / payment details:")
            except ValueError:
                await msg.reply_text("Send a valid number.")
            return

        if state == 'withdraw_details':
            payout_details = text
            if not deduct_balance(uid, state_data['vault_coins'], 'withdrawal',
                                   f"Withdrawal request: {state_data['vault_coins']} VC → ₹{state_data['amount_inr']}"):
                await msg.reply_text("❌ Insufficient balance.")
                user_states.pop(uid, None)
                return
            conn = get_db()
            conn.execute("INSERT INTO withdrawals (user_id, vault_coins, amount_inr, payout_details) VALUES (?,?,?,?)",
                          (uid, state_data['vault_coins'], state_data['amount_inr'], payout_details))
            conn.commit(); conn.close()
            user_states.pop(uid, None)
            await msg.reply_text(f"✅ Withdrawal requested!\n{state_data['vault_coins']:.0f} 🪙 → ₹{state_data['amount_inr']:.2f}\nPayout to: {payout_details}\n\nWaiting for admin processing.",
                                  reply_markup=InlineKeyboardMarkup([[back_btn("wallet")]]))
            try:
                await context.bot.send_message(ADMIN_USER_ID,
                    f"📤 <b>New Withdrawal</b>\nUser: {user.first_name} ({uid})\n{state_data['vault_coins']} 🪙 → ₹{state_data['amount_inr']}\nPayout: {payout_details}",
                    parse_mode=ParseMode.HTML)
            except: pass
            return

        # ---- EDIT BIO ----
        if state == 'edit_bio':
            bio = text[:200]
            conn = get_db()
            conn.execute("UPDATE users SET bio=? WHERE user_id=?", (bio, uid))
            conn.commit(); conn.close()
            user_states.pop(uid, None)
            await msg.reply_text(f"✅ Bio updated!", reply_markup=InlineKeyboardMarkup([[back_btn("profile")]]))
            return

        # ---- EDIT SKILLS ----
        if state == 'edit_skills':
            skills = json.dumps([s.strip() for s in text.split(',')])
            conn = get_db()
            conn.execute("UPDATE users SET skills=? WHERE user_id=?", (skills, uid))
            conn.commit(); conn.close()
            user_states.pop(uid, None)
            await msg.reply_text("✅ Skills updated!", reply_markup=InlineKeyboardMarkup([[back_btn("profile")]]))
            return

        # ---- POST GIG FLOW ----
        if state == 'gig_title':
            user_states[uid] = {'state': 'gig_description', 'title': text[:100]}
            await msg.reply_text("📝 Now send the gig description:")
            return

        if state == 'gig_description':
            user_states[uid] = {**state_data, 'state': 'gig_category', 'description': text[:500]}
            cats = get_categories()
            kb = [[InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"gig_cat_select_{c['name']}")]
                  for c in cats]
            await msg.reply_text("📂 Select category:", reply_markup=InlineKeyboardMarkup(kb))
            return

        if state == 'gig_budget':
            try:
                budget = float(text)
                min_b = float(get_setting('min_gig_budget', '20'))
                max_b = float(get_setting('max_gig_budget', '50000'))
                if budget < min_b or budget > max_b:
                    await msg.reply_text(f"Budget must be {min_b:.0f}-{max_b:.0f} 🪙")
                    return
                u = ensure_user(uid)
                if u['balance'] < budget:
                    await msg.reply_text(f"Insufficient balance ({u['balance']:.0f} 🪙)")
                    return
                user_states[uid] = {**state_data, 'state': 'gig_deadline', 'budget': budget}
                await msg.reply_text("⏰ Deadline in hours (default 48):")
            except ValueError:
                await msg.reply_text("Send a valid number.")
            return

        if state == 'gig_deadline':
            try:
                hours = int(text) if text.strip() else 48
            except:
                hours = 48
            data = state_data
            conn = get_db()
            conn.execute("INSERT INTO gigs (poster_id, title, description, category, budget, deadline_hours) VALUES (?,?,?,?,?,?)",
                          (uid, data['title'], data['description'], data.get('category', 'Other'), data['budget'], hours))
            conn.commit(); conn.close()
            user_states.pop(uid, None)
            await msg.reply_text(f"✅ Gig posted!\n<b>{data['title']}</b>\n💰 {data['budget']:.0f} 🪙 | ⏰ {hours}h",
                                  parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            return

        # ---- GIG APPLICATION ----
        if state == 'gig_apply_text':
            gig_id = state_data['gig_id']
            conn = get_db()
            conn.execute("INSERT INTO gig_applications (gig_id, applicant_id, proposal_text) VALUES (?,?,?)",
                          (gig_id, uid, text[:500]))
            conn.commit()
            gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
            conn.close()
            user_states.pop(uid, None)
            await msg.reply_text("✅ Application submitted!", reply_markup=InlineKeyboardMarkup([[back_btn(f"view_gig_{gig_id}")]]))
            if gig:
                await notify_user(context.bot, gig['poster_id'],
                                  f"📝 New application for <b>{gig['title']}</b> from {user.first_name}")
            return

        # ---- DELIVER GIG ----
        if state == 'deliver_gig':
            gig_id = state_data['gig_id']
            delivery_text = text or ''
            file_id = ''
            if msg.document:
                file_id = msg.document.file_id
            elif msg.photo:
                file_id = msg.photo[-1].file_id
            conn = get_db()
            conn.execute("UPDATE gigs SET status='delivered', delivered_at=CURRENT_TIMESTAMP, delivery_text=?, delivery_file_id=? WHERE id=?",
                          (delivery_text, file_id, gig_id))
            conn.commit()
            gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
            conn.close()
            user_states.pop(uid, None)
            await msg.reply_text("✅ Work delivered!", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            if gig:
                await notify_user(context.bot, gig['poster_id'],
                                  f"📦 Work delivered for <b>{gig['title']}</b>! Review and approve.")
            return

        # ---- DISPUTE ----
        if state == 'dispute_reason':
            gig_id = state_data['gig_id']
            conn = get_db()
            gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
            if gig:
                against = gig['claimed_by'] if uid == gig['poster_id'] else gig['poster_id']
                conn.execute("INSERT INTO disputes (gig_id, filed_by, filed_against, reason) VALUES (?,?,?,?)",
                              (gig_id, uid, against, text[:500]))
                conn.execute("UPDATE gigs SET status='disputed' WHERE id=?", (gig_id,))
                conn.execute("UPDATE users SET total_disputes_filed=total_disputes_filed+1 WHERE user_id=?", (uid,))
                conn.commit()
                await notify_user(context.bot, against, f"⚠️ A dispute was filed against you for gig <b>{gig['title']}</b>")
                try:
                    await context.bot.send_message(ADMIN_USER_ID,
                        f"⚠️ <b>New Dispute</b>\nGig: {gig['title']} (#{gig_id})\nFiled by: {user.first_name}",
                        parse_mode=ParseMode.HTML)
                except: pass
            conn.close()
            user_states.pop(uid, None)
            await msg.reply_text("⚠️ Dispute filed. Admin will review.", reply_markup=InlineKeyboardMarkup([[back_btn("gigs_menu")]]))
            return

        # ---- RATING ----
        if state == 'rate_worker':
            try:
                rating = float(text)
                if rating < 1 or rating > 5: raise ValueError
                gig_id = state_data['gig_id']
                conn = get_db()
                gig = conn.execute("SELECT * FROM gigs WHERE id=?", (gig_id,)).fetchone()
                if gig:
                    conn.execute("UPDATE gigs SET worker_rating=? WHERE id=?", (rating, gig_id))
                    conn.execute("UPDATE users SET total_ratings=total_ratings+1, rating_sum=rating_sum+? WHERE user_id=?",
                                  (rating, gig['claimed_by']))
                    conn.commit()
                    update_reputation(gig['claimed_by'])
                    await check_badges(gig['claimed_by'], context.bot)
                conn.close()
                user_states.pop(uid, None)
                await msg.reply_text(f"✅ Rated {rating}⭐", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
            except ValueError:
                await msg.reply_text("Send a number 1-5.")
            return

        if state == 'rate_product':
            try:
                rating = float(text)
                if rating < 1 or rating > 5: raise ValueError
                prod_id = state_data['product_id']
                conn = get_db()
                conn.execute("UPDATE product_purchases SET rating=? WHERE product_id=? AND buyer_id=? ORDER BY created_at DESC LIMIT 1",
                              (rating, prod_id, uid))
                p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
                if p:
                    new_total = p['total_ratings'] + 1
                    new_avg = ((p['avg_rating'] * p['total_ratings']) + rating) / new_total
                    conn.execute("UPDATE products SET avg_rating=?, total_ratings=? WHERE id=?", (new_avg, new_total, prod_id))
                    conn.execute("UPDATE users SET total_ratings=total_ratings+1, rating_sum=rating_sum+? WHERE user_id=?",
                                  (rating, p['seller_id']))
                    conn.commit()
                    update_reputation(p['seller_id'])
                conn.close()
                user_states.pop(uid, None)
                await msg.reply_text(f"✅ Rated {rating}⭐", reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
            except ValueError:
                await msg.reply_text("Send a number 1-5.")
            return

        # ---- SELL PRODUCT FLOW ----
        if state == 'product_file':
            file_id = ''
            if msg.document:
                file_id = msg.document.file_id
            elif msg.photo:
                file_id = msg.photo[-1].file_id
            elif msg.video:
                file_id = msg.video.file_id
            elif msg.audio:
                file_id = msg.audio.file_id
            else:
                await msg.reply_text("Please send a file, photo, video, or audio.")
                return
            user_states[uid] = {'state': 'product_title', 'file_id': file_id}
            await msg.reply_text("📝 Send product title:")
            return

        if state == 'product_title':
            user_states[uid] = {**state_data, 'state': 'product_description', 'title': text[:100]}
            await msg.reply_text("📝 Send product description:")
            return

        if state == 'product_description':
            user_states[uid] = {**state_data, 'state': 'product_category', 'description': text[:500]}
            cats = get_categories()
            kb = [[InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"prod_cat_select_{c['name']}")]
                  for c in cats]
            await msg.reply_text("📂 Select category:", reply_markup=InlineKeyboardMarkup(kb))
            return

        if state == 'product_price':
            try:
                price = float(text)
                if price <= 0: raise ValueError
                data = state_data
                conn = get_db()
                conn.execute("INSERT INTO products (seller_id, title, description, category, price, file_id) VALUES (?,?,?,?,?,?)",
                              (uid, data['title'], data['description'], data.get('category','Other'), price, data['file_id']))
                conn.commit(); conn.close()
                user_states.pop(uid, None)
                await msg.reply_text(f"✅ Product listed!\n<b>{data['title']}</b> — {price:.0f} 🪙",
                                      parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[back_btn("store_menu")]]))
            except ValueError:
                await msg.reply_text("Send a valid price.")
            return

        # ---- SEARCH ----
        if state == 'search_gigs':
            keyword = text.strip()
            conn = get_db()
            results = conn.execute("SELECT * FROM gigs WHERE status='open' AND (title LIKE ? OR description LIKE ?) LIMIT 10",
                                    (f'%{keyword}%', f'%{keyword}%')).fetchall()
            conn.close()
            user_states.pop(uid, None)
            t = f"<b>🔍 Search: {keyword}</b>\n\n"
            if not results: t += "No results."
            for g in results:
                t += f"• <b>{g['title']}</b> — {g['budget']:.0f} 🪙\n"
            kb = [[InlineKeyboardButton(g['title'][:30], callback_data=f"view_gig_{g['id']}")] for g in results]
            kb.append([back_btn("gigs_menu")])
            await msg.reply_text(t, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
            return

        if state == 'search_products':
            keyword = text.strip()
            conn = get_db()
            results = conn.execute("SELECT * FROM products WHERE is_active=1 AND (title LIKE ? OR description LIKE ?) LIMIT 10",
                                    (f'%{keyword}%', f'%{keyword}%')).fetchall()
            conn.close()
            user_states.pop(uid, None)
            t = f"<b>🔍 Search: {keyword}</b>\n\n"
            if not results: t += "No results."
            for p in results:
                t += f"• <b>{p['title']}</b> — {p['price']:.0f} 🪙\n"
            kb = [[InlineKeyboardButton(p['title'][:30], callback_data=f"view_product_{p['id']}")] for p in results]
            kb.append([back_btn("store_menu")])
            await msg.reply_text(t, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
            return

        # ---- ADMIN STATES ----
        if state == 'admin_edit_setting' and uid == ADMIN_USER_ID:
            if text.startswith('set '):
                parts = text[4:].split(' ', 1)
                if len(parts) == 2:
                    set_setting(parts[0], parts[1])
                    user_states.pop(uid, None)
                    await msg.reply_text(f"✅ {parts[0]} = {parts[1]}", reply_markup=InlineKeyboardMarkup([[back_btn("admin_settings")]]))
                else:
                    await msg.reply_text("Format: set key value")
            return

        if state == 'admin_broadcast' and uid == ADMIN_USER_ID:
            conn = get_db()
            users = conn.execute("SELECT user_id FROM users WHERE is_banned=0").fetchall()
            conn.close()
            footer = get_setting('broadcast_footer', '')
            broadcast_text = text + (f"\n\n{footer}" if footer else '')
            sent = 0
            for u in users:
                try:
                    await context.bot.send_message(u['user_id'], broadcast_text, parse_mode=ParseMode.HTML)
                    sent += 1
                except: pass
            user_states.pop(uid, None)
            await msg.reply_text(f"📢 Broadcast sent to {sent}/{len(users)} users.",
                                  reply_markup=InlineKeyboardMarkup([[back_btn("admin_panel")]]))
            return

        if state == 'admin_add_balance' and uid == ADMIN_USER_ID:
            try:
                amount = float(text)
                target = state_data['target_uid']
                add_balance(target, amount, 'admin_add', f'Added by admin')
                user_states.pop(uid, None)
                await msg.reply_text(f"✅ Added {amount} 🪙 to user {target}",
                                      reply_markup=InlineKeyboardMarkup([[back_btn(f"admin_user_{target}")]]))
            except: await msg.reply_text("Send a valid number.")
            return

        if state == 'admin_deduct_balance' and uid == ADMIN_USER_ID:
            try:
                amount = float(text)
                target = state_data['target_uid']
                if deduct_balance(target, amount, 'admin_deduct', f'Deducted by admin'):
                    await msg.reply_text(f"✅ Deducted {amount} 🪙 from user {target}")
                else:
                    await msg.reply_text("❌ Insufficient balance.")
                user_states.pop(uid, None)
            except: await msg.reply_text("Send a valid number.")
            return

        if state == 'admin_balance_uid' and uid == ADMIN_USER_ID:
            try:
                target_uid = int(text)
                user_states[uid] = {'state': 'admin_balance_action', 'target_uid': target_uid}
                await msg.reply_text(f"User {target_uid}. Send: <code>add AMOUNT</code> or <code>deduct AMOUNT</code>",
                                      parse_mode=ParseMode.HTML)
            except: await msg.reply_text("Send valid user ID.")
            return

        if state == 'admin_balance_action' and uid == ADMIN_USER_ID:
            parts = text.split()
            if len(parts) == 2:
                action, amount = parts[0].lower(), float(parts[1])
                target = state_data['target_uid']
                if action == 'add':
                    add_balance(target, amount, 'admin_add', 'Admin adjustment')
                    await msg.reply_text(f"✅ +{amount} 🪙 to {target}")
                elif action == 'deduct':
                    deduct_balance(target, amount, 'admin_deduct', 'Admin adjustment')
                    await msg.reply_text(f"✅ -{amount} 🪙 from {target}")
                user_states.pop(uid, None)
            return

        if state == 'admin_check_user' and uid == ADMIN_USER_ID:
            try:
                target_uid = int(text)
                # Redirect to user detail
                conn = get_db()
                u = conn.execute("SELECT * FROM users WHERE user_id=?", (target_uid,)).fetchone()
                conn.close()
                if u:
                    risk = calculate_risk_score(target_uid)
                    circular = detect_circular_transactions(target_uid)
                    u = dict(u)
                    level, xp = calculate_level(u)
                    t = (f"<b>🔍 User Check: {u['first_name']}</b>\n\n"
                         f"ID: {target_uid}\n"
                         f"Balance: {u['balance']:.1f} | Frozen: {u['frozen_balance']:.1f}\n"
                         f"Earned: {u['total_earned']:.1f} | Spent: {u['total_spent']:.1f}\n"
                         f"Gigs: {u['completed_gigs']} | Failed: {u['failed_gigs']}\n"
                         f"Risk: {risk} | VPN: {'⚠️' if u['is_vpn_detected'] else '✅'}\n"
                         f"Disputes: filed {u['total_disputes_filed']}, lost {u['total_disputes_lost']}\n")
                    if circular:
                        t += f"⚠️ CIRCULAR TX with: {circular}\n"
                    await msg.reply_text(t, parse_mode=ParseMode.HTML,
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("View Full", callback_data=f"admin_user_{target_uid}"), back_btn("admin_panel")]]))
                else:
                    await msg.reply_text("User not found.")
                user_states.pop(uid, None)
            except: await msg.reply_text("Send valid user ID.")
            return

    except Exception as e:
        logger.error(f"handle_message error: {e}")

# ============================================================
# CATEGORY SELECTION CALLBACKS (for gig/product creation)
# ============================================================

async def gig_cat_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cat = query.data.replace("gig_cat_select_", "")
    uid = query.from_user.id
    if uid in user_states:
        user_states[uid] = {**user_states[uid], 'state': 'gig_budget', 'category': cat}
        await query.edit_message_text(f"Category: {cat}\n\n💰 Send the budget in 🪙:")

async def prod_cat_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cat = query.data.replace("prod_cat_select_", "")
    uid = query.from_user.id
    if uid in user_states:
        user_states[uid] = {**user_states[uid], 'state': 'product_price', 'category': cat}
        await query.edit_message_text(f"Category: {cat}\n\n💰 Send the price in 🪙:")

# ============================================================
# AUTO-COMPLETE JOB (runs periodically)
# ============================================================

async def auto_complete_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        hours = int(get_setting('auto_complete_hours', '168'))
        conn = get_db()
        overdue = conn.execute(
            "SELECT * FROM gigs WHERE status='delivered' AND delivered_at < datetime('now', ?)",
            (f'-{hours} hours',)).fetchall()
        for g in overdue:
            escrow = conn.execute("SELECT * FROM escrow WHERE gig_id=? AND status='held'", (g['id'],)).fetchone()
            if escrow:
                fee_pct = escrow['platform_fee']
                transfer_frozen_to_user(g['poster_id'], g['claimed_by'], escrow['amount'], fee_pct)
                conn.execute("UPDATE gigs SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", (g['id'],))
                conn.execute("UPDATE escrow SET status='released', released_at=CURRENT_TIMESTAMP WHERE id=?", (escrow['id'],))
                conn.execute("UPDATE users SET completed_gigs=completed_gigs+1 WHERE user_id=?", (g['claimed_by'],))
                try:
                    await context.bot.send_message(g['poster_id'], f"⏰ Gig <b>{g['title']}</b> auto-completed.", parse_mode=ParseMode.HTML)
                    await context.bot.send_message(g['claimed_by'], f"⏰ Gig <b>{g['title']}</b> auto-completed. Payment released!", parse_mode=ParseMode.HTML)
                except: pass
        # Check overdue assigned gigs
        overdue_assigned = conn.execute(
            "SELECT * FROM gigs WHERE status='assigned' AND datetime(claimed_at, '+' || deadline_hours || ' hours') < datetime('now')").fetchall()
        for g in overdue_assigned:
            conn.execute("UPDATE gigs SET status='overdue' WHERE id=? AND status='assigned'", (g['id'],))
            try:
                await context.bot.send_message(g['poster_id'], f"⏰ Gig <b>{g['title']}</b> is overdue!", parse_mode=ParseMode.HTML)
                await context.bot.send_message(g['claimed_by'], f"⏰ Gig <b>{g['title']}</b> is overdue! Please deliver ASAP.", parse_mode=ParseMode.HTML)
            except: pass
        conn.commit(); conn.close()
    except Exception as e:
        logger.error(f"auto_complete job error: {e}")

# ============================================================
# MAIN
# ============================================================

def main():
    init_db()
    logger.info(f"Starting The Vault Bot (Admin: {ADMIN_USER_ID})")

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start_cmd))

    # Callback queries
    cb_handlers = {
        "main_menu": main_menu_cb,
        "wallet": wallet_cb,
        "deposit": deposit_cb,
        "withdraw": withdraw_cb,
        "profile": profile_cb,
        "edit_bio": edit_bio_cb,
        "edit_skills": edit_skills_cb,
        "referrals": referrals_cb,
        "premium_menu": premium_menu_cb,
        "gigs_menu": gigs_menu_cb,
        "gig_categories": gig_categories_cb,
        "post_gig": post_gig_cb,
        "search_gigs": search_gigs_cb,
        "search_products": search_products_cb,
        "store_menu": store_menu_cb,
        "product_categories": product_categories_cb,
        "sell_product": sell_product_cb,
        "leaderboard": leaderboard_cb,
        "notifications": notifications_cb,
        "admin_panel": admin_panel_cb,
        "admin_settings": admin_settings_cb,
        "admin_set_economy": admin_set_economy_cb,
        "admin_set_payments": admin_set_payments_cb,
        "admin_set_premium": admin_set_premium_cb,
        "admin_set_referrals": admin_set_referrals_cb,
        "admin_set_security": admin_set_security_cb,
        "admin_set_general": admin_set_general_cb,
        "admin_broadcast": admin_broadcast_cb,
        "admin_balance": admin_balance_cb,
        "admin_check_user": admin_check_user_cb,
        "admin_analytics": admin_analytics_cb,
        "admin_risks": admin_risks_cb,
    }
    for pattern, handler in cb_handlers.items():
        app.add_handler(CallbackQueryHandler(handler, pattern=f"^{pattern}$"))

    # Pattern-based callbacks
    app.add_handler(CallbackQueryHandler(tx_history_cb, pattern=r"^tx_history_\d+$"))
    app.add_handler(CallbackQueryHandler(browse_gigs_cb, pattern=r"^browse_gigs_"))
    app.add_handler(CallbackQueryHandler(view_gig_cb, pattern=r"^view_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(apply_gig_cb, pattern=r"^apply_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(gig_apps_cb, pattern=r"^gig_apps_\d+$"))
    app.add_handler(CallbackQueryHandler(accept_app_cb, pattern=r"^accept_app_\d+$"))
    app.add_handler(CallbackQueryHandler(deliver_gig_cb, pattern=r"^deliver_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(approve_gig_cb, pattern=r"^approve_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(revision_gig_cb, pattern=r"^revision_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(dispute_gig_cb, pattern=r"^dispute_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(cancel_gig_cb, pattern=r"^cancel_gig_\d+$"))
    app.add_handler(CallbackQueryHandler(my_gigs_cb, pattern=r"^my_gigs_\d+$"))
    app.add_handler(CallbackQueryHandler(my_work_cb, pattern=r"^my_work_\d+$"))
    app.add_handler(CallbackQueryHandler(browse_products_cb, pattern=r"^browse_products_"))
    app.add_handler(CallbackQueryHandler(view_product_cb, pattern=r"^view_product_\d+$"))
    app.add_handler(CallbackQueryHandler(buy_product_cb, pattern=r"^buy_product_\d+$"))
    app.add_handler(CallbackQueryHandler(sell_product_cb, pattern=r"^sell_product$"))
    app.add_handler(CallbackQueryHandler(my_products_cb, pattern=r"^my_products_\d+$"))
    app.add_handler(CallbackQueryHandler(my_purchases_cb, pattern=r"^my_purchases_\d+$"))
    app.add_handler(CallbackQueryHandler(delete_product_cb, pattern=r"^delete_product_\d+$"))
    app.add_handler(CallbackQueryHandler(buy_premium_cb, pattern=r"^buy_premium_"))
    app.add_handler(CallbackQueryHandler(admin_deposits_cb, pattern=r"^admin_deposits_\d+$"))
    app.add_handler(CallbackQueryHandler(approve_deposit_cb, pattern=r"^approve_deposit_\d+$"))
    app.add_handler(CallbackQueryHandler(reject_deposit_cb, pattern=r"^reject_deposit_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_withdrawals_cb, pattern=r"^admin_withdrawals_\d+$"))
    app.add_handler(CallbackQueryHandler(approve_withdrawal_cb, pattern=r"^approve_withdrawal_\d+$"))
    app.add_handler(CallbackQueryHandler(reject_withdrawal_cb, pattern=r"^reject_withdrawal_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_disputes_cb, pattern=r"^admin_disputes_\d+$"))
    app.add_handler(CallbackQueryHandler(resolve_dispute_cb, pattern=r"^resolve_dispute_"))
    app.add_handler(CallbackQueryHandler(admin_users_cb, pattern=r"^admin_users_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_user_detail_cb, pattern=r"^admin_user_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_ban_cb, pattern=r"^admin_ban_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_verify_cb, pattern=r"^admin_verify_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_add_bal_cb, pattern=r"^admin_add_bal_\d+$"))
    app.add_handler(CallbackQueryHandler(admin_deduct_bal_cb, pattern=r"^admin_deduct_bal_\d+$"))
    app.add_handler(CallbackQueryHandler(gig_cat_select_cb, pattern=r"^gig_cat_select_"))
    app.add_handler(CallbackQueryHandler(prod_cat_select_cb, pattern=r"^prod_cat_select_"))

    # Message handler for all text/file input
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))

    # Periodic jobs
    if app.job_queue:
        app.job_queue.run_repeating(auto_complete_job, interval=3600, first=60)

    if WEBHOOK_URL:
        logger.info(f"Bot starting webhook on port {PORT}...")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=f"webhook/{BOT_TOKEN}",
            webhook_url=f"{WEBHOOK_URL}/webhook/{BOT_TOKEN}",
            drop_pending_updates=True,
        )
    else:
        logger.info("Bot starting polling...")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
