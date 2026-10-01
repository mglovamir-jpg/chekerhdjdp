import asyncio
import aiohttp
import json
import os
import sys
from collections import Counter


if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATE_FILE = os.path.join(BASE_DIR, "vk_monitor_state.json")
DOMAINS_FILE = os.path.join(BASE_DIR, "domains.json")
DEBUG_LOG = os.path.join(BASE_DIR, "debug_log.txt")

VK_URL = "https://api.vk.com/method/utils.resolveScreenName"
VK_API_VERSION = "5.199"

MAX_TOKENS = 19
CONCURRENCY = 350

REQUEST_TIMEOUT = 5
SESSION_TIMEOUT = 20

CHECK_INTERVAL = 3


# === VK токены (вставь свои) ===
VK_TOKENS = [
    "vk1.a.wAl6aBQ_GmEsQGAi6q7BtV0FYDxuZJORtL3OIAufyutdfThOeKrHw6DAl5EI9Wrh66fEnNyGWp8IDuW30lT5WQrwZi_iZhkapyTXBhVGU4AyXYRqJ3FZ9U3vTd0Q4u83XgQfN8tJgM1pwRghLcmYmRyD32J_m544PAOwBYCXIJb8NJ0JCLIcLrIsy6nLZps82ZOm1XU_fUskrUbL-g433d7BANtFHXWMhszNoFYnlw4",
    "vk1.a.7iqpuE8101IKlxdmvmT6nrmt1alOyChIfcQNu3aVXpXV1c2gRc7wR0cl5ZPOJKNqfbfqGVAaGsIJp2506FyTziX4CFxo964JgmvI_jiTZ3luLN814KnFdXB-v9hPg69_y0Z1iMoWPFbKjCUcK9xrqZa1rcIqGnuEf00_7w1GhNtW4-cp8R74_ixqCJWGAAuxcLxhzYLydlwcsaihXOCh9HbmLEbaX-KB1L2sgOOmFwk",
    "vk1.a.D8E7pkXFH3f_6cJB1yiy3WYSmwuGNBmG1MIa1XHYExgxn_L8TnddrwMAzP7NAl8xm2Bqr_LvXdF7wgnLsSPxGjjXjexPDoJD4cePAEZq-qSFsPXeiYSlmtyx7hUob-QQmbcpEbtykuF5OC_IfklPkDBOU2hVSiw2NKasT6ssihG6q317boqikVmucgi_9eDy2DW5-vz7r2mjr45WlYCM7PSiv2F-AiHzV2A_3i-ssDw",
    "vk1.a.etuL9WYu0qSWi8DxSWjzM6NC5cP5ZNNiGDhib_kfp3mCNmojD35U_5BPeY9j5m62fwCTDp2UPy1g2azuhApbXRJPXEPdaERkjeq44yH8Tx8lhJfLVTd44Exiclqh9aDcOpUPP5k34ONZQdW0ys_XQ4zF-CTGPNk8OerEsXo32-8L0LUPsTZOHkTEZh932273nUvHamwAaLO9olXRV6ukgxJpoGxgmDykRQRSq-l9W7Y",
    "vk1.a.RI19EXM1EVRC9qEph2F3XE0Oksx7W7q_6VUIcsvoNtSD3DIo_qSshe0NACz25_PUqHneeKb6rIi-QekU3jJI1LE0-Coc0uyB8jOSU2hIpLlb_kogKTwWg2bdVMZtXIIt0kXqvapccgWLC-LwCSyr3f3aTzbcHu5l1OJNqXRo-azE28s6hbrsWCK38OdIGTHYGDYy2PMhU74YGYgqLx6sONZJ_XHr5RmC0jQEiLWFJNA",
    "vk1.a.0cnQMXMgwtwqtqhTXZb4aEEH1Do6MP3sbP2mljdfMZ4lwKyntAb6o0sB1JiBDpELp2JkpOe49MyvD6F2Dv5ofOPtX6CcZh-z2g7mutzVvhQCBl9catNi3xp9xX_ZFMy_MR2e1oyGaI983HNRPOKIC33pC_8KMR3jPE9MPjLEFg59qCBS2OCIVbAIPs1ay_K64UqGrazkgczI7mWBNQ5VpjK2u_fKSSYm2ofgClalKRo",
    "vk1.a.nwOhJ9t7VIcMGcID1W9D-5tk7L4yiexgAblS_buC88aQKh7onxxuz273aFbtsLkamKf_ysA9u4Qlgon5F9SozuJidqS6Kz4v3_myQVJ5GRbzGWNCF70j-zV3vbiuvmbKN5IjkBHol24QuvB3hqGBPoJkXybvXbbtqg2J0-0GatAoiPnSyNKD90_u2ox5PYqI-T0PSkjj5cKeXkPIR4-LzNbFRx8CfbOJWfFdNFwc85E",
    "vk1.a.CQP-VM5H6QH4zRFGc9VI876jJhydGIzaigAZZGdfC10BpZ0XmYHTPCy2F-5ciHA0LMsK290HVjKyiJpIryPud8K_-722_xepI8hRWV2WfmdhRkM1B7To4aJqO2XwC5ImU5oMwODLZqhhBpXAgnFynxGjCd6M3xxE2EtklgWWWYVAUztUi3G-QXlEkUyRcF-zNPGvoI_IzNhqDS5FCwIfAc0pkTd--x4I_o-wABmDHLw",
    "vk1.a.zkMELjHGLTYm1ItKA3ASbXjqqmp0aXmoqoJYxVrpSkYOK88nSTUBfhwSIYZv2nCItnfUkD2d0EdBAmliC9p3JSPi-d5bl4Tg4xpAEjCGJsLMmMMHSMhKFgXN0hUfnJy17Cg30liqHUUTrlll9B8x-eqyeBrqH9MPTEyQ8PS_QqY0NH5U5jKuFx0xXzI-mQbkwf6sh1bi9qlDh6aHk9sGALpDg-08vbIRpqJ36NptX-o",
    "vk1.a.NUjSRfXeoIPNO1LdFCTkt64Py7cV0dkV18X0ijumwqx-VvZoTGqS5xIoOLahG-0Ci_IqxHcOL0I2D4j4CtFmz-bLcW5B5MgX_QmUMoW5pTv0LmepTadEeBkf8gWd_oan8zx9I5CTizXlMn9eTt3NdjH3G2I_SI-SKmjgD6eIep-bTImv7LxR8qhbGzXjOkSL8b0YhAmJCB1Pic4YkaevtYzGVQWbNkgZNjwbL9k3TH8",
    "vk1.a.tnfKVGlz8f_NjnFhnvq_9CNTrD85ofMS6c6tiTMvS0gmskN3E0_WY7zqohMBAE4DlIBFrIStZ9qomgPmpfW95hVQ9z8Q5GS9MdVRAdV_i2me8IKQ3Qs3Q4lknT4CQ0XEsPxwfoH-hBtcyY5YX4t-KjF6Vrj5ghVimXGAUY1eFqAmLp4cCmEsqgbd5O0uO6CLfku9uOeZnTATB__2bnZ-kK8sNEJqL9vMwHX1x-xZHHE",
    "vk1.a.PaT0ENF-_5lZqCbDKVHWPbWjFaFkKIPzBBz9V_r4-PjC22_fe6RA7I5JM-8ji37oSo8WhPN7CKtnWKwKqXo8Zb36-jkDdFmp7O9KcLqSb0GfruTuB75kLd0fDZBHHS89JKa6C4Fg9YaP6PyJV1abDNN3Bn9NwcuD7RFCPzF-hXERGgYHEQntDnLmioDsOy2e3h5NZi2YPn1w-aWeVHkgf-ilyo226KqmLNfL9G-iGvw",
    "vk1.a._r6Ut_VTtpmBy4QJrphiDhgroaq4o4ld_WHg2OCofNkNyxrbcZd-JMaEYelHeae2_Jtxvxq1HLLHBCw5oQrgPn_QWY6yC9mo_6dR0Ys8AqFcNEaiq2YTZs_jUGBJPzv5rhT9jQsVWYlEKYm19m73po6VypdtfTTpOsa3qzOjufeRv8zq398QYYYyOuCmANTRzOUPvhQZaThfhiZDjHTWUYuIjziz7YGkt6ymriymL0s",
    "vk1.a.Cn2dW0AOFpU0bBHDZBI6Tpssvu--jJJEvjJgvYEHU11djEuV-ypoFgm3iSM5QQ93QVAHgdWsGbvGpcVw3z-PBNNtTexBN-3ffTSHRTIbJbXWuMWDs9VX0VNEWUjnWCtdtiA9EN3Jri7AA7QQUbzYnKvO5dYHEnimu6zdgVAG4TQAOWZyHf8ScHW0ZnL0AKMkRewsgbrwr07iPS3WOZ-NdCr3lWTZ0Gltzo3y5Wpaxx8",
    "vk1.a.RrZtXGTdt6gDQcr0xzdflPwXeDusuvl4Z3WgIA4yLvbOTmcllADBD33BdsUrKdozEwwtt-HrqZwaya5Ji8gcnoIHmlawVHwbbrizQebOToGqbe1Vg8wp546rFunqzp5mmhp-0veLzSxyWE0OxzP55pP4Al_D6m7Fx7_bdDGjXta1dS4ZW-UwKjLOIi7oRKFi-FrRF9JKMcJn3lvcq8B6-Ehd_nWFLqrfCrcACjHSskA",
    "vk1.a.Iw9XXSuQNtPn0oXKbEPukzYh7FORedjx--_Me-9ybaqsqEFwoh4dlK21zgNM44E_v0W9rFe_PKS798Xb3PyCCUkqKJ2NtFS9-qluEdlJM095z7FpkWCggMksbkBNTJZp3ibyayVI45WvcsIWavtNqEHOVI3s_O0JDzXB3hMMY0yn_ULPe0NC8gwoU0CNKZ057Ev31suYcUzpBGpxXi4tayEFpOLfSn6bFVUMtGJ4gOs",
    "vk1.a.e6SrzCMlLxiHJ2N4EDrNau1Lradl8Sb8-oN_UH9gwOyeJihyklrof91dFi590QWcn5htIB5WLe_ArN-_Xt0pb-TUCyBjHTJR1ioQHS9Dsma1tL339h9F4YFboywvZwY-SgxAD97rgtIHA_m3VyDAiY5bwYoylJ8x7wHdpSiTqsXaXTh8V63QpV5oXikkLDG2S2OvEE603NRCEbzwtVjpg3b2oOmSi3bA7Bsq4Km0ND4",
    "vk1.a.WUCIErVivWNxe164WBy0Y2a5mlwatX8BmpgunVvEQLyGRsts9FLJTD0scFi3R3-730guQ_qGC0VvFr8cILoJP7KCrcLkawxgWnV0Mewf3EIHM8GXyl_bx6K2cuSDA_qZjjirdhYpU6ZwEsV2HLtPwmo9WmsK6h96xsgP6dAn3zQqCTYfTy-3Dg2u7_UfEdyg2yysEnTkU-aIOMKuZj_ap5D4IyfHgFo5SRVXzhfU75E",
    "vk1.a.Pt10zABl2XhHcHDEF2RMWWCNxKwsVqgkwI-O_71OXw1q4XEmX8OC3yIACxGAGD7aK6Lvoyl8Cv_EBkMImcvrX24pBCJ3oNw9eObc9khV-IjoKENFIWUM_gooCYXEb4NWfCtyDO2F0B7kF3TT9MjOGRX4QSO5QtYPryt6QWIshdt4jOyIZEccY6kCD14_thrVJbQ4XYDbX96mAcBX34zV9PdF-DRpbPGjNhuSCo8opI8",
    "vk1.a.o5UtMbriKbUJk40f4Yhzxtjlz-qwsrxqj8SX0TW9rYlEe56Xzn2_O8hWfajMrHuXNtriCAwMEymOYE3oxrSKEkKk7nmjFBVgljHcuZqAItoeLKm0AqDTW2N0njHhCO6hrlkF5Txk_BRAn7bQOVY2adDN1ka8ewAYT5oZdqQiAuhdnF28zKIL87yO3Kvhwj_EGWGeA4W8GlH0iAiZoo4L9e1pdGzqu8v4zndnH3eQVMc",
]


# === Telegram ===
TG_BOT_TOKEN = "8805037958:AAH3ZZFCLj4gif--H8cQRDHXyAY1L_bcLpk"
TG_CHAT_ID = "-1004314906489"


# Глобальные счётчики ошибок
errors = Counter()
error_details = []


def log_error(category, message):
    errors[category] += 1
    error_details.append(f"[{category}] {message}")


def load_json(filename, default):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error("load_json", f"{filename}: {e}")
        return default


def save_json(filename, data):
    tmp = filename + ".tmp"
    sorted_data = dict(sorted(data.items()))
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, filename)


def load_tokens():
    tokens = []
    for token in VK_TOKENS:
        if isinstance(token, str):
            token = token.strip()
            if token:
                tokens.append(token)
    tokens = tokens[:MAX_TOKENS]
    if not tokens:
        raise RuntimeError("Нет VK токенов")
    return tokens


def load_domains():
    if not os.path.exists(DOMAINS_FILE):
        raise RuntimeError(f"Файл {DOMAINS_FILE} не найден")
    raw = load_json(DOMAINS_FILE, [])
    if not raw:
        raise RuntimeError("domains.json пустой")

    result = []
    seen = set()
    for domain in raw:
        if not isinstance(domain, str):
            log_error("domain_type", f"не строка: {domain!r}")
            continue
        d = domain.strip().lower()
        if not d:
            continue
        d = d.replace("https://", "").replace("http://", "")
        d = d.replace("www.vk.com/", "").replace("vk.com/", "")
        d = d.lstrip("@").rstrip("/")
        if d and d not in seen:
            seen.add(d)
            result.append(d)

    if not result:
        raise RuntimeError("Нет валидных доменов")
    return result


def owner_number(object_id):
    return "FREE" if object_id is None else str(object_id)


def owner_url(object_type, object_id):
    if object_id is None:
        return "FREE"
    if object_type == "user":
        return f"vk.com/id{object_id}"
    if object_type in ("group", "page"):
        return f"vk.com/club{object_id}"
    if object_type == "application":
        return f"vk.com/app{object_id}"
    return f"vk.com/{object_type}{object_id}"


# === ПРОВЕРКА ТОКЕНОВ ===
async def check_tokens(session, tokens):
    print("\n=== ПРОВЕРКА VK ТОКЕНОВ ===")

    url = "https://api.vk.com/method/users.get"
    alive = 0

    for i, token in enumerate(tokens, 1):
        params = {"user_ids": "1", "access_token": token, "v": VK_API_VERSION}
        try:
            async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                data = await response.json(content_type=None)
        except Exception as e:
            print(f"[{i:2}] ❌ NETWORK ERROR — {type(e).__name__}")
            log_error("token_network", f"token #{i}: {type(e).__name__}")
            continue

        if "error" in data:
            err = data["error"]
            code = err.get("error_code")
            msg = err.get("error_msg", "unknown")
            print(f"[{i:2}] ❌ МЁРТВЫЙ — error {code}: {msg}")
            log_error("token_dead", f"token #{i}: error {code} — {msg}")
        elif "response" in data:
            alive += 1
            print(f"[{i:2}] ✅ ЖИВОЙ")
        else:
            print(f"[{i:2}] ❌ НЕИЗВЕСТНО — {data}")
            log_error("token_unknown", f"token #{i}: {data}")

    print(f"\nЖивых: {alive} / {len(tokens)}")
    return alive


# === ПРОВЕРКА TELEGRAM ===
async def setup_telegram(session):
    print("\n=== ПРОВЕРКА TELEGRAM ===")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getMe"
    try:
        async with session.get(url) as response:
            if response.status != 200:
                msg = f"HTTP {response.status}"
                print(f"❌ {msg}")
                log_error("tg_token", msg)
                raise RuntimeError(msg)
            data = await response.json(content_type=None)
            if not data.get("ok"):
                msg = str(data)
                print(f"❌ {msg}")
                log_error("tg_token", msg)
                raise RuntimeError(msg)
            print(f"✅ Токен OK: @{data['result'].get('username')}")
    except Exception as e:
        raise RuntimeError(f"Telegram недоступен: {e}")

    # Проверка отправки в чат
    print(f"\nПроверка отправки в чат {TG_CHAT_ID}...")
    ok = await send_telegram(
        session, TG_BOT_TOKEN, TG_CHAT_ID,
        "🧪 Тестовое сообщение от cheker_debug"
    )
    if ok:
        print("✅ Сообщение успешно отправлено")
    else:
        print("❌ Не удалось отправить сообщение")

    return TG_BOT_TOKEN, TG_CHAT_ID


async def send_telegram(session, bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        async with session.post(url, data=payload) as response:
            if response.status != 200:
                log_error("tg_send", f"HTTP {response.status}")
                return False
            data = await response.json(content_type=None)
            if not data.get("ok"):
                log_error("tg_send", str(data))
                return False
            return True
    except Exception as e:
        log_error("tg_send", f"{type(e).__name__}: {e}")
        return False


# === VK ЗАПРОС ===
async def fetch_domain(session, domain, token):
    params = {
        "screen_name": domain,
        "access_token": token,
        "v": VK_API_VERSION
    }
    try:
        async with session.get(
            VK_URL, params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as response:
            if response.status != 200:
                return {"ok": False, "error": f"http_{response.status}"}
            data = await response.json(content_type=None)
    except asyncio.TimeoutError:
        return {"ok": False, "error": "timeout"}
    except aiohttp.ClientError as e:
        return {"ok": False, "error": f"network:{type(e).__name__}"}
    except Exception as e:
        return {"ok": False, "error": f"exc:{type(e).__name__}"}

    if "error" in data:
        err = data["error"]
        code = err.get("error_code")
        msg = err.get("error_msg", "unknown")
        return {"ok": False, "error": f"vk_{code}: {msg}"}

    response_data = data.get("response")
    if not response_data:
        return {"ok": True, "object_id": None, "object_type": None}

    object_id = response_data.get("object_id")
    object_type = response_data.get("type")
    if object_id is None:
        return {"ok": True, "object_id": None, "object_type": None}

    try:
        object_id = int(object_id)
    except Exception:
        pass
    return {"ok": True, "object_id": object_id, "object_type": object_type}


async def verify_change(session, domain, expected_id, token):
    result = await fetch_domain(session, domain, token)
    if not result["ok"]:
        return None
    if result["object_id"] == expected_id:
        return result
    return None


async def run_pass(session, domains, tokens, state, semaphore):
    notifications = []
    checked_ok = 0
    checked_fail = 0

    async def check_one(index, domain):
        nonlocal checked_ok, checked_fail
        async with semaphore:
            token_index = index % len(tokens)
            token = tokens[token_index]

            current = await fetch_domain(session, domain, token)
            if not current["ok"]:
                checked_fail += 1
                err = current.get("error", "?")
                log_error(f"domain_{err.split(':')[0]}", f"{domain}: {err}")
                return

            checked_ok += 1

            current_id = current["object_id"]
            current_type = current["object_type"]

            old = state.get(domain)
            if old is None:
                state[domain] = {
                    "object_id": current_id,
                    "object_type": current_type
                }
                return

            last_id = old.get("object_id")
            last_type = old.get("object_type")

            if current_id == last_id:
                if current_type != last_type:
                    state[domain] = {
                        "object_id": current_id,
                        "object_type": current_type
                    }
                return

            if last_id is not None and current_id is None:
                state[domain] = {"object_id": None, "object_type": None}
                notifications.append(
                    f"✅Detected release!\n"
                    f"{domain}: {owner_number(last_id)} → FREE"
                )
                return

            if last_id is None and current_id is not None:
                if len(tokens) > 1:
                    verify_token = tokens[(token_index + 1) % len(tokens)]
                else:
                    verify_token = token
                verified = await verify_change(session, domain, current_id, verify_token)
                if verified is None:
                    return
                new_type = verified["object_type"]
                state[domain] = {"object_id": current_id, "object_type": new_type}
                notifications.append(
                    f"✅Detected swap!\n"
                    f"{domain}: FREE → {owner_url(new_type, current_id)}"
                )
                return

            if last_id is not None and current_id is not None and current_id != last_id:
                if len(tokens) > 1:
                    verify_token = tokens[(token_index + 1) % len(tokens)]
                else:
                    verify_token = token
                verified = await verify_change(session, domain, current_id, verify_token)
                if verified is None:
                    return
                new_type = verified["object_type"]
                state[domain] = {"object_id": current_id, "object_type": new_type}
                notifications.append(
                    f"✅Detected swap!\n"
                    f"{domain}: {owner_number(last_id)} → {owner_url(new_type, current_id)}"
                )

    queue = asyncio.Queue()
    for i, d in enumerate(domains):
        await queue.put((i, d))

    async def worker():
        while True:
            try:
                i, d = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await check_one(i, d)
            except Exception as e:
                log_error("worker_exception", f"{d}: {type(e).__name__}: {e}")
            finally:
                queue.task_done()

    worker_count = min(CONCURRENCY, len(domains))
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]

    await queue.join()
    await asyncio.gather(*workers, return_exceptions=True)

    print(f"  checked_ok={checked_ok}, checked_fail={checked_fail}")
    return notifications


async def main():
    print("=" * 60)
    print("  ТЕСТОВЫЙ ЗАПУСК cheker_debug.py")
    print("=" * 60)

    try:
        tokens = load_tokens()
        domains = load_domains()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}")
        return

    state = load_json(STATE_FILE, {})
    for domain in domains:
        if domain not in state:
            state[domain] = {"object_id": None, "object_type": None}

    print(f"\nЗагружено доменов: {len(domains)}")
    print(f"Загружено токенов: {len(tokens)}")
    print(f"Конкурентность: {CONCURRENCY}")

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        limit_per_host=CONCURRENCY,
        ttl_dns_cache=3600,
        enable_cleanup_closed=True,
        keepalive_timeout=300,
        force_close=False,
        use_dns_cache=True
    )
    timeout = aiohttp.ClientTimeout(total=SESSION_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        # 1. Проверка токенов
        await check_tokens(session, tokens)

        # 2. Проверка Telegram
        try:
            bot_token, chat_id = await setup_telegram(session)
        except RuntimeError as e:
            print(f"\n[КРИТИЧНО] {e}")
            print("Скрипт дальше не пойдёт")
            return

        # 3. Один прогон доменов
        print("\n=== ПРОГОН ДОМЕНОВ (1 круг) ===")
        started = asyncio.get_running_loop().time()

        try:
            notifications = await run_pass(
                session=session,
                domains=domains,
                tokens=tokens,
                state=state,
                semaphore=asyncio.Semaphore(CONCURRENCY)
            )
        except Exception as e:
            print(f"[RUN ERROR] {type(e).__name__}: {e}")
            log_error("run_pass", f"{type(e).__name__}: {e}")
            notifications = []

        elapsed = asyncio.get_running_loop().time() - started

        try:
            save_json(STATE_FILE, state)
            print(f"✅ state сохранён: {len(state)} записей")
        except Exception as e:
            print(f"[SAVE ERROR] {e}")
            log_error("save_state", str(e))

        print(f"\n[CHECK] {len(domains)} доменов за {elapsed:.2f} сек.")
        print(f"        в state: {len(state)}")

        if notifications:
            print(f"\n=== УВЕДОМЛЕНИЯ ({len(notifications)}) ===")
            for n in notifications:
                print(f"  {n.replace(chr(10), ' | ')}")

    # === ИТОГОВЫЙ ОТЧЁТ ===
    print("\n" + "=" * 60)
    print("  ИТОГОВЫЙ ОТЧЁТ")
    print("=" * 60)

    if not errors:
        print("✅ Ошибок не обнаружено")
    else:
        print("Ошибки по категориям:")
        for cat, count in errors.most_common():
            print(f"  {cat}: {count}")

        print("\nПервые 50 ошибок:")
        for line in error_details[:50]:
            print(f"  {line}")

        # Сохраняем полный лог в файл
        with open(DEBUG_LOG, "w", encoding="utf-8") as f:
            f.write("ОШИБКИ ПО КАТЕГОРИЯМ:\n")
            for cat, count in errors.most_common():
                f.write(f"  {cat}: {count}\n")
            f.write("\nВСЕ ОШИБКИ:\n")
            for line in error_details:
                f.write(f"{line}\n")

        print(f"\n📄 Полный лог: {DEBUG_LOG}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nОстановлено пользователем")
