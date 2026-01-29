# Telegram Setup (QLIR Notification Server)

This document describes the **minimum steps** required to send notifications to Telegram from the QLIR notification server.

No formatting, routing, or alert semantics are involved here — this is purely delivery setup.

---

## 1. Create a Telegram bot

1. Open Telegram
2. Search for `@BotFather`
3. Start the chat
4. Run:

   ```
   /newbot
   ```
5. Choose:

   * Bot name (any)
   * Bot username (must end in `bot`)

You will receive a **bot token**, e.g.:

```
123456789:AAAbbbCCCdddEEEfff
```

Save this token.

---

## 2. Start a chat with the bot (required)

Bots **cannot message users unless the user starts the chat first**.

On your phone or desktop:

1. Search for your bot by username
2. Open the chat
3. Press **Start**
4. Send any message (e.g. `hi`)
  - it seems that you might need to send a message from your phone... it wasnt working when i sent the messafge from my laptop (telegram web)... 
   seems odd but idk. also try adding the url param offset=0 

---

## 3. Get your chat ID

After sending a message to the bot, open the following URL in a browser:

```
https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
```

Look for an entry like:

```json
"message": {
  "chat": {
    "id": 987654321,
    "type": "private"
  }
}
```

The value of `chat.id` is your **chat ID**.

⚠️ **Important**

* Use `chat.id`
* Do NOT use any ID where `"is_bot": true`

Note: This is actually the same value as your user.id, so if you have multiple bots to the same user, then you can just reuse the chat/user id 

---

## 4. Export required environment variables

In the shell where you will start the notification server:

```bash
export TELEGRAM_BOT_TOKEN="123456789:AAAbbbCCCdddEEEfff"
export TELEGRAM_CHAT_ID="987654321"
```

These must be present **in the same environment** that starts the server.

---

## 5. Start the notification server

```bash
python notification_server.py
```

(or however the server is launched in your setup)

The server will read the environment variables at startup.

---

## 6. Smoke test

Write a test alert to the outbox:

```bash
./smoke_alert.sh
```

Expected result:

* A Telegram message appears
* Phone vibrates
* Alert file moves from `alerts/outbox/` → `alerts/sent/`

---

## Common errors

### `403: bots can't send messages to bots`

* You used a bot ID instead of a human chat ID
* Re-run `getUpdates` after sending a message from your account

### No messages arrive

* Bot chat was not started
* Wrong chat ID
* Environment variables not set in server shell

---

## Summary

Telegram requires exactly two things:

* **Bot token** → who is sending
* **Chat ID** → where the message goes

Once configured, Telegram delivery is reliable and push-based by default.
