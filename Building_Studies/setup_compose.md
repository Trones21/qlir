# Setting Up a Compose Key for Math & Greek Symbols (Wayland / XWayland)

This guide walks through **how Compose actually works**, how to **inspect the default mappings**, and how to **extend them safely under Wayland/XWayland** without relying on deprecated X11 tricks.

It assumes:

* Linux
* Wayland session (with or without XWayland)
* You want math + Greek symbols
* You *don’t* want to hand-write everything

---

## 0. Why this works under Wayland

Under Wayland:

* `xmodmap` and custom dead keys are **unreliable**
* Compose is the **supported and portable extension mechanism**
* Both Wayland-native and XWayland apps respect Compose

So we:

* keep system defaults
* inspect them
* layer minimal overrides

---

## 1. Find your locale (this matters)

Compose mappings are **locale-dependent**.

Run:

```bash
locale
```

Look for:

```text
LANG=en_US.UTF-8
```

That value determines **which Compose file is loaded by default**.

---

## 2. Inspect the system Compose file (ground truth)

The default mappings live here:

```bash
less /usr/share/X11/locale/en_US.UTF-8/Compose
```

(Replace `en_US.UTF-8` with whatever `LANG` returned.)

This file is:

* huge
* authoritative
* already contains *most* symbols you want

You are **not** supposed to reimplement this.

---

## 3. How to search the Compose file correctly

Inside `less`, search with `/`.

### Important detail: `Compose` vs `Multi_key`

On many systems, **Compose is named `Multi_key` internally**.

So if you search for:

```
/<Compose>
```

You may only find **one or zero matches**.

Instead, search for:

```
/<Multi_key>
```

That’s the real identifier.

Example entry:

```text
<Multi_key> <minus> <greater> : "→"
```

Which corresponds to:

```
Compose  -  >  → 
```

This explains why:

* you “only see one Compose”
* but many sequences still work

---

## 4. Verify what already exists (before adding anything)

Try these **without any custom config**:

```
Compose - >   → 
Compose < =   ≤
Compose ! =   ≠
```

If these work:

* Compose is enabled
* system mappings are loading correctly

Greek usually **will not** work yet — that’s expected.

---

## 5. Why Greek isn’t in Compose by default

In the system file you’ll see things like:

```text
<dead_greek> <d> : "δ"
<dead_greek> <D> : "Δ"
```

Meaning:

* Greek is implemented via a **dead key**
* not via Compose
* and often **no physical key emits `dead_greek`**

Under Wayland, remapping dead keys is not reliable — so we don’t use them.

---

## 6. Create `~/.XCompose` (user override layer)

Compose supports **user overrides** via `~/.XCompose`.

Create it:

```bash
nano ~/.XCompose
```

### Always start with this line

```text
include "%L"
```

This means:

> “Load the full system Compose file for my locale.”

Without this line, you’d lose all defaults.

---

## 7. Add only the missing pieces (Greek)

Append below the include:

```text
# --- Greek letters (Compose-friendly replacement for dead_greek) ---
<Multi_key> <g> <d> : "δ"
<Multi_key> <g> <D> : "Δ"
<Multi_key> <g> <s> : "σ"
<Multi_key> <g> <m> : "μ"
<Multi_key> <g> <p> : "π"
<Multi_key> <g> <l> : "λ"
```

Mnemonic:

* `g` = Greek namespace
* second key = letter

Example:

```
Compose g d → δ
```

---

## 8. Ensure Compose is actually being loaded

### Check the environment variable

Compose uses this variable internally:

```bash
echo $XCOMPOSEFILE
```

Possible results:

* empty → defaults to `~/.XCompose` (fine)
* set → verify it points where you expect

If you want to be explicit (optional):

```bash
export XCOMPOSEFILE="$HOME/.XCompose"
```

(Usually not necessary.)

---

## 9. Reloading Compose (there is no “source”)

Unlike `.bashrc`, Compose **cannot be reloaded live**.

To reload it, you must restart the session.

### Clean, universal way (Wayland-safe)

```bash
loginctl terminate-user $USER
```

This:

* ends the Wayland session
* restarts the compositor
* reloads keyboard config
* reloads Compose

Then log back in.

---

## 10. Verify after restart

Open a terminal and test:

```
Compose g d
Compose - >
Compose < =
```

If these work:

* system Compose loaded
* `~/.XCompose` loaded
* overrides applied correctly

You’re done.

---

## 11. Recommended key choice

Common Compose key choices:

* **Right Alt** (recommended)
* Right Shift
* Menu key

Set it via:

* Desktop settings (preferred under Wayland)
* or `setxkbmap -option compose:ralt`

---

## 12. Design guideline (strongly recommended)

**Use ASCII internally, Unicode at the edges.**

```python
# internal logic
delta_price = df["price"].diff()

# presentation
df["Δprice"] = df["price"].diff()
```

Best of both worlds:

* portability
* readability
* expressive output

---

## Summary

* Inspect system Compose first (`less`, `Multi_key`)
* Don’t reimplement defaults
* Use `include "%L"`
* Add Greek via Compose (not dead keys)
* Restart session once
* Enjoy symbols everywhere

This setup is:

* Wayland-safe
* XWayland-safe
* portable
* minimal
* future-proof

If you want next, we can:

* extract a **“quant / math” Compose pack**
* or turn this into a **portable dotfiles module**
