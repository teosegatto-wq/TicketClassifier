"""
diagnosi_boilerplate_oggetto.py
================================
Stima quanti ticket del training set hanno il blocco "Orario reperibilità"
nell'oggetto — e quindi nel testo_input — senza dover ricalcolare embeddings.

Esegui dalla cartella del progetto (dove c'è dataset_clean.csv):
    python diagnosi_boilerplate_oggetto.py
"""

import re
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent.parent.parent  # notebooks/analysis/ → notebooks/ → root
CSV_PATH  = BASE_DIR / 'data' / 'dataset_clean.csv'
SOGLIA_SPLIT = '2025-11-01'

PATTERN_REP = re.compile(r'Orario reperibilit', re.IGNORECASE)

# ── Caricamento ───────────────────────────────────────────────────────────────
print("Carico dataset_clean.csv ...")
df = pd.read_csv(CSV_PATH, parse_dates=['data_creazione'], engine='python')
print(f"  Righe totali : {len(df):,}")

# ── Split train/test (identico al training) ───────────────────────────────────
mask_train = df['data_creazione'] < SOGLIA_SPLIT
df_train = df[mask_train].copy()
df_test  = df[~mask_train].copy()
print(f"  Train        : {len(df_train):,}")
print(f"  Test         : {len(df_test):,}")
print(f"\n  Colonne disponibili: {df.columns.tolist()}\n")

# ── CHECK 1: boilerplate nel testo_input ─────────────────────────────────────
# dataset_clean.csv contiene solo testo_input (oggetto + descrizione già uniti).
# Qui vediamo l'impatto reale su ciò che il modello ha effettivamente visto.
mask_ogg_train = df_train['testo_input'].str.contains(PATTERN_REP, na=False)
mask_ogg_test  = df_test['testo_input'].str.contains(PATTERN_REP, na=False)

print("=" * 60)
print("CHECK 1 — 'testo_input' con boilerplate reperibilità")
print("=" * 60)
print(f"  Train  : {mask_ogg_train.sum():>6,}  /  {len(df_train):,}  "
      f"({mask_ogg_train.mean()*100:.1f}%)")
print(f"  Test   : {mask_ogg_test.sum():>6,}  /  {len(df_test):,}  "
      f"({mask_ogg_test.mean()*100:.1f}%)\n")

# ── CHECK 2: alias — testo_input è l'unica fonte testuale nel CSV ────────────
# mask_ti_train coincide con mask_ogg_train sopra, la rinominiamo per coerenza
# con il resto dello script che la usa nei check successivi.
mask_ti_train = mask_ogg_train
mask_ti_test  = mask_ogg_test

print("=" * 60)
print("CHECK 2 — conferma: mask_ti == mask_ogg (stessa colonna)")
print("=" * 60)
print(f"  Ticket inquinati train : {mask_ti_train.sum():,}  ({mask_ti_train.mean()*100:.1f}%)")
print(f"  Ticket inquinati test  : {mask_ti_test.sum():,}  ({mask_ti_test.mean()*100:.1f}%)\n")

# ── CHECK 3: distribuzione per area nei ticket "inquinati" ───────────────────
# Capire se alcune classi sono più colpite di altre
if 'area_v2' in df_train.columns:
    area_col = 'area_v2'
elif 'area' in df_train.columns:
    area_col = 'area'
else:
    area_col = None

if area_col:
    print("=" * 60)
    print(f"CHECK 3 — % ticket con boilerplate per classe ({area_col})")
    print("=" * 60)
    grp = df_train[mask_ti_train][area_col].value_counts()
    tot = df_train[area_col].value_counts()
    report = pd.DataFrame({
        'con_boilerplate' : grp,
        'totale_classe'   : tot,
    }).dropna()
    report['pct'] = (report['con_boilerplate'] / report['totale_classe'] * 100).round(1)
    report = report.sort_values('pct', ascending=False)
    print(report.to_string())
    print()

# ── CHECK 4: quante parole occupa il boilerplate nel testo_input ──────────────
# Misura quanto "spazio" rubava al segnale utile
def conta_parole_boilerplate(testo):
    """Conta le parole del blocco boilerplate se presente."""
    if not isinstance(testo, str):
        return 0
    m = PATTERN_REP.search(testo)
    if not m:
        return 0
    # Prende tutto dal match fino alla fine dell'oggetto (approssimazione:
    # il blocco nel campo oggetto di solito termina prima del '  Buongiorno')
    blocco = testo[m.start():]
    # Tronca al primo saluto o a 80 caratteri (il blocco oggetto è corto)
    fine = re.search(r'\s{2,}|Buongiorno|Salve|Ciao', blocco)
    if fine:
        blocco = blocco[:fine.start()]
    return len(blocco.split())

inquinati = df_train[mask_ti_train].copy()
inquinati['parole_boilerplate'] = inquinati['testo_input'].apply(conta_parole_boilerplate)
inquinati['n_parole_testo']     = inquinati['testo_input'].str.split().str.len()
inquinati['pct_inquinamento']   = (
    inquinati['parole_boilerplate'] / inquinati['n_parole_testo'] * 100
).round(1)

print("=" * 60)
print("CHECK 4 — peso del boilerplate sul testo_input (ticket inquinati)")
print("=" * 60)
desc = inquinati[['parole_boilerplate', 'n_parole_testo', 'pct_inquinamento']].describe().round(1)
print(desc.to_string())
print()

# ── CHECK 5: esempi concreti per ispezione visiva ─────────────────────────────
print("=" * 60)
print("CHECK 5 — 3 esempi di testo_input con boilerplate residuo")
print("=" * 60)
for i, (_, row) in enumerate(inquinati.head(3).iterrows()):
    print(f"\n  [{i+1}] area={row.get(area_col, '?')} | "
          f"parole_totali={int(row['n_parole_testo'])} | "
          f"parole_boilerplate≈{int(row['parole_boilerplate'])} "
          f"({row['pct_inquinamento']}%)")
    print(f"  testo_input[:300]:")
    print(f"  {row['testo_input'][:300]!r}")

# ── VERDETTO ──────────────────────────────────────────────────────────────────
pct_impatto = mask_ti_train.mean() * 100
print("\n" + "=" * 60)
print("VERDETTO")
print("=" * 60)
if pct_impatto < 5:
    giudizio = "BASSO — fix utile ma non urgente per il retraining"
elif pct_impatto < 15:
    giudizio = "MEDIO — vale rigenerare dataset_clean.csv prima del prossimo retraining"
else:
    giudizio = "ALTO — il boilerplate è un artefatto significativo, priorità alta"

print(f"  Ticket train inquinati : {mask_ti_train.sum():,} ({pct_impatto:.1f}%)")
print(f"  Giudizio               : {giudizio}")
print()
