import pymysql
import pandas as pd
import os
from dotenv import load_dotenv

print('Import completato')

# carico le variabili d'ambiente per connettermi al db
load_dotenv()

# Connessione diretta al DB
conn = pymysql.connect(
    host = os.getenv('DB_HOST'),
    port = int(os.getenv('DB_PORT', 3306)),
    user = os.getenv('DB_USER'),
    password = os.getenv('DB_PASSWORD'),
    database = os.getenv('DB_NAME')
)

cursor = conn.cursor()

print("Connessione al DB stabilita. Eseguo query...")

# STEP 1 — imposto il limite PRIMA della query
cursor.execute("SET SESSION group_concat_max_len = 1000000;")

# STEP 2 — query completa
query = """
SELECT
    concat('https://maia.zucchettihc.it/index.php?module=Cases&action=DetailView&record=',t.id) url_ticket,
    t.case_number,
    t.name oggetto,
    tt.crm_plain_description_c descrizione,
    t.priority priorita_finale,
    audit_p.before_value_string priorita_iniziale_cliente,
    tt.crm_area_c area,
    tt.crm_tipologia_intervento_c tipologia_intervento,
    a.name articolo,
    m.name modulo_sw,
    r.name reparto,
    t.date_entered data_creazione,
    tt.crm_data_risoluzione_c data_risoluzione,
    GROUP_CONCAT(
        CONCAT(
            IF(o.user_name IS NULL, 'CLIENTE', o.user_name),
            ': ',
            u.description
        )
        ORDER BY u.date_entered ASC
        SEPARATOR '\n---\n'
    ) AS conversazione
FROM cases t
JOIN cases_cstm tt ON t.id = tt.id_c
LEFT JOIN aos_products a ON tt.aos_products_id_c = a.id AND a.deleted = 0
LEFT JOIN aos_product_categories m ON m.id = tt.aos_product_categories_id_c AND m.deleted = 0
LEFT JOIN crm_reparti r ON tt.crm_reparti_id_c = r.id AND r.deleted = 0
LEFT JOIN aop_case_updates u ON u.case_id = t.id AND u.deleted = 0
LEFT JOIN users o ON u.created_by = o.id
LEFT JOIN (
    SELECT ca.parent_id, ca.before_value_string
    FROM cases_audit ca
    INNER JOIN (
        SELECT parent_id, MIN(date_created) as prima_modifica
        FROM cases_audit
        WHERE field_name = 'priority'
        AND date_created >= '2024-09-01'
        GROUP BY parent_id
    ) first_audit 
    ON ca.parent_id = first_audit.parent_id 
    AND ca.date_created = first_audit.prima_modifica
    WHERE ca.field_name = 'priority'
) audit_p ON audit_p.parent_id = t.id
WHERE t.deleted = 0
AND t.state in ('Closed', 'ManuallyClosed')
AND t.date_modified >= '2024-09-01'
AND t.date_entered >= '2024-09-01'
GROUP BY
    t.id, t.case_number, t.name, tt.crm_plain_description_c,
    t.priority, audit_p.before_value_string, tt.crm_area_c,
    tt.crm_tipologia_intervento_c, a.name, m.name, r.name,
    t.date_entered, tt.crm_data_risoluzione_c
"""

# STEP 3 — eseguo e salvo direttamente su CSV
print("Esecuzione query...")
df_new = pd.read_sql(query, conn)
print(f"Righe estratte: {len(df_new):,}")

conn.close()

# STEP 4 — salvo
df_new.to_csv("TicketEstrazione090326.csv", index=False, encoding='utf-8-sig')
print("Salvato!")