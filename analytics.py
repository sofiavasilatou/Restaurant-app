import sqlite3
import pandas as pd
from pandas.tseries.offsets import DateOffset
import matplotlib.pyplot as plt

conn = sqlite3.connect("restaurant3telefteo.sqlite")
#συνολικα εσοδα για καθε μηνα

df_income = pd.read_sql("""
SELECT date, total_amount
FROM RECEIPT
WHERE paid_off = 1
""", conn)

df_income["date"] = pd.to_datetime(df_income["date"])

# μόνο έτος 2025
df_income_2025 = df_income[
    df_income["date"].dt.year == 2025
]

# ISO εβδομάδα
df_income_2025["week"] = df_income_2025["date"].dt.to_period("W")

weekly_income_2025 = (
    df_income_2025
    .groupby("week")["total_amount"]
    .sum()
    .reset_index()
)

# μετατροπή week σε datetime για plot
weekly_income_2025["week"] = weekly_income_2025["week"].dt.start_time

print(weekly_income_2025)
plt.figure(figsize=(10, 5))

plt.plot(
    weekly_income_2025["week"],
    weekly_income_2025["total_amount"],
    marker="o"
)
for x, y in zip(
    weekly_income_2025["week"],
    weekly_income_2025["total_amount"]
):
    plt.text(
        x, y,
        f"{y:,.0f}",
        ha="center",
        va="bottom",
        fontsize=8
    )

plt.xlabel("Εβδομάδα")
plt.ylabel("Συνολικά Έσοδα (€)")
plt.title("Εβδομαδιαία Έσοδα Εστιατορίου (Ιαν–Μαρ 2025)")

plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()



#συνολικα φιλοδωρηματα ανα μερα
# -----------------------------
# ΦΙΛΟΔΩΡΗΜΑΤΑ ΑΝΑ ΗΜΕΡΑ (2025)
# -----------------------------

df_tips = pd.read_sql("""
SELECT date, tips
FROM RECEIPT
WHERE paid_off = 1
""", conn)

df_tips["date"] = pd.to_datetime(df_tips["date"])

# μόνο έτος 2025
df_tips_2025 = df_tips[
    df_tips["date"].dt.year == 2025
]

# ομαδοποίηση ανά ημέρα
daily_tips_2025 = (
    df_tips_2025
    .groupby(df_tips_2025["date"].dt.date)["tips"]
    .sum()
    .reset_index()
)

print(daily_tips_2025)
plt.figure(figsize=(10, 5))

plt.plot(
    daily_tips_2025["date"],
    daily_tips_2025["tips"],
    marker="o"
)
for x, y in zip(
    daily_tips_2025["date"],
    daily_tips_2025["tips"]
):
    plt.text(
        x, y,
        f"{y:.0f}",
        ha="center",
        va="bottom",
        fontsize=8
    )

plt.xlabel("Ημερομηνία")
plt.ylabel("Συνολικά Φιλοδωρήματα ($)")
plt.title("Συνολικά Φιλοδωρήματα ανά Ημέρα (Ιαν–Μαρ 2025)")

plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()


#πιο πολυ παραγγελμενο πιατο ανα μηνα
df_items = pd.read_sql("""
SELECT r.date, r.item_id, mi.description, r.qty
FROM RECEIPT r
JOIN MENU_ITEM mi ON mi.item_id = r.item_id
""", conn)

df_items["date"] = pd.to_datetime(df_items["date"])

df_items_2025 = df_items[
    df_items["date"].dt.year == 2025
]

df_items_2025["month"] = df_items_2025["date"].dt.to_period("M")

monthly_dish_totals = (
    df_items_2025
    .groupby(["month", "description"])["qty"]
    .sum()
    .reset_index()
)

most_ordered_per_month = (
    monthly_dish_totals
    .sort_values("qty", ascending=False)
    .groupby("month")
    .first()
    .reset_index()
)

print(most_ordered_per_month)
top_dishes_2025 = (
    df_items_2025
    .groupby("description")["qty"]
    .sum()
    .sort_values(ascending=False)
)

plt.figure(figsize=(8, 5))

top_dishes_2025.plot(kind="barh")

plt.xlabel("Σύνολο Παραγγελιών")
plt.ylabel("Πιάτο")
plt.title("Most Ordered Dishes (Έτος 2025)")

plt.gca().invert_yaxis()  # πιο δημοφιλές πάνω
plt.tight_layout()
plt.show()

#παραγγελιεσ ανα μερα εβδομαδας
df_orders = pd.read_sql("""
SELECT datetime
FROM ORDERS
""", conn)

df_orders["datetime"] = pd.to_datetime(
    df_orders["datetime"],
    format="mixed"
)


# -----------------------------
# 4. ΦΙΛΤΡΑΡΙΣΜΑ: μόνο έτος 2025
# -----------------------------
df_orders_2025 = df_orders[
    df_orders["datetime"].dt.year == 2025
]

# -----------------------------
# 5. Υπολογισμός ημέρας εβδομάδας
# -----------------------------
df_orders_2025["weekday"] = df_orders_2025["datetime"].dt.day_name()

# -----------------------------
# 6. Αριθμός παραγγελιών ανά ημέρα
# -----------------------------
orders_per_weekday = (
    df_orders_2025
    .groupby("weekday")
    .size()
    .reset_index(name="orders")
)

# -----------------------------
# 7. ΣΩΣΤΗ ΣΕΙΡΑ ΗΜΕΡΩΝ
# -----------------------------
weekday_order = [
    "Monday", "Tuesday", "Wednesday",
    "Thursday", "Friday", "Saturday", "Sunday"
]

orders_per_weekday["weekday"] = pd.Categorical(
    orders_per_weekday["weekday"],
    categories=weekday_order,
    ordered=True
)

orders_per_weekday = orders_per_weekday.sort_values("weekday")

# -----------------------------
# 8. Μετάφραση ημερών στα Ελληνικά
# -----------------------------
weekday_gr = {
    "Monday": "ΔΕΥΤΕΡΑ",
    "Tuesday": "ΤΡΙΤΗ",
    "Wednesday": "ΤΕΤΑΡΤΗ",
    "Thursday": "ΠΕΜΠΤΗ",
    "Friday": "ΠΑΡΑΣΚΕΥΗ",
    "Saturday": "ΣΑΒΒΑΤΟ",
    "Sunday": "ΚΥΡΙΑΚΗ",
}

orders_per_weekday["weekday_gr"] = (
    orders_per_weekday["weekday"]
    .astype(str)
    .map(weekday_gr)
)

# -----------------------------
# 9. ΓΡΑΦΗΜΑ
# -----------------------------
plt.figure(figsize=(9, 5))

plt.bar(
    orders_per_weekday["weekday_gr"],
    orders_per_weekday["orders"]
)

plt.xlabel("Ημέρα Εβδομάδας")
plt.ylabel("Αριθμός Παραγγελιών")
plt.title("Αριθμός Παραγγελιών ανά Ημέρα Εβδομάδας (Έτος 2025)")

plt.tight_layout()
plt.show()

# -----------------------------
# 10. Εκτύπωση πίνακα (προαιρετικό)
# -----------------------------

print(orders_per_weekday[["weekday_gr", "orders"]])
