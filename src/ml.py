# Importera nödvändiga bibliotek
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report

# Läs in data
df = pd.read_csv("../data/stock_data.csv")  # Justera sökvägen vid behov

# Exempel: anta att du har kolumnerna 'open', 'close', och 'winner' (target)
# Skapa en enkel feature: prisförändring
df["change"] = df["close"] - df["open"]

# Skapa target-kolumnen 'winner': 1 om close > open, annars 0
df["winner"] = (df["close"] > df["open"]).astype(int)

# Räkna antal vinst- och förlustdagar
antal_vinst = df["winner"].sum()
antal_forlust = len(df) - antal_vinst
winrate = antal_vinst / len(df) * 100

print(f"Antal vinstdagar: {antal_vinst}")
print(f"Antal förlustdagar: {antal_forlust}")
print(f"Winrate: {winrate:.2f}%")

# Features och target
X = df[["change"]]
y = df["winner"]  # 1 = vinst, 0 = förlust

# Dela upp i train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Skapa och träna modellen
model = LogisticRegression()
model.fit(X_train, y_train)

# Gör prediktioner
y_pred = model.predict(X_test)

# Utvärdera
print(classification_report(y_test, y_pred))