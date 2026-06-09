from __future__ import annotations
import csv
import json
from pathlib import Path

BUSINESS_DATE = "2026-03-24"

CLIENTS = [
    {"ClientID": "C001", "FullName": "Alice van Dijk", "Segment": "retail", "Country": "NL", "BirthDate": "1988-04-12"},
    {"ClientID": "C002", "FullName": "Bruno Meyer", "Segment": "sme", "Country": "DE", "BirthDate": "1979-09-01"},
    {"ClientID": "C003", "FullName": "Claire Dubois", "Segment": "private", "Country": "FR", "BirthDate": "1990-02-20"},
    {"ClientID": "C004", "FullName": "David Peeters", "Segment": "corporate", "Country": "BE", "BirthDate": "1983-11-15"},
    {"ClientID": "C002", "FullName": "Bruno Meyer", "Segment": "sme", "Country": "DE", "BirthDate": "1979-09-01"},
]

CREDITS = [
    {"CreditID": "CR001", "ClientID": "C001", "CreditStatus": "active", "OutstandingBalance": "12500.00", "MonthlyPayment": "320.00", "OriginationDate": "2022-01-10"},
    {"CreditID": "CR002", "ClientID": "C002", "CreditStatus": "active", "OutstandingBalance": "98500.50", "MonthlyPayment": "1250.00", "OriginationDate": "2021-06-18"},
    {"CreditID": "CR003", "ClientID": "C003", "CreditStatus": "delinquent", "OutstandingBalance": "158000.00", "MonthlyPayment": "0.00", "OriginationDate": "2020-03-05"},
    {"CreditID": "CR004", "ClientID": "C004", "CreditStatus": "closed", "OutstandingBalance": "0.00", "MonthlyPayment": "0.00", "OriginationDate": "2019-07-22"},
    {"CreditID": "CR002", "ClientID": "C002", "CreditStatus": "active", "OutstandingBalance": "98500.50", "MonthlyPayment": "1250.00", "OriginationDate": "2021-06-18"},
]

MARKET = [
    {"country_code": "NL", "market_rate": 3.1, "market_snapshot_date": BUSINESS_DATE},
    {"country_code": "DE", "market_rate": 2.8, "market_snapshot_date": BUSINESS_DATE},
    {"country_code": "FR", "market_rate": 3.4, "market_snapshot_date": BUSINESS_DATE},
    {"country_code": "BE", "market_rate": 2.9, "market_snapshot_date": BUSINESS_DATE},
]

def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

def write_json(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

def main() -> None:
    root = Path(__file__).resolve().parent
    write_csv(root / "raw" / "clients" / "clients_2026_03_24.csv", CLIENTS)
    write_csv(root / "raw" / "credits" / "credits_2026_03_24.csv", CREDITS)
    write_json(root / "raw" / "market_info" / "market_info_2026_03_24.json", MARKET)
    print("Sample data generated under:", root / "raw")

if __name__ == "__main__":
    main()
