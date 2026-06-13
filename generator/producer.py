# What this script does:

# Creates 10 fake subcontractors with contracts and rates
# Generates realistic interventions with real durations
# Generates invoices — 80% normal, 20% anomalies of 3 types:

# overbilling — billed more hours than worked
# wrong_rate — applied a higher rate than the contract
# duplicate — same intervention billed twice


# Sends both interventions and invoices to Kafka every second





import json
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer

fake = Faker('fr_FR')

# ── Subcontractors (static reference data) ──────────────────────────────────
SUBCONTRACTORS = [
    {"subcontractor_id": f"SUB{i:03d}",
     "name": fake.company(),
     "contract_rate": round(random.uniform(35.0, 85.0), 2),
     "zone": random.choice(["IDF", "PACA", "ARA", "OCC", "BRE", "NAQ"]),
     "contract_start_date": fake.date_between(start_date="-3y", end_date="-6m").isoformat()}
    for i in range(1, 11)
]

# ── Intervention types & durations ──────────────────────────────────────────
INTERVENTION_TYPES = {
    "installation":  {"min_hours": 2, "max_hours": 8},
    "repair":        {"min_hours": 1, "max_hours": 4},
    "maintenance":   {"min_hours": 1, "max_hours": 3},
    "audit":         {"min_hours": 2, "max_hours": 6},
}

# ── Kafka producer ───────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def generate_intervention(subcontractor):
    intervention_type = random.choice(list(INTERVENTION_TYPES.keys()))
    limits = INTERVENTION_TYPES[intervention_type]
    real_duration = round(random.uniform(limits["min_hours"], limits["max_hours"]), 2)
    start_time = fake.date_time_between(start_date="-30d", end_date="now")
    end_time = start_time + timedelta(hours=real_duration)

    return {
        "intervention_id":   f"INT{fake.unique.random_int(min=10000, max=99999)}",
        "subcontractor_id":  subcontractor["subcontractor_id"],
        "technician_id":     f"TECH{random.randint(1, 50):03d}",
        "intervention_type": intervention_type,
        "start_time":        start_time.isoformat(),
        "end_time":          end_time.isoformat(),
        "real_duration":     real_duration,
        "location":          fake.city(),
        "status":            random.choices(
                                ["completed", "failed", "re_iterated"],
                                weights=[75, 10, 15]
                             )[0]
    }


def generate_invoice(intervention, subcontractor):
    # Inject anomalies ~20% of the time
    anomaly = random.random() < 0.20
    if anomaly:
        anomaly_type = random.choice(["overbilling", "wrong_rate", "duplicate"])
    else:
        anomaly_type = None

    real_duration = intervention["real_duration"]
    contract_rate = subcontractor["contract_rate"]

    if anomaly_type == "overbilling":
        billed_duration = round(real_duration * random.uniform(1.3, 2.0), 2)
        rate_applied = contract_rate
    elif anomaly_type == "wrong_rate":
        billed_duration = real_duration
        rate_applied = round(contract_rate * random.uniform(1.2, 1.8), 2)
    else:
        billed_duration = real_duration
        rate_applied = contract_rate

    billed_amount = round(billed_duration * rate_applied, 2)
    expected_amount = round(real_duration * contract_rate, 2)

    return {
        "invoice_id":        f"INV{fake.unique.random_int(min=100000, max=999999)}",
        "intervention_id":   intervention["intervention_id"],
        "subcontractor_id":  subcontractor["subcontractor_id"],
        "billed_amount":     billed_amount,
        "expected_amount":   expected_amount,
        "billed_duration":   billed_duration,
        "real_duration":     real_duration,
        "rate_applied":      rate_applied,
        "contract_rate":     contract_rate,
        "billing_date":      datetime.now().isoformat(),
        "anomaly_type":      anomaly_type,
    }


def main():
    print("Starting Billing Sentinel data generator...")
    print(f"Loaded {len(SUBCONTRACTORS)} subcontractors")
    print("Sending events to Kafka topic: invoices\n")

    while True:
        subcontractor = random.choice(SUBCONTRACTORS)
        intervention = generate_intervention(subcontractor)
        invoice = generate_invoice(intervention, subcontractor)

        # Send intervention event
        producer.send("interventions", value=intervention)

        # Send invoice event
        producer.send("invoices", value=invoice)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"Sent invoice {invoice['invoice_id']} | "
              f"Subcontractor: {subcontractor['name']} | "
              f"Billed: {invoice['billed_amount']}€ | "
              f"Expected: {invoice['expected_amount']}€ | "
              f"Anomaly: {invoice['anomaly_type'] or 'none'}")

        time.sleep(1)


if __name__ == "__main__":
    main()