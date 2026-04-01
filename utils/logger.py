from datetime import datetime

class AlertsLogger:
    def __init__(self, filepath="log.txt"):
        self.filepath = filepath

    def log(self, alerts_df):
        run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(self.filepath, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"RUN: {run_timestamp} | Alerts: {len(alerts_df)}\n")
            f.write(f"{'='*60}\n")
            f.write(alerts_df.to_csv(sep="|", index=False))