import uvicorn
from api import app
import threading
import schedule
import time
from scheduler import scrape_and_update

def run_scheduler():
    """Run the scheduler in a separate thread."""
    scrape_and_update()  # Run immediately at startup
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep to reduce CPU usage

if __name__ == "__main__":
    # Start the scheduler in a background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    # Start the FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=8000)
