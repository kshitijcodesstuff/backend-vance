import uvicorn
from api import app
import threading
import schedule
import time
from scheduler import scrape_and_update

def run_scheduler():
    """Run the scheduler in a separate thread."""
    scrape_and_update() 
    while True:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == "__main__":

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()


    uvicorn.run(app, host="0.0.0.0", port=8000)
