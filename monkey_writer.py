import time
import numpy as np
from loguru import logger

if __name__ == "__main__":

    try:
        while True:

            # Need to open/close file on every write so that watchdog can see each change
            with open('data/fake_data.txt', "a") as f:

                # Write current timestamp
                number = time.time()
                f.write(str(number) + '\n')
                logger.debug(f"Wrote number {number} to file")

            # Wait a variable amount of time before writing next
            time.sleep(np.random.randint(10))

    except KeyboardInterrupt:
        logger.error("Monkey writer has been stopped via Keyboard Interrupt.")


